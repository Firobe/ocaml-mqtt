open Eio

let fmt = Format.asprintf

let read_char =
  let buf = Cstruct.create 1 in
  fun flow ->
    assert (Flow.single_read flow buf = 1);
    Cstruct.get_char buf 0

let decode_length sock =
  let rec loop value mult =
    let ch = read_char sock in
    let ch = Char.code ch in
    let digit = ch land 127 in
    let value = value + (digit * mult) in
    let mult = mult * 128 in
    if ch land 128 = 0 then value else loop value mult
  in
  loop 0 1

let read_packet sock =
  let header_byte = read_char sock in
  let msgid, opts =
    Mqtt_packet.Decoder.decode_fixed_header (Char.code header_byte)
  in
  let count = decode_length sock in

  let data = Cstruct.create count in
  let () =
    try Flow.read_exact sock data
    with End_of_file -> failwith "could not read bytes"
  in
  let pkt =
    Read_buffer.make (Cstruct.to_bytes data |> Bytes.to_string)
    |> Mqtt_packet.Decoder.decode_packet opts msgid
  in
  (opts, pkt)

module Log = Logs

type connection = [ `Flow | `R | `W | `Shutdown ] Std.r

type t = {
  cxn : connection;
  id : string;
  inflight : (int, Condition.t * Mqtt_packet.t) Hashtbl.t;
  on_message : topic:string -> string -> unit;
  on_disconnect : t -> unit;
  on_error : t -> exn -> unit;
  should_stop_reader : Condition.t;
}

let wrap_catch client f = try f () with exc -> client.on_error client exc

let default_on_error client exn =
  Log.err (fun log -> log "[%s]: Unhandled exception: %a" client.id Fmt.exn exn)

let default_on_message ~topic:_ _ = ()
let default_on_disconnect _ = ()

let read_packets client =
  let ack_inflight id pkt =
    try
      let cond, expected_ack_pkt = Hashtbl.find client.inflight id in
      if pkt = expected_ack_pkt then (
        Hashtbl.remove client.inflight id;
        Condition.broadcast cond)
      else failwith "unexpected packet in ack"
    with Not_found -> failwith (fmt "ack for id=%d not found" id)
  in

  let rec loop () =
    let (_dup, qos, _retain), packet = read_packet client.cxn in
    let () =
      match packet with
      (* Publish with QoS 0: push *)
      | Publish (None, topic, payload) when qos = Atmost_once ->
        client.on_message ~topic payload
      (* Publish with QoS 0 and packet identifier: error *)
      | Publish (Some _id, _topic, _payload) when qos = Atmost_once ->
        failwith
          "protocol violation: publish packet with qos 0 must not have id"
      (* Publish with QoS 1 *)
      | Publish (Some id, topic, payload) when qos = Atleast_once ->
        (* - Push the message to the consumer queue.
           - Send back the PUBACK packet. *)
        client.on_message ~topic payload;
        let puback = Mqtt_packet.Encoder.puback id in
        Flow.copy_string puback client.cxn
      | Publish (None, _topic, _payload) when qos = Atleast_once ->
        failwith "protocol violation: publish packet with qos > 0 must have id"
      | Publish _ -> failwith "not supported publish packet (probably qos 2)"
      | Suback (id, _)
      | Unsuback id
      | Puback id
      | Pubrec id
      | Pubrel id
      | Pubcomp id ->
        ack_inflight id packet
      | Pingresp -> ()
      | _ -> failwith "unknown packet from server"
    in
    loop ()
  in

  Log.debug (fun log -> log "[%s] Starting reader loop..." client.id);
  Eio.Switch.run ~name:"reader loop" (fun sw ->
      Fiber.fork_daemon ~sw loop;
      Condition.await_no_mutex client.should_stop_reader;
      Log.info (fun log -> log "[%s] Stopping reader loop..." client.id))

let disconnect client =
  Log.info (fun log -> log "[%s] Disconnecting client..." client.id);
  Condition.broadcast client.should_stop_reader;
  Flow.copy_string (Mqtt_packet.Encoder.disconnect ()) client.cxn;
  client.on_disconnect client;
  Log.info (fun log -> log "[%s] Client disconnected." client.id)

let shutdown client =
  Log.debug (fun log -> log "[%s] Shutting down the connection..." client.id);
  (* Flow.close client.cxn; TODO implement close for TLS *)
  Log.debug (fun log -> log "[%s] Client connection shut down." client.id)

let run_pinger ~clock ~keep_alive client =
  Log.debug (fun log -> log "Starting ping timer...");
  (* 25% leeway *)
  let keep_alive = 0.75 *. float_of_int keep_alive in
  let rec loop () =
    Time.sleep clock keep_alive;
    let pingreq_packet = Mqtt_packet.Encoder.pingreq () in
    Flow.copy_string pingreq_packet client.cxn;
    loop ()
  in
  loop ()

exception Connection_error

let open_tcp_connection ~sw ~net ~client_id host port =
  let addresses =
    Eio.Net.getaddrinfo_stream ~service:(string_of_int port) net host
  in
  match addresses with
  | address :: _ -> (Eio.Net.connect ~sw net address :> connection)
  | _ ->
    Log.err (fun log ->
        log "[%s] could not get address info for %S" client_id host);
    raise Connection_error

let open_tls_connection ~sw ~net ~client_id ~ca_file host port =
  let sock = open_tcp_connection ~sw ~net ~client_id host port in
  let authenticator = X509_eio.authenticator (`Ca_file ca_file) in
  let config = Tls.Config.client ~authenticator () in
  (Tls_eio.client_of_flow config sock :> connection)

let rec create_connection ~sw ~net ?tls_ca ~port ~client_id hosts =
  match hosts with
  | [] ->
    Log.err (fun log ->
        log "[%s] Could not connect to any of the hosts (on port %d): %a"
          client_id port
          Fmt.Dump.(list string)
          hosts);
    raise Connection_error
  | host :: hosts -> (
    try
      Log.debug (fun log ->
          log "[%s] Connecting to `%s:%d`..." client_id host port);
      let connection =
        match tls_ca with
        | Some ca_file ->
          open_tls_connection ~sw ~net ~client_id ~ca_file host port
        | None -> open_tcp_connection ~sw ~net ~client_id host port
      in
      Log.info (fun log ->
          log "[%s] Connection opened on `%s:%d`." client_id host port);
      connection
    with _ ->
      Log.debug (fun log ->
          log "[%s] Could not connect, trying next host..." client_id);
      create_connection ~sw ~net ?tls_ca ~port ~client_id hosts)

let connect ~sw ~net ~clock ?(id = "ocaml-mqtt") ?tls_ca ?credentials ?will
    ?(clean_session = true) ?(keep_alive = 30)
    ?(on_message = default_on_message) ?(on_disconnect = default_on_disconnect)
    ?(on_error = default_on_error) ?(port = 1883) hosts =
  let flags =
    if clean_session || id = "" then [ Mqtt_packet.Clean_session ] else []
  in
  let cxn_data =
    { Mqtt_packet.clientid = id; credentials; will; flags; keep_alive }
  in

  let sock = create_connection ~sw ~net ?tls_ca ~port ~client_id:id hosts in

  let connect_packet =
    Mqtt_packet.Encoder.connect ?credentials:cxn_data.credentials
      ?will:cxn_data.will ~flags:cxn_data.flags ~keep_alive:cxn_data.keep_alive
      cxn_data.clientid
  in
  Flow.copy_string connect_packet sock;
  let inflight = Hashtbl.create 16 in

  match read_packet sock with
  | _, Connack { connection_status = Accepted; session_present } ->
    Log.debug (fun log ->
        log "[%s] Connection acknowledged (session_present=%b)" id
          session_present);

    let client =
      {
        cxn = sock;
        id;
        inflight;
        should_stop_reader = Condition.create ();
        on_message;
        on_disconnect;
        on_error;
      }
    in

    Fiber.fork ~sw (fun () ->
        Log.debug (fun log -> log "[%s] Packet reader started." client.id);
        Switch.run ~name:"mqtt" (fun sw ->
            Fiber.fork_daemon ~sw (fun () ->
                run_pinger ~clock ~keep_alive client);
            wrap_catch client (fun () -> read_packets client));
        Log.debug (fun log ->
            log "[%s] Packet reader stopped, shutting down..." client.id);
        shutdown client);

    client
  | _, Connack pkt ->
    let conn_status =
      Mqtt_packet.connection_status_to_string pkt.connection_status
    in
    Log.err (fun log -> log "[%s] Connection failed: %s" id conn_status);
    raise Connection_error
  | _ ->
    Log.err (fun log ->
        log "[%s] Invalid response from broker on connection" id);
    raise Connection_error

let publish ?(dup = false) ?(qos = Mqtt_core.Atleast_once) ?(retain = false)
    ~topic payload client =
  match qos with
  | Atmost_once ->
    let pkt_data =
      Mqtt_packet.Encoder.publish ~dup ~qos ~retain ~id:0 ~topic payload
    in
    Flow.copy_string pkt_data client.cxn
  | Atleast_once ->
    let id = Mqtt_packet.gen_id () in
    let cond = Condition.create () in
    let expected_ack_pkt = Mqtt_packet.puback id in
    Hashtbl.add client.inflight id (cond, expected_ack_pkt);
    let pkt_data =
      Mqtt_packet.Encoder.publish ~dup ~qos ~retain ~id ~topic payload
    in
    Flow.copy_string pkt_data client.cxn;
    Condition.await_no_mutex cond
  | Exactly_once ->
    let id = Mqtt_packet.gen_id () in
    let cond = Condition.create () in
    let expected_ack_pkt = Mqtt_packet.pubrec id in
    Hashtbl.add client.inflight id (cond, expected_ack_pkt);
    let pkt_data =
      Mqtt_packet.Encoder.publish ~dup ~qos ~retain ~id ~topic payload
    in
    Flow.copy_string pkt_data client.cxn;
    Condition.await_no_mutex cond;
    let expected_ack_pkt = Mqtt_packet.pubcomp id in
    Hashtbl.add client.inflight id (cond, expected_ack_pkt);
    let pkt_data = Mqtt_packet.Encoder.pubrel id in
    Flow.copy_string pkt_data client.cxn;
    Condition.await_no_mutex cond

let subscribe topics client =
  if topics = [] then raise (Invalid_argument "empty topics");
  let pkt_id = Mqtt_packet.gen_id () in
  let subscribe_packet = Mqtt_packet.Encoder.subscribe ~id:pkt_id topics in
  let qos_list = List.map (fun (_, q) -> Ok q) topics in
  let cond = Condition.create () in
  Hashtbl.add client.inflight pkt_id (cond, Suback (pkt_id, qos_list));
  wrap_catch client (fun () ->
      Flow.copy_string subscribe_packet client.cxn;
      Condition.await_no_mutex cond;
      let topics = List.map fst topics in
      Log.info (fun log ->
          log "[%s] Subscribed to %a." client.id Fmt.Dump.(list string) topics))

include Mqtt_core

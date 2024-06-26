open Eio

let port = ref 9000

let port () =
  port := !port + 1;
  !port

let timeout ~clock ?(t = 10.) () =
  Time.sleep clock t;
  Alcotest.fail "ERROR client timeout"

let read_char =
  let buf = Cstruct.create 1 in
  fun flow ->
    assert (Flow.single_read flow buf = 1);
    Cstruct.get_char buf 0

let read ~count in_ =
  let buf = Cstruct.create count in
  Flow.read_exact in_ buf;
  Cstruct.to_string buf

let write out str = Flow.copy_string str out
let write_char out ch = write out (String.of_seq Seq.(cons ch empty))

let establish_server ~net ~port callback =
  let cond = Condition.create () in
  let callback sock ch =
    callback sock ch;
    Condition.broadcast cond
  in
  let addr = `Tcp (Eio.Net.Ipaddr.V4.loopback, port) in
  Switch.run (fun sw ->
      let sock = Eio.Net.listen net ~sw ~reuse_addr:true ~backlog:5 addr in
      Fiber.fork_daemon ~sw (fun () ->
          Eio.Net.run_server sock callback ~on_error:(traceln "exc %a" Fmt.exn));
      Condition.await_no_mutex cond)

(** Module that gathers most io read and writes for the fake server *)
module Server = struct
  let qos_to_string = function
    | Mqtt_client.Atmost_once -> "\000"
    | Mqtt_client.Atleast_once -> "\001"
    | Mqtt_client.Exactly_once -> "\002"

  let qos_to_int = function
    | Mqtt_client.Atmost_once -> 0
    | Mqtt_client.Atleast_once -> 1
    | Mqtt_client.Exactly_once -> 2

  let utf_string_length in_ =
    let c1 = read_char in_ in
    let c2 = read_char in_ in
    (Char.code c1 * 256) + Char.code c2

  let check_connect in_ id =
    let l = String.length id in
    let c = read_char in_ in
    Alcotest.(check char "CONNECT fixed header" c '\016');
    let c = read_char in_ in
    Alcotest.(check int "CONNECT remaining length" (Char.code c) (12 + l));
    let s = read ~count:2 in_ in
    Alcotest.(check string "CONNECT var header - protocol length" s "\000\004");
    let s = read ~count:4 in_ in
    Alcotest.(check string "CONNECT var header - protocol name" s "MQTT");
    let c = read_char in_ in
    Alcotest.(check char "CONNECT var header - version" c '\004');
    let c = read_char in_ in
    Alcotest.(check char "CONNECT var header - connect flag" c '\002');
    let s = read ~count:2 in_ in
    Alcotest.(check string "CONNECT var header - connect flag" s "\000\030");
    let l' = utf_string_length in_ in
    Alcotest.(check int "CONNECT payload - client_id length" l' l);
    if l' > 0 then
      (* parse client id *)
      let s = read ~count:l in_ in
      Alcotest.(check string "CONNECT payload - client_id" s id)
    else
      (* empty client id *)
      Alcotest.(check string "CONNECT payload - empty client_id" id "")

  let answer_connack ?(result = "\000") out = write out ("\032\002\000" ^ result)

  let check_disconnect in_ _id =
    let _msg = Flow.read_all in_ in
    ()

  let check_publish_qos_0 in_ topic content =
    let lt = String.length topic in
    let lc = String.length content in
    let c = read_char in_ in
    Alcotest.(check char "PUBLISH Q0 fixed header" c '\048');
    let c = read_char in_ in
    Alcotest.(
      check int "PUBLISH Q0 remaining length" (Char.code c) (2 + lt + lc));
    let lt' = utf_string_length in_ in
    Alcotest.(check int "PUBLISH Q0 payload - topic length" lt' lt);
    (if lt' > 0 then
       (* parse client id *)
       let s = read ~count:lt in_ in
       Alcotest.(check string "PUBLISH Q0 payload - topic" s topic)
     else
       (* empty client id *)
       Alcotest.(check string "PUBLISH Q0 payload - empty topic" topic ""));
    let s = read ~count:lc in_ in
    Alcotest.(check string "PUBLISH Q0 content" s content)

  let check_publish_qos_1 in_ topic content =
    let lt = String.length topic in
    let lc = String.length content in
    let c = read_char in_ in
    Alcotest.(check char "PUBLISH Q1 fixed header" c '\050');
    let c = read_char in_ in
    Alcotest.(
      check int "PUBLISH Q1 remaining length" (Char.code c) (4 + lt + lc));
    let lt' = utf_string_length in_ in
    Alcotest.(check int "PUBLISH Q1 payload - topic length" lt' lt);
    (if lt' > 0 then
       (* parse client id *)
       let s = read ~count:lt in_ in
       Alcotest.(check string "PUBLISH Q1 payload - topic" s topic)
     else
       (* empty client id *)
       Alcotest.(check string "PUBLISH Q1 payload - empty topic" topic ""));
    let packet_id = read ~count:2 in_ in
    let s = read ~count:lc in_ in
    Alcotest.(check string "PUBLISH Q1 content" s content);
    packet_id

  let answer_puback out packet_id = write out ("\064\002" ^ packet_id)

  let check_publish_qos_2 in_ topic content =
    let lt = String.length topic in
    let lc = String.length content in
    let c = read_char in_ in
    Alcotest.(check char "PUBLISH Q2 fixed header" c '\052');
    let c = read_char in_ in
    Alcotest.(
      check int "PUBLISH Q2 remaining length" (Char.code c) (4 + lt + lc));
    let lt' = utf_string_length in_ in
    Alcotest.(check int "PUBLISH Q2 payload - topic length" lt' lt);
    (if lt' > 0 then
       (* parse client id *)
       let s = read ~count:lt in_ in
       Alcotest.(check string "PUBLISH Q2 payload - topic" s topic)
     else
       (* empty client id *)
       Alcotest.(check string "PUBLISH Q2 payload - empty topic" topic ""));
    let packet_id = read ~count:2 in_ in
    let s = read ~count:lc in_ in
    Alcotest.(check string "PUBLISH Q2 content" s content);
    packet_id

  let answer_pubrec out packet_id = write out ("\080\002" ^ packet_id)

  let check_pubrel in_ packet_id =
    let c = read_char in_ in
    Alcotest.(check char "PUBREL fixed header" c '\098');
    let c = read_char in_ in
    Alcotest.(check char "PUBREL remaining length" c '\002');
    let rel_packet_id = read ~count:2 in_ in
    Alcotest.(check string "PUBREL packet id" rel_packet_id packet_id)

  let answer_pubcomp out packet_id = write out ("\112\002" ^ packet_id)

  let answer_publish out topic content =
    let lc = String.length content in
    let l = Char.chr @@ (2 + String.length topic + lc) in
    write out "\048";
    write_char out l;
    let lc1 = Char.chr (lc / 256) in
    let lc2 = Char.chr (lc mod 256) in
    write_char out lc1;
    write_char out lc2;
    write out (topic ^ content)

  (* can only test SUBSCRIBE packet with one filter *)
  let check_subscribe in_ topic qos =
    let qos = qos_to_int qos in
    let l = String.length topic in
    let c = read_char in_ in
    Alcotest.(check char "SUBSCRIBE fixed header" c '\130');
    let c = read_char in_ in
    Alcotest.(check int "SUBSCRIBE remaining length" (Char.code c) (5 + l));
    let packet_id = read ~count:2 in_ in
    let l' = utf_string_length in_ in
    Alcotest.(check int "SUBSCRIBE payload - topic length" l' l);
    let s = read ~count:l in_ in
    Alcotest.(check string "SUBSCRIBE filter" s topic);
    let c = read_char in_ in
    Alcotest.(check int "SUBSCRIBE filter qos" (Char.code c) qos);
    packet_id

  let answer_suback out packet_id qos =
    let qos = qos_to_string qos in
    write out ("\144\003" ^ packet_id ^ qos)
end

(** Connect/Connack packet tests

    Tests on the mandatory normative statements on CONNECT/CONNACK packets which
    are meaningful on the client side. Here is a non-exhaustive list of the
    statements currently tested:

    MQTT 3.1.0-1: After a Network Connection is established by a Client to a
    Server, the first Packet sent from the Client to the Server MUST be a
    CONNECT Packet.

    MQTT 3.1.2-2: The Server MUST respond to the CONNECT Packet with a CONNACK
    return code 0x01 (unacceptable protocol level) and then disconnect the
    Client if the Protocol Level is not supported by the Server.

    MQTT 3.1.2-3: The Server MUST validate that the reserved flag in the CONNECT
    Control Packet is set to zero and disconnect the Client if it is not zero.

    MQTT 3.1.2-18: If the User Name Flag is set to 0, a user name MUST NOT be
    present in the payload.

    MQTT 3.1.2-20: If the Password Flag is set to 0, a password MUST NOT be
    present in the payload.

    MQTT 3.1.2-22: If the User Name Flag is set to 0, the Password Flag MUST be
    set to 0

    MQTT 3.1.3-3: The Client Identifier (ClientId) MUST be present and MUST be
    the first field in the CONNECT packet payload.

    MQTT 3.1.3-4: The ClientId MUST be a UTF-8 encoded string as defined in
    Section 1.5.3.

    MQTT 3.1.3-7: If the Client supplies a zero-byte ClientId, the Client MUST
    also set CleanSession to 1.

    MQTT 3.1.4-4: If CONNECT validation is successful the Server MUST
    acknowledge the CONNECT Packet with a CONNACK Packet containing a zero
    return code. *)

(* Test: Verify that the client succesfully sends a CONNECT packet and
   understands the answered CONNACK *)
let test_connect_success ~net ~clock _switch () =
  let id = "test_connect_success" in
  let port = port () in
  let server () =
    let callback flow _ =
      Server.check_connect flow id;
      Server.answer_connack flow;
      Server.check_disconnect flow id
    in
    establish_server ~net ~port callback;
    Printf.printf "Server stopped\n%!"
  in
  let client () =
    Switch.run (fun sw ->
        let client =
          Mqtt_client.connect ~sw ~net ~clock ~id ~port [ "0.0.0.0" ]
        in
        Mqtt_client.disconnect client);
    Printf.printf "Client stopped\n%!"
  in
  Fiber.first (fun () -> Fiber.both server client) (timeout ~clock)

(* Test: Verify that a CONNACK packet with return code of 0x01 is understood as
   a connection error *)
let test_connect_protocol_error ~net ~clock _switch () =
  let id = "test_connect_protocol_error" in
  let port = port () in
  let server () =
    let callback flow _ =
      Server.check_connect flow id;
      (* fake CONNACK code 0x01 *)
      Server.answer_connack ~result:"\001" flow
    in
    establish_server ~net callback
  in
  let client () =
    Switch.run (fun sw ->
        try
          (* attempt to connect *)
          let _client =
            Mqtt_client.connect ~sw ~net ~clock ~id ~port [ "0.0.0.0" ]
          in
          (* fail upon succesfull connect*)
          Alcotest.fail "CONNECT success"
        with exn ->
          (* verify that a Mqtt_client.Connection_error was correctly raised *)
          Alcotest.check
            (Alcotest.testable Fmt.exn ( = ))
            "CONNECT connection error" Mqtt_client.Connection_error exn)
  in
  Fiber.first
    (fun () -> Fiber.both (server ~port) client)
    (fun () -> timeout ~clock ())

(* Test: Verify that the client overrides the clean session flag if the client
   id is empty. *)
let test_connect_client_id_clean_session ~net ~clock _switch () =
  (* purposefully use an empty client id *)
  let id = "" in
  let port = port () in
  let server () =
    let callback flow _ =
      Server.check_connect flow id;
      Server.answer_connack flow;
      Server.check_disconnect flow id
    in
    establish_server ~port ~net callback
  in
  let client () =
    Switch.run (fun sw ->
        let client =
          Mqtt_client.connect ~sw ~net ~clock ~id ~port ~clean_session:false
            [ "0.0.0.0" ]
        in
        Mqtt_client.disconnect client)
  in
  Fiber.first (fun () -> Fiber.both server client) (fun () -> timeout ~clock ())

(** Publish packet tests

    Tests on the mandatory normative statements on PUBLISH/PUBACK/PUBREL/PUBREC/
    PUBREL/PUBCOMP packets which are meaningful on the client side. Here is a
    non-exhaustive list of the statements currently tested:

    MQTT 3.3.1-2: The DUP flag MUST be set to 0 for all QoS 0 messages.

    MQTT 3.3.2-1: The Topic Name MUST be present as the first field in the
    PUBLISH Packet Variable header. It MUST be a UTF-8 encoded string.

    MQTT 3.6.1-1: Bits 3,2,1 and 0 of the fixed header in the PUBREL Control
    Packet are reserved and MUST be set to 0,0,1 and 0 respectively. The Server
    MUST treat any other value as malformed and close the Network Connection.

    MQTT 4.3.1-1: In the QoS 0 delivery protocol, the Sender:

    - MUST send a PUBLISH packet with QoS=0, DUP=0.

    MQTT 4.3.2-1: In the QoS 1 delivery protocol, the Sender:

    - MUST assign an unused Packet Identifier each time it has a new Application
      Message to publish.
    - MUST send a PUBLISH Packet containing this Packet Identifier with QoS=1,
      DUP=0.
    - MUST treat the PUBLISH Packet as "unacknowledged" until it has received
      the corresponding PUBACK packet from the receiver. See Section 4.4 for a
      discussion of unacknowledged messages

    MQTT 4.3.2-2: In the QoS 1 delivery protocol, the Receiver:

    - MUST respond with a PUBACK Packet containing the Packet Identifier from
      the incoming PUBLISH Packet, having accepted ownership of the Application
      Message.
    - After it has sent a PUBACK Packet the Receiver MUST treat any incoming
      PUBLISH packet that contains the same Packet Identifier as being a new
      publication, irrespective of the setting of its DUP flag.

    MQTT 4.3.3-1: In the QoS 2 delivery protocol, the Sender:

    - MUST assign an unused Packet Identifier when it has a new Application
      Message to publish.
    - MUST send a PUBLISH packet containing this Packet Identifier with QoS=2,
      DUP=0.
    - MUST treat the PUBLISH packet as "unacknowledged" until it has received
      the corresponding PUBREC packet from the receiver. See Section 4.4 for a
      discussion of unacknowledged messages.
    - MUST send a PUBREL packet when it receives a PUBREC packet from the
      receiver. This PUBREL packet MUST contain the same Packet Identifier as
      the original PUBLISH packet.
    - MUST treat the PUBREL packet as "unacknowledged" until it has received the
      corresponding PUBCOMP packet from the receiver.
    - MUST NOT re-send the PUBLISH once it has sent the corresponding PUBREL
      packet

    MQTT 4.3.3-2: In the QoS 2 delivery protocol, the Receiver:

    - MUST respond with a PUBREC containing the Packet Identifier from the
      incoming PUBLISH Packet, having accepted ownership of the Application
      Message.
    - Until it has received the corresponding PUBREL packet, the Receiver MUST
      acknowledge any subsequent PUBLISH packet with the same Packet Identifier
      by sending a PUBREC. It MUST NOT cause duplicate messages to be delivered
      to any onward recipients in this case.
    - MUST respond to a PUBREL packet by sending a PUBCOMP packet containing the
      same Packet Identifier as the PUBREL.
    - After it has sent a PUBCOMP, the receiver MUST treat any subsequent
      PUBLISH packet that contains that Packet Identifier as being a new
      publication. *)

(* Test: Verify that PUBLISH packets with QoS 0 are sent with QoS= 0 & DUP=0 *)
let test_publish_qos_0 ~net ~clock _switch () =
  let id = "test_publish_qos_0" in
  let port = port () in
  let topic = "qos_0" in
  let content = "foo" in
  let server () =
    let callback flow _ =
      Server.check_connect flow id;
      Server.answer_connack flow;
      (* beginning of the test *)
      Server.check_publish_qos_0 flow topic content;
      Server.check_publish_qos_0 flow topic content;
      Server.check_disconnect flow id
    in
    establish_server ~net callback
  in
  let client () =
    Switch.run (fun sw ->
        let client =
          Mqtt_client.connect ~sw ~net ~clock ~id ~port [ "0.0.0.0" ]
        in
        Mqtt_client.publish ~qos:Atmost_once ~topic content client;
        (* force the dup flag to true to verify that it is set to 0 correctly *)
        Mqtt_client.publish ~dup:true ~qos:Atmost_once ~topic content client;
        Mqtt_client.disconnect client)
  in
  Fiber.first
    (fun () -> Fiber.both (server ~port) client)
    (fun () -> timeout ~clock ())

(* Test: Verify that PUBLISH packets with QoS 1 are sent and unacknowledged
   until a PUBACK packet is received *)
let test_publish_qos_1 ~net ~clock _switch () =
  let id = "test_publish_qos_1" in
  let port = port () in
  let topic = "qos_1" in
  let content = "foo" in
  let server () =
    let callback flow _ =
      Server.check_connect flow id;
      Server.answer_connack flow;
      (* beginning of the test *)
      let packet_id = Server.check_publish_qos_1 flow topic content in
      Server.answer_puback flow packet_id;
      Server.check_disconnect flow id
    in
    establish_server ~net callback
  in
  let client () =
    Switch.run (fun sw ->
        let client =
          Mqtt_client.connect ~sw ~net ~clock ~id ~port [ "0.0.0.0" ]
        in
        Mqtt_client.publish ~qos:Atleast_once ~topic content client;
        Mqtt_client.disconnect client)
  in
  Fiber.first
    (fun () -> Fiber.both (server ~port) client)
    (fun () -> timeout ~clock ())

(* Test: Verify that new packet ids are used instead of packet ids used by
   unacknowledged packets *)
let test_publish_qos_1_packet_id ~net ~clock _switch () =
  let id = "mtest_publish_qos_1_packet_id" in
  let port = port () in
  let topic = "qos_1" in
  let content = "foo" in
  let cond = Condition.create () in
  let server () =
    let callback flow _ =
      Server.check_connect flow id;
      Server.answer_connack flow;
      (* beginning of the test *)
      (* read first packet *)
      let packet_id_1 = Server.check_publish_qos_1 flow topic content in
      (* notify condition *)
      Condition.broadcast cond;
      (* read second packet - dont answer first packet yet *)
      let packet_id_2 = Server.check_publish_qos_1 flow topic content in
      (* answer both packets *)
      Server.answer_puback flow packet_id_1;
      Server.answer_puback flow packet_id_2;
      Server.check_disconnect flow id
    in
    establish_server ~net callback
  in
  let client () =
    Switch.run (fun sw ->
        let client =
          Mqtt_client.connect ~sw ~net ~clock ~id ~port [ "0.0.0.0" ]
        in
        (* send first packet*)
        let p1 =
          Fiber.fork_promise ~sw (fun () ->
              Mqtt_client.publish ~qos:Atleast_once ~topic content client)
        in
        (* wait that it is received *)
        Condition.await_no_mutex cond;
        (* send second packet and wait for the response *)
        Mqtt_client.publish ~qos:Atleast_once ~topic content client;
        (* wait for the first packet's response *)
        Promise.await_exn p1;
        Mqtt_client.disconnect client)
  in
  Fiber.first
    (fun () -> Fiber.both (server ~port) client)
    (fun () -> timeout ~clock ())

(* Test: Verify that PUBLISH packets with QoS 2 are sent and unacknowledged
   until a PUBREC packet is received, then followed by a PUBREL packet sent by
   the client, itself answered with a PUBCOMP packet *)
let test_publish_qos_2 ~net ~clock _switch () =
  let id = "test_publish_qos_2" in
  let port = port () in
  let topic = "qos_2" in
  let content = "foo" in
  let server () =
    let callback flow _ =
      Server.check_connect flow id;
      Server.answer_connack flow;
      (* beginning of the test *)
      let packet_id = Server.check_publish_qos_2 flow topic content in
      Server.answer_pubrec flow packet_id;
      Server.check_pubrel flow packet_id;
      Server.answer_pubcomp flow packet_id;
      Server.check_disconnect flow id
    in
    establish_server ~net callback
  in
  let client () =
    Switch.run (fun sw ->
        let client =
          Mqtt_client.connect ~sw ~net ~clock ~id ~port [ "0.0.0.0" ]
        in
        Mqtt_client.publish ~qos:Exactly_once ~topic content client;
        Mqtt_client.disconnect client)
  in
  Fiber.first
    (fun () -> Fiber.both (server ~port) client)
    (fun () -> timeout ~clock ())

(** Subscribe packet tests

    Tests on the mandatory normative statements on SUBSCRIBE packets which are
    meaningful on the client side. Here is a non-exhaustive list of the
    statements currently tested:

    MQTT 3.8.1-1: Bits 3,2,1 and 0 of the fixed header of the SUBSCRIBE Control
    Packet are reserved and MUST be set to 0,0,1 and 0 respectively. The Server
    MUST treat any other value as malformed and close the Network Connection.

    MQTT 3.8.3-1: The Topic Filters in a SUBSCRIBE packet payload MUST be UTF-8
    encoded strings as defined in Section 1.5.3.

    MQTT 3.8.3-3: The payload of a SUBSCRIBE packet MUST contain at least one
    Topic Filter / QoS pair. A SUBSCRIBE packet with no payload is a protocol
    violation.

    MQTT 3.8.3-4: The Server MUST treat a SUBSCRIBE packet as malformed and
    close the Network Connection if any of Reserved bits in the payload are
    non-zero, or QoS is not 0,1 or 2.

    MQTT 3.8.4-1: When the Server receives a SUBSCRIBE Packet from a Client, the
    Server MUST respond with a SUBACK Packet.

    MQTT 3.9.3-2: SUBACK return codes other than 0x00, 0x01, 0x02 and 0x80 are
    reserved and MUST NOT be used *)

(* Test: Verify that the client can subscribe to a topic and understand the
   answered SUBACK *)
let test_subscribe_sub ~net ~clock _switch () =
  let id = "test_subscribe_sub" in
  let port = port () in
  let topic = "sub" in
  let qos = Mqtt_client.Atmost_once in
  let server () =
    let callback flow _ =
      Server.check_connect flow id;
      Server.answer_connack flow;
      (* beginning of the test *)
      let packet_id = Server.check_subscribe flow topic qos in
      Server.answer_suback flow packet_id qos;
      Server.check_disconnect flow id
    in
    establish_server ~net callback
  in
  let client () =
    Switch.run (fun sw ->
        let client =
          Mqtt_client.connect ~sw ~net ~clock ~id ~port [ "0.0.0.0" ]
        in
        Mqtt_client.subscribe [ (topic, qos) ] client;
        Mqtt_client.disconnect client)
  in
  Fiber.first
    (fun () -> Fiber.both (server ~port) client)
    (fun () -> timeout ~clock ())

(* Test: Verify that the client cannot send subscriptions without any topics *)
let test_subscribe_empty_sub ~net ~clock _switch () =
  let id = "test_subscribe_sub" in
  let port = port () in
  let server () =
    let callback flow _ =
      Server.check_connect flow id;
      Server.answer_connack flow;
      (* beginning of the test *)
      ()
    in
    establish_server ~net callback
  in
  let client () =
    Switch.run (fun sw ->
        try
          (* attempt to connect *)
          let client =
            Mqtt_client.connect ~sw ~net ~clock ~id ~port [ "0.0.0.0" ]
          in
          Mqtt_client.subscribe [] client;
          (* fail upon succesfull subscribe*)
          Alcotest.fail "CONNECT success"
        with exn ->
          (* an exception should be raised *)
          Alcotest.check
            (Alcotest.testable Fmt.exn ( = ))
            "SUBSCRIBE empty topics" (Invalid_argument "empty topics") exn)
  in
  Fiber.first
    (fun () -> Fiber.both (server ~port) client)
    (fun () -> timeout ~clock ())

(* Test: Verify that the client calls the on_message callback when a message is
   received *)
let test_subscribe_msg ~net ~clock _switch () =
  let id = "test_subscribe_msg" in
  let port = port () in
  let topic = "sub" in
  let content = "foo" in
  let server () =
    let callback flow _ =
      Server.check_connect flow id;
      Server.answer_connack flow;
      (* beginning of the test *)
      (* add a message *)
      Server.answer_publish flow topic content;
      Server.check_disconnect flow id
    in
    establish_server ~net callback
  in
  let client () =
    Switch.run (fun sw ->
        let cond = Condition.create () in
        let on_message ~topic:topic' str =
          Alcotest.(check string "PUBLISH topic" topic' topic);
          Alcotest.(check string "PUBLISH content" str content);
          Condition.broadcast cond
        in
        let client =
          Mqtt_client.connect ~sw ~net ~clock ~id ~port ~on_message
            [ "0.0.0.0" ]
        in
        Condition.await_no_mutex cond;
        Mqtt_client.disconnect client)
  in
  Fiber.first
    (fun () -> Fiber.both (server ~port) client)
    (fun () -> timeout ~clock ())

(* Run the tests *)
let () =
  let open Alcotest in
  Eio_main.run (fun env ->
      let test_case a b f =
        let f = f ~net:(Stdenv.net env) ~clock:(Stdenv.clock env) () in
        test_case a b f
      in
      run "Mqtt"
        [
          ( "connect",
            [
              test_case "Connect success" `Quick test_connect_success;
              test_case "Connect protocol error" `Quick
                test_connect_protocol_error;
              test_case "Connect with empty client id" `Quick
                test_connect_client_id_clean_session;
            ] );
          ( "publish",
            [
              test_case "Publish qos 0" `Quick test_publish_qos_0;
              test_case "Publish qos 1" `Quick test_publish_qos_1;
              test_case "Multiple publish qos 1" `Quick
                test_publish_qos_1_packet_id;
              test_case "Publish qos 2" `Quick test_publish_qos_2;
            ] );
          ( "subscribe",
            [
              test_case "Subscribe to a topic" `Quick test_subscribe_sub;
              test_case "Subscribe without topics" `Slow
                test_subscribe_empty_sub;
              test_case "Receive publish message" `Quick test_subscribe_msg;
            ] );
        ])

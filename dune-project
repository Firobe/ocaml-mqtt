(lang dune 3.13)

(name mqtt-client)

(generate_opam_files true)

(source
 (github Firobe/ocaml-mqtt))

(authors "Rizo I. <rizo@odis.io>" "Josh Allmann <joshua.allmann@gmail.com>")

(maintainers "Rizo I. <rizo@odis.io>" "Leo Soares <leojrfs@gmail.com>")

(license BSD-3-clause)

(documentation https://hyper-systems.github.io/ocaml-mqtt/mqtt-client)

(package
 (name mqtt-client)
 (synopsis "MQTT client implementation for OCaml and ReasonML")
 (depends
  (ocaml
   (>= 4.08.0))
  dune
  (ocplib-endian
   (>= 0.6))
  (logs
   (>= 0.7.0))
  (fmt
   (>= 0.8.7))
  eio
  tls-eio
  (tls
   (>= 0.17.3))
  (odoc
   (and
    :with-doc
    (>= 2.4.1)))))

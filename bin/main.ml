(* mon — a blackbox availability/throughput prober.

   A hostname usually publishes several endpoints (multiple A/AAAA records,
   behind round-robin DNS / anycast / several backends). A plain
   [curl https://host] lets the resolver and curl pick one, which can silently
   hide a single dead backend. So, like ocurrent-observer, for each target we
   resolve every published address and probe each one individually with
   [curl --resolve host:port:ip ...], pinning the connection to that endpoint
   while keeping SNI / Host / certificate validation correct.

   The results — outcome, last duration / throughput, HTTP and curl exit codes —
   are exposed as Prometheus metrics on [/metrics], labelled by the specific
   [ip] (and [family]) so per-endpoint reliability is visible. *)

open Prometheus

let namespace = "probe"

(* Per-endpoint labels: the target URL, the specific resolved IP, and family. *)
let base = [ "target"; "ip"; "family" ]

let success_g =
  Gauge.v_labels ~label_names:base ~namespace
    ~help:"1 if the last probe of this endpoint succeeded, 0 otherwise" "success"

let http_status_g =
  Gauge.v_labels ~label_names:base ~namespace
    ~help:"HTTP status code of the last probe (0 if no response)"
    "http_status_code"

let exit_code_g =
  Gauge.v_labels ~label_names:base ~namespace
    ~help:"curl exit code of the last probe (0 = success, 28 = timeout, ...)"
    "curl_exit_code"

let size_g =
  Gauge.v_labels ~label_names:base ~namespace
    ~help:"Number of bytes downloaded by the last probe" "download_size_bytes"

let attempts_total =
  Counter.v_labels ~label_names:base ~namespace
    ~help:"Total number of probe attempts" "attempts_total"

let failures_total =
  Counter.v_labels ~label_names:(base @ [ "reason" ]) ~namespace
    ~help:"Total number of failed probes, labelled by curl exit code"
    "failures_total"

(* Last-value gauges: the actual measurement from the most recent probe. More
   intuitive than histogram quantiles for a once-per-interval probe — plotted
   directly, each point is the real duration/throughput at that moment. *)
let last_duration_g =
  Gauge.v_labels ~label_names:base ~namespace
    ~help:"Total request time of the last probe, in seconds" "last_duration_seconds"

let last_speed_g =
  Gauge.v_labels ~label_names:base ~namespace
    ~help:"Download throughput of the last probe, in bytes/second"
    "last_download_speed_bytes_per_second"

(* Target-level DNS metrics (resolution is done in-process, before pinning). *)
let dns_success_g =
  Gauge.v_label ~label_name:"target" ~namespace
    ~help:"1 if the last DNS resolution of the target succeeded" "dns_success"

let dns_addresses_g =
  Gauge.v_label ~label_name:"target" ~namespace
    ~help:"Number of endpoints (A + AAAA records) the target last resolved to"
    "dns_addresses"

(* curl -w template: one [key=value] per line so we can parse it back. *)
let write_out =
  String.concat ""
    [ "code=%{http_code}\n"; "total=%{time_total}\n"; "size=%{size_download}\n";
      "speed=%{speed_download}\n" ]

type family = V4 | V6

let family_to_string = function V4 -> "ipv4" | V6 -> "ipv6"

(* Which address families to probe (ping-style -4/-6; default both). *)
type proto = Any | Only_v4 | Only_v6

let keep_family = function
  | Any -> fun _ -> true
  | Only_v4 -> ( function V4 -> true | V6 -> false)
  | Only_v6 -> ( function V6 -> true | V4 -> false)

(* [pinned] true: probe this exact IP via curl --resolve (one series per IP,
   for fixed backends). false: "pooled" — let curl pick an IP for the family
   (CDN / load-balanced hosts with rotating IPs), labelled ip="pool". *)
type endpoint = {
  url : string;
  host : string;
  port : int;
  ip : string;
  family : family;
  pinned : bool;
}

(* Parse curl's [-w] output ([key=value] per line) into an assoc list. *)
let parse out =
  String.split_on_char '\n' out
  |> List.filter_map (fun line ->
         match String.index_opt line '=' with
         | Some i ->
             let k = String.trim (String.sub line 0 i) in
             let v = String.trim (String.sub line (i + 1) (String.length line - i - 1)) in
             Some (k, v)
         | None -> None)

let getf fields k =
  match List.assoc_opt k fields with
  | Some s -> Option.value ~default:0. (float_of_string_opt s)
  | None -> 0.

(* Run curl once against [url], pinned to a single endpoint via --resolve.
   [-f] makes an HTTP >= 400 a failure, mirroring [curl -fsSL]. *)
let run_curl ~mgr ~timeout ~mode url =
  let addr_args =
    match mode with
    | `Pinned resolve -> [ "--resolve"; resolve ]
    | `Family V4 -> [ "-4" ]
    | `Family V6 -> [ "-6" ]
  in
  let out = Buffer.create 512 and err = Buffer.create 256 in
  let status =
    Eio.Switch.run @@ fun sw ->
    let child =
      Eio.Process.spawn ~sw mgr
        ~stdout:(Eio.Flow.buffer_sink out)
        ~stderr:(Eio.Flow.buffer_sink err)
        ([ "curl"; "-f"; "-s"; "-S"; "-L" ] @ addr_args
        @ [ "--max-time"; Printf.sprintf "%g" timeout; "-o"; "/dev/null"; "-w";
            write_out; url ])
    in
    Eio.Process.await child
  in
  let exit_code = match status with `Exited n -> n | `Signaled n -> 128 + n in
  (exit_code, parse (Buffer.contents out), Buffer.contents err)

let probe_endpoint ~mgr ~timeout e =
  let l = [ e.url; e.ip; family_to_string e.family ] in
  Counter.inc_one (Counter.labels attempts_total l);
  let mode =
    if e.pinned then
      let addr = match e.family with V6 -> "[" ^ e.ip ^ "]" | V4 -> e.ip in
      `Pinned (Printf.sprintf "%s:%d:%s" e.host e.port addr)
    else `Family e.family
  in
  let exit_code, t, stderr = run_curl ~mgr ~timeout ~mode e.url in
  let success = exit_code = 0 in
  Gauge.set (Gauge.labels success_g l) (if success then 1. else 0.);
  Gauge.set (Gauge.labels exit_code_g l) (float_of_int exit_code);
  Gauge.set (Gauge.labels http_status_g l) (getf t "code");
  Gauge.set (Gauge.labels size_g l) (getf t "size");
  if not success then
    Counter.inc_one (Counter.labels failures_total (l @ [ string_of_int exit_code ]));

  let total = getf t "total" in
  Gauge.set (Gauge.labels last_duration_g l) total;
  Gauge.set (Gauge.labels last_speed_g l) (getf t "speed");

  if success then
    Logs.info (fun f ->
        f "%s [%s] ok: %.0fB in %.3fs (%.0f B/s, http %.0f)" e.url e.ip
          (getf t "size") total (getf t "speed") (getf t "code"))
  else
    Logs.warn (fun f ->
        f "%s [%s] FAILED: curl exit %d (http %.0f) after %.3fs%s" e.url e.ip
          exit_code (getf t "code") total
          (if stderr = "" then "" else ": " ^ String.trim stderr))

let split_url url =
  let uri = Uri.of_string url in
  let scheme = Option.value ~default:"https" (Uri.scheme uri) in
  let host = match Uri.host uri with Some h -> h | None -> url in
  let port =
    match Uri.port uri with Some p -> p | None -> if scheme = "http" then 80 else 443
  in
  (host, port)

(* Resolve [url]'s host, then probe. [pinned] targets get one probe per
   resolved IP (labelled by ip); [pooled] (CDN/LB) targets get one probe per
   address family with curl resolving, labelled ip="pool" — bounded cardinality
   despite rotating IPs, while still distinguishing IPv4/IPv6 reachability. *)
let probe_target ~pooled ~keep ~net ~mgr ~timeout url =
  let host, port = split_url url in
  match Eio.Net.getaddrinfo_stream ~service:(string_of_int port) net host with
  | exception (Eio.Cancel.Cancelled _ as exn) -> raise exn
  | exception ex ->
      Gauge.set (dns_success_g url) 0.;
      Gauge.set (dns_addresses_g url) 0.;
      Logs.warn (fun f -> f "%s DNS resolution failed: %a" url Fmt.exn ex)
  | addrs ->
      let resolved =
        List.filter_map (function `Tcp (ip, _) -> Some ip | _ -> None) addrs
        |> List.map (fun ip ->
               let family = Eio.Net.Ipaddr.fold ~v4:(fun _ -> V4) ~v6:(fun _ -> V6) ip in
               (Fmt.to_to_string Eio.Net.Ipaddr.pp ip, family))
        |> List.sort_uniq compare
        |> List.filter (fun (_, family) -> keep family)
      in
      let endpoints =
        if pooled then
          List.map snd resolved |> List.sort_uniq compare
          |> List.map (fun family -> { url; host; port; ip = "pool"; family; pinned = false })
        else
          List.map
            (fun (ip, family) -> { url; host; port; ip; family; pinned = true })
            resolved
      in
      Gauge.set (dns_success_g url) 1.;
      Gauge.set (dns_addresses_g url) (float_of_int (List.length endpoints));
      Eio.Fiber.List.iter (probe_endpoint ~mgr ~timeout) endpoints

let rec target_loop ~pooled ~keep ~clock ~net ~mgr ~interval ~timeout url =
  (try probe_target ~pooled ~keep ~net ~mgr ~timeout url
   with
   | Eio.Cancel.Cancelled _ as exn -> raise exn
   | ex ->
     Gauge.set (dns_success_g url) 0.;
     Logs.err (fun f -> f "probe %s raised: %a" url Fmt.exn ex));
  Eio.Time.sleep clock interval;
  target_loop ~pooled ~keep ~clock ~net ~mgr ~interval ~timeout url

let setup_log style_renderer level =
  Fmt_tty.setup_std_outputs ?style_renderer ();
  Logs.set_level level;
  Logs.set_reporter (Logs_fmt.reporter ())

let main () targets pooled_targets interval timeout port proto remote_write auth
    rw_insecure extra_labels =
  Mirage_crypto_rng_unix.use_default ();
  if targets = [] && pooled_targets = [] then
    Logs.warn (fun f -> f "no targets given (use --target / --pooled-target)");
  let keep = keep_family proto in
  Eio_main.run @@ fun env ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let mgr = Eio.Stdenv.process_mgr env in
  Eio.Switch.run @@ fun sw ->
  let socket =
    Eio.Net.listen ~reuse_addr:true ~backlog:128 ~sw net
      (`Tcp (Eio.Net.Ipaddr.V4.any, port))
  in
  let server = Cohttp_eio.Server.make ~callback:Prometheus_eio.callback () in
  let serve () =
    Logs.info (fun f -> f "Serving metrics on http://0.0.0.0:%d/metrics" port);
    Cohttp_eio.Server.run socket server ~on_error:(fun ex ->
        Logs.warn (fun f -> f "http: %a" Fmt.exn ex))
  in
  let probes =
    List.map
      (fun url () -> target_loop ~pooled:false ~keep ~clock ~net ~mgr ~interval ~timeout url)
      targets
    @ List.map
        (fun url () -> target_loop ~pooled:true ~keep ~clock ~net ~mgr ~interval ~timeout url)
        pooled_targets
  in
  let push =
    match remote_write with
    | None -> []
    | Some url ->
        [ (fun () ->
            Remote_write.run ~clock ~net ~url ~auth ~insecure:rw_insecure ~interval
              ~extra_labels ()) ]
  in
  Logs.info (fun f ->
      f "Probing %d pinned + %d pooled target(s) every %gs (curl --max-time %gs)"
        (List.length targets) (List.length pooled_targets) interval timeout);
  Eio.Fiber.all ((serve :: probes) @ push)

open Cmdliner

(* "NAME=VALUE" / "USER:PASS" parsed (and validated) at the cmdliner layer, so
   malformed input is a usage error rather than a silent runtime skip. *)
let pair_conv sep ~docv ~hide_value =
  let parse s =
    match String.index_opt s sep with
    | Some i when i > 0 ->
        Ok (String.sub s 0 i, String.sub s (i + 1) (String.length s - i - 1))
    | _ -> Error (`Msg (Printf.sprintf "%S is not of the form %s" s docv))
  in
  let print ppf (k, v) =
    Format.fprintf ppf "%s%c%s" k sep (if hide_value then "****" else v)
  in
  Arg.conv ~docv (parse, print)

let kv_conv = pair_conv '=' ~docv:"NAME=VALUE" ~hide_value:false
let userpass_conv = pair_conv ':' ~docv:"USER:PASS" ~hide_value:true

let setup_log_t =
  Term.(const setup_log $ Fmt_cli.style_renderer () $ Logs_cli.level ())

let targets =
  Arg.(
    value & opt_all string []
    & info [ "t"; "target" ] ~docv:"URL"
        ~doc:
          "URL to probe per published IP (repeatable). For CDN / load-balanced \
           hosts whose IPs rotate, use --pooled-target instead.")

let interval =
  Arg.(
    value & opt float 30.
    & info [ "i"; "interval" ] ~docv:"SECONDS" ~absent:"30s"
        ~doc:"Seconds between probes of each target.")

let timeout =
  Arg.(
    value & opt float 30.
    & info [ "timeout" ] ~docv:"SECONDS" ~absent:"30s"
        ~doc:"curl --max-time: a probe taking longer is counted as a failure.")

let port =
  Arg.(
    value & opt int 9686
    & info [ "p"; "port" ] ~docv:"PORT" ~absent:"9686"
        ~doc:"Port to serve /metrics on.")

let pooled_targets =
  Arg.(
    value & opt_all string []
    & info [ "pooled-target" ] ~docv:"URL"
        ~doc:
          "CDN / load-balanced target: probe once per address family (curl \
           resolves; no per-IP pinning, labelled ip=pool). Use for hosts whose \
           IPs rotate, to avoid unbounded ip-label churn. Repeatable.")

let proto =
  Arg.(
    value
    & vflag Any
        [ (Only_v4, info [ "4" ] ~doc:"Probe IPv4 (A) endpoints only (default: both families).");
          (Only_v6, info [ "6" ] ~doc:"Probe IPv6 (AAAA) endpoints only (default: both families).") ])

let remote_write =
  Arg.(
    value & opt (some string) None
    & info [ "remote-write" ] ~docv:"URL"
        ~doc:
          "If set, also push metrics via Prometheus remote_write to this URL \
           (e.g. http://central:9090/api/v1/write). Snapshot cadence follows \
           --interval.")

let remote_write_auth =
  Arg.(
    value & opt (some userpass_conv) None
    & info [ "remote-write-auth" ] ~docv:"USER:PASS"
        ~doc:"HTTP basic-auth credentials for the remote_write endpoint.")

let remote_write_insecure =
  Arg.(
    value & flag
    & info [ "remote-write-insecure" ]
        ~doc:
          "Skip TLS certificate verification for the remote_write endpoint \
           (self-signed / internal PKI / testing only).")

let external_labels =
  Arg.(
    value & opt_all kv_conv []
    & info [ "external-label" ] ~docv:"NAME=VALUE"
        ~doc:
          "Label added to every remote_write series (repeatable), e.g. \
           vantage=poland.")

let cmd =
  let doc = "Blackbox availability/throughput prober exposing Prometheus metrics" in
  let man =
    [ `S Manpage.s_examples;
      `P "Probe two sites locally and serve /metrics:";
      `Pre "  mon -t https://get.dune.build/install -t https://opam.ocaml.org";
      `P "Push to a central Prometheus as a named vantage:";
      `Pre
        "  mon -t https://get.dune.build/install \
         --remote-write=https://mon.example.com/api/v1/write \
         --remote-write-auth=mon:secret --external-label=vantage=home" ]
  in
  let info = Cmd.info "mon" ~doc ~man in
  Cmd.v info
    Term.(const main $ setup_log_t $ targets $ pooled_targets $ interval $ timeout
         $ port $ proto $ remote_write $ remote_write_auth $ remote_write_insecure
         $ external_labels)

let () = exit (Cmd.eval cmd)

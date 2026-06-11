(* In-process Prometheus remote-write client.

   The prober snapshots its own registry on a timer, encodes a prompb
   [WriteRequest] (via the generated {!Mon_pb.Remote} encoders), Snappy-compresses
   it, and POSTs to a central receiver's [/api/v1/write]. A bounded FIFO decouples
   production from sending: the sender drains it front-first, retrying transient
   (5xx / network) failures with backoff and dropping permanently-rejected (4xx)
   batches — so a central/link outage buffers and backfills on recovery, and a
   poison batch can't wedge the queue.

   Snapshot interval = probe interval, so there's a single rate to reason about.
   Connections are pull-style locally (we read our own registry); only the push
   across the boundary is remote_write. *)

module Pb = Mon_pb.Remote

(* --- registry snapshot -> prompb TimeSeries list --- *)

let timeseries_of_snapshot ~ts_ms ~extra_labels snapshot =
  let open Prometheus in
  MetricFamilyMap.fold
    (fun info label_map acc ->
      let mname = (info.MetricInfo.name :> string) in
      let lnames =
        List.map (fun (ln : LabelName.t) -> (ln :> string)) info.MetricInfo.label_names
      in
      LabelSetMap.fold
        (fun lvalues sample_set acc ->
          List.fold_left
            (fun acc (s : Sample_set.sample) ->
              let metric_labels = List.combine lnames lvalues in
              let bucket =
                match s.Sample_set.bucket with
                | Some (ln, v) -> [ ((ln :> string), Printf.sprintf "%g" v) ]
                | None -> []
              in
              let labels =
                (("__name__", mname ^ s.Sample_set.ext) :: metric_labels)
                @ bucket @ extra_labels
                |> List.sort (fun (a, _) (b, _) -> String.compare a b)
                |> List.map (fun (name, value) -> Pb.make_label ~name ~value ())
              in
              let samples = [ Pb.make_sample ~value:s.Sample_set.value ~timestamp:ts_ms () ] in
              Pb.make_time_series ~labels ~samples () :: acc)
            acc sample_set)
        label_map acc)
    snapshot []

(* Snapshot the default registry into a ready-to-POST (protobuf + snappy) body. *)
let build_payload ~clock ~extra_labels () =
  let ts_ms = Int64.of_float (Eio.Time.now clock *. 1000.) in
  let snapshot = Prometheus.CollectorRegistry.(collect default) in
  match timeseries_of_snapshot ~ts_ms ~extra_labels snapshot with
  | [] -> None
  | timeseries ->
      let enc = Pbrt.Encoder.create () in
      Pb.encode_pb_write_request (Pb.make_write_request ~timeseries ()) enc;
      Some (Snappy.compress (Pbrt.Encoder.to_string enc))

(* --- HTTP --- *)

(* TLS for https:// remote-write URLs (e.g. a home/edge prober pushing over the
   internet to a Caddy-fronted central). Verifies against system CAs;
   [~insecure] skips verification for self-signed / internal-PKI / testing. *)
let make_tls_config ~insecure =
  let authenticator =
    if insecure then Ok (fun ?ip:_ ~host:_ _ -> Ok None : X509.Authenticator.t)
    else Ca_certs.authenticator ()
  in
  match authenticator with
  | Error (`Msg m) -> failwith ("remote_write TLS authenticator: " ^ m)
  | Ok authenticator -> (
      match Tls.Config.client ~authenticator () with
      | Ok c -> c
      | Error (`Msg m) -> failwith ("remote_write TLS config: " ^ m))

let https tls_config uri raw_flow =
  let host =
    match Option.bind (Uri.host uri) (fun h -> Result.to_option (Domain_name.of_string h)) with
    | Some d -> Result.to_option (Domain_name.host d)
    | None -> None
  in
  Tls_eio.client_of_flow tls_config ?host raw_flow

let make_client ~insecure net =
  Cohttp_eio.Client.make ~https:(Some (https (make_tls_config ~insecure))) net

let make_headers auth =
  let h =
    Http.Header.of_list
      [ ("content-type", "application/x-protobuf");
        ("content-encoding", "snappy");
        ("x-prometheus-remote-write-version", "0.1.0");
        ("user-agent", "mon-remote-write") ]
  in
  match auth with
  | Some (user, pass) -> Cohttp.Header.add_authorization h (`Basic (user, pass))
  | None -> h

let drain (body : Cohttp_eio.Body.t) =
  ignore (Eio.Buf_read.parse_exn Eio.Buf_read.take_all body ~max_size:1_000_000)

(* One POST attempt. Fresh switch per request so the connection is closed after.
   [`Ok] = accepted, [`Retry] = transient (retry), [`Drop] = permanent (discard). *)
let post ~headers client uri payload =
  Eio.Switch.run @@ fun sw ->
  let resp, body =
    Cohttp_eio.Client.post ~headers ~body:(Cohttp_eio.Body.of_string payload) client ~sw uri
  in
  drain body;
  let code = Http.Status.to_int (Http.Response.status resp) in
  if code >= 200 && code < 300 then `Ok
  else if code = 429 || code >= 500 then `Retry code
  else `Drop code

(* --- bounded FIFO, drop-oldest on overflow --- *)

module Fifo = struct
  type t = {
    q : string Queue.t;
    mutex : Eio.Mutex.t;
    cond : Eio.Condition.t;
    cap : int;
  }

  let create cap =
    { q = Queue.create (); mutex = Eio.Mutex.create (); cond = Eio.Condition.create (); cap }

  let push t x =
    let dropped =
      Eio.Mutex.use_rw ~protect:true t.mutex (fun () ->
          Queue.push x t.q;
          let d = ref 0 in
          while Queue.length t.q > t.cap do
            ignore (Queue.pop t.q);
            incr d
          done;
          !d)
    in
    Eio.Condition.broadcast t.cond;
    if dropped > 0 then
      Logs.warn (fun f -> f "remote_write queue full: dropped %d oldest batch(es)" dropped)

  (* Blocking pop of the oldest entry. *)
  let pop t =
    Eio.Mutex.use_rw ~protect:false t.mutex (fun () ->
        while Queue.is_empty t.q do
          Eio.Condition.await t.cond t.mutex
        done;
        Queue.pop t.q)
end

(* --- fibers --- *)

let rec send_with_retry ~clock ~headers client uri payload backoff =
  match post ~headers client uri payload with
  | `Ok -> ()
  | `Drop code ->
      Logs.warn (fun f -> f "remote_write rejected (HTTP %d) — dropping batch" code)
  | `Retry code ->
      Logs.debug (fun f -> f "remote_write HTTP %d, retry in %.0fs" code backoff);
      Eio.Time.sleep clock backoff;
      send_with_retry ~clock ~headers client uri payload (Float.min (backoff *. 2.) 60.)
  | exception (Eio.Cancel.Cancelled _ as e) -> raise e
  | exception ex ->
      Logs.debug (fun f -> f "remote_write send failed (%a), retry in %.0fs" Fmt.exn ex backoff);
      Eio.Time.sleep clock backoff;
      send_with_retry ~clock ~headers client uri payload (Float.min (backoff *. 2.) 60.)

let rec sender ~clock ~headers client uri fifo =
  send_with_retry ~clock ~headers client uri (Fifo.pop fifo) 1.;
  sender ~clock ~headers client uri fifo

let rec producer ~clock ~extra_labels ~interval fifo =
  (match build_payload ~clock ~extra_labels () with
   | None -> ()
   | Some payload -> Fifo.push fifo payload
   | exception (Eio.Cancel.Cancelled _ as e) -> raise e
   | exception ex -> Logs.err (fun f -> f "remote_write snapshot failed: %a" Fmt.exn ex));
  Eio.Time.sleep clock interval;
  producer ~clock ~extra_labels ~interval fifo

(* Start the producer + sender. Blocks; the caller adds it to its [Fiber.all]. *)
let run ~clock ~net ~url ~auth ~insecure ~interval ~extra_labels () =
  let uri = Uri.of_string url in
  let headers = make_headers auth in
  let client = make_client ~insecure net in
  let fifo = Fifo.create 10_000 in
  Logs.info (fun f -> f "remote_write -> %s every %gs%s" url interval
    (match auth with Some (u, _) -> Printf.sprintf " (basic auth as %s)" u | None -> ""));
  Eio.Fiber.both
    (fun () -> producer ~clock ~extra_labels ~interval fifo)
    (fun () -> sender ~clock ~headers client uri fifo)

# mon

A small blackbox availability/throughput prober for remote endpoints. It
periodically drives `curl` against each configured URL — the same client whose
failures we want to characterise — captures the phase timings, throughput and
outcome, and exposes them as Prometheus metrics on `/metrics`. A bundled
Prometheus + Grafana stack turns those samples into availability histograms and
latency/throughput distributions over time.

It exists because a one-off `dig`/`curl` (e.g. in an ocurrent pipeline) only
tells you the *current* status. To answer "how unreliable is `get.dune.build`,
actually?" you need a stream of independent samples accumulated over time.

## Probe every published endpoint

A hostname usually publishes several endpoints — multiple A/AAAA records behind
round-robin DNS, anycast, or several backends. A plain `curl https://host` lets
the resolver and curl pick *one*, which can silently hide a single dead backend
(curl/glibc happily falls back to a working address and reports success). So,
following the pattern in `ocurrent-observer`, for each target `mon` resolves
**every** published address and probes each one individually with
`curl --resolve host:port:ip …`, pinning the connection to that endpoint while
keeping SNI / Host / certificate validation correct. Metrics are labelled by the
specific `ip` so per-endpoint reliability is visible.

(For example, `get.dune.build`'s IPv4 endpoint can be up while its IPv6 endpoint
is unreachable — a difference invisible to a single `curl`.)

## Why curl rather than a native OCaml HTTP client

The thing being measured is literally the `curl … | sh` install path. Driving
`curl` reproduces the exact client behaviour and exit codes (notably exit `28`,
the connect/operation timeout seen on `get.dune.build`) and gives accurate
per-phase timing via curl's `-w` template, rather than approximating them with a
different HTTP stack. The OCaml daemon (Eio) owns DNS resolution, scheduling,
aggregation and metric exposition.

## Metrics

Per-endpoint series carry `target` (the URL), `ip`, and `family` (`ipv4`/`ipv6`).

| Metric | Type | Meaning |
| --- | --- | --- |
| `probe_success` | gauge | `1` if the last probe of this endpoint succeeded, else `0` |
| `probe_attempts_total` | counter | Total probe attempts |
| `probe_failures_total` | counter | Failed probes, with `reason` = curl exit code |
| `probe_http_status_code` | gauge | HTTP status of the last probe (`0` = no response) |
| `probe_curl_exit_code` | gauge | curl exit code of the last probe |
| `probe_download_size_bytes` | gauge | Bytes downloaded by the last probe |
| `probe_duration_seconds` | histogram | Total request time |
| `probe_phase_duration_seconds` | histogram | Per-`phase` time: `resolve`, `connect`, `tls`, `processing`, `transfer` |
| `probe_download_speed_bytes_per_second` | histogram | Mean throughput |

Target-level series carry only `target`:

| Metric | Type | Meaning |
| --- | --- | --- |
| `probe_dns_success` | gauge | `1` if the target's last DNS resolution succeeded |
| `probe_dns_addresses` | gauge | Number of endpoints (A + AAAA) resolved |
| `probe_dns_lookup_seconds` | histogram | DNS resolution time |

Because connections are pinned with `--resolve`, curl's own DNS phase is ~0; the
real DNS signal is `probe_dns_lookup_seconds`. Useful curl exit codes in
`probe_failures_total{reason=...}`: `6` DNS, `7` connection refused, `22` HTTP
≥ 400 (because we run `curl -f`), `28` timeout, `35` TLS handshake.

## Run it

### Whole stack (Docker Compose)

Copy the example config (these hold no secrets in git — you fill them in):

```sh
cp .env.example .env                                                   # set passwords
cp docker-compose.example.yml docker-compose.yml                       # set your targets
cp prometheus/web.example.yml prometheus/web.yml                       # bcrypt of PROM password
cp grafana/provisioning/datasources/datasource.yml.example \
   grafana/provisioning/datasources/datasource.yml
cp alertmanager/alertmanager.example.yml alertmanager/alertmanager.yml
cp alertmanager/slack_url.example alertmanager/slack_url               # your Slack webhook
docker compose up -d --build
```

- Grafana:    http://localhost:3000  (admin / `$GF_ADMIN_PASSWORD`) — dashboard "mon — endpoint reliability" is auto-provisioned
- Prometheus: http://localhost:9090  (basic auth — see `prometheus/web.yml`)
- Prober:     http://localhost:9686/metrics

Add or change targets in `docker-compose.yml` under the `mon` service
`command:` (one `--target=URL` per endpoint), then `docker compose up -d`.

### Prober alone

```sh
day10 build .
./_build/default/bin/main.exe \
  --target https://get.dune.build/install \
  --interval 15 --timeout 30 --port 9686
curl -s localhost:9686/metrics | grep '^probe_'
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--target` / `-t` | (none) | URL to probe per published A/AAAA endpoint (repeatable) |
| `--pooled-target` | (none) | URL to probe once per address family (curl resolves; `ip=pool`). For CDN/load-balanced hosts with rotating IPs — avoids unbounded `ip`-label churn (repeatable) |
| `--interval` / `-i` | `30` | Seconds between probes of each target |
| `--timeout` | `30` | curl `--max-time`; a slower probe counts as a failure |
| `--port` / `-p` | `9686` | Port to serve `/metrics` on |
| `-4` / `-6` | both | Probe IPv4 (A) or IPv6 (AAAA) endpoints only (ping-style; mutually exclusive) |

## Handy PromQL

```promql
# Availability % over the dashboard range, per endpoint
100 * avg_over_time(probe_success{target="https://get.dune.build/install"}[$__range])

# Actual last request duration / throughput per endpoint
probe_last_duration_seconds
probe_last_download_speed_bytes_per_second

# Failure rate broken down by cause (curl exit code)
sum by (reason) (rate(probe_failures_total[5m]))

# Per-vantage view: is it down from everywhere (real outage) or just one path?
probe_success                       # group by vantage, ip in Grafana
```

## Edge / multi-vantage deployment

The prober is a single, **outbound-only** binary: it makes outbound probes
(curl) and an outbound `remote_write` POST to the central Prometheus. It needs
no inbound connectivity, so it runs happily behind home NAT / CGNAT — e.g. a
Raspberry Pi — giving a second vantage point. "Down from everywhere" is a real
outage; "down from one vantage" is a network-path problem.

**Choose a dual-stack vantage — or pin the family.** A vantage with no IPv6
(e.g. a home Pi on an IPv4-only ISP) will report *every* AAAA endpoint as down —
that's the vantage's missing IPv6, not the target's health, and it produces
false-down noise. Either probe from a host with working IPv6 (a datacentre box),
or pass **`-4`** so that vantage only probes IPv4 endpoints — still a useful
signal, without the spurious IPv6 failures. (`-6` is the converse.)

The central runs the full stack (`docker-compose.yml`); each edge runs only the
prober via `docker-compose.edge.yml`, labelled with its own `vantage`:

```sh
# on the edge host (e.g. the Pi)
export MON_CENTRAL=https://mon.example.com/api/v1/write   # needs TLS in front
export MON_AUTH=mon:yourpassword
export MON_VANTAGE=home
docker compose -f docker-compose.edge.yml up -d --build
```

The central's `/api/v1/write` is protected by basic auth (`prometheus/web.yml`)
and should sit behind TLS before facing the internet. For a self-signed /
internal-PKI endpoint, add `--remote-write-insecure` to the prober.

## Other architectures (ARM, RISC-V, …)

The Dockerfile is architecture-agnostic (multi-arch base images, arch-neutral
package names). Both `ocaml/opam:debian-13-ocaml-5.4` and `debian:13-slim` ship
**amd64, arm64, ppc64le, riscv64 and s390x**, so the prober builds unchanged on
all of them (verified on real arm64 and riscv64 hardware). Three options:

```sh
# 1. Native build on the Pi (simplest; slow first time)
docker compose -f docker-compose.edge.yml up -d --build

# 2. Cross-build on fast hardware + push a multi-arch image to a registry,
#    then the Pi just pulls it (set MON_IMAGE in the edge compose)
docker buildx build --platform linux/amd64,linux/arm64 \
  -t registry.example.com/mon:latest --push .

# 3. Build on a native arm64 builder node (no QEMU emulation)
docker --context=<arm64-builder> buildx build --platform linux/arm64 \
  -t registry.example.com/mon:latest --push .
```

Runtime deps on the Pi image: `curl`, `ca-certificates`, `libsnappy1v5`,
`libgmp10` (all installed by the Dockerfile's runtime stage).

## Building

This depends on the backend-agnostic `prometheus` / `prometheus-eio` packages.
Under `day10` they resolve from the local opam repository; for plain opam they
are pinned via `pin-depends` in `mon.opam` to the
`proto/backend-agnostic` branch of the fork.

This depends on the backend-agnostic `prometheus` / `prometheus-eio` packages.
Under `day10` they resolve from the local opam repository; for plain opam they
are pinned via `pin-depends` in `mon.opam` to the
`proto/backend-agnostic` branch of the fork.

# --- build stage -----------------------------------------------------------
FROM ocaml/opam:debian-13-ocaml-5.4 AS build

# System libraries needed by eio (liburing via the `uring` package) and the
# build. opam will also pull depexts automatically, but installing the common
# ones up front keeps the layer cache stable.
RUN sudo apt-get update \
 && sudo apt-get install -y --no-install-recommends libev-dev libsnappy-dev libgmp-dev pkg-config m4 \
 && sudo rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Resolve dependencies first for better layer caching. The prometheus-eio fork
# is fetched from the git pins declared in mon.opam (pin-depends).
COPY --chown=opam:opam mon.opam dune-project ./
RUN opam install . --deps-only -y

COPY --chown=opam:opam . .
RUN opam exec -- dune build ./bin/main.exe

# --- runtime stage ---------------------------------------------------------
FROM debian:13-slim

# The prober shells out to curl; ca-certificates is needed for HTTPS targets.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates libsnappy1v5 libgmp10 \
 && rm -rf /var/lib/apt/lists/*

COPY --from=build /src/_build/default/bin/main.exe /usr/local/bin/mon

EXPOSE 9686
ENTRYPOINT ["/usr/local/bin/mon"]
CMD ["--port", "9686"]

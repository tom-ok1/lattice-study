# lattice-study

`lattice-study` is an experimental Rust implementation of a distributed peer-to-peer service bus. It explores a mesh architecture inspired by publicly available information about Anduril's Distributed Service Bus and Lattice Mesh; it is an independent study and is not an official Anduril implementation.

The project keeps the control plane, forwarding logic, and publish/subscribe behavior in I/O-free state machines. Tokio-based adapters connect those components to real TCP sockets, which allows the same core behavior to be exercised in both runtime tests and the interactive Mesh Lab simulator.

The current implementation includes:

- link-state routing with signed LSAs, flooding, anti-entropy, expiry, and shortest-path calculation;
- protobuf wire frames and a loopback TCP transport;
- unicast and explicit-multicast forwarding with bounded priority queues, flow control, and conflation;
- live publish/subscribe with LSA-based subscriber discovery; and
- a browser-based lab for running, observing, and disrupting a multi-node mesh.

## Prerequisites

- Rust 1.75 or later
- Cargo

Run all commands below from the repository root.

## Running the server

`mblab-node` runs one mesh node with the production control, forwarding, and TCP runtime. It also exposes a lab-only Admin API on a loopback address.

```sh
cargo run -p mblab-node -- \
  --id 1 \
  --name node-1 \
  --mesh-port 7001 \
  --admin-port 9001 \
  --seq-path target/mblab-state/node-1.seq
```

The server prints its mesh and Admin API addresses when it is ready. Check the example node with:

```sh
curl http://127.0.0.1:9001/health
```

`--id` and `--seq-path` are required. The name defaults to `node-<id>`, while both port options default to `0`, which asks the operating system to choose available ports. Stop the server with `Ctrl+C`.

## Running the simulator

Mesh Lab starts multiple `mblab-node` processes, connects them through real loopback TCP sockets, and serves an interactive browser UI. Build the entire workspace first so that the node executable is available next to the simulator executable:

```sh
cargo build --workspace
cargo run -p mblab -- --initial 5 --port 8080
```

Open <http://127.0.0.1:8080> in a browser.

The UI can add, remove, crash, and restart nodes; connect and disconnect links; inject latency, packet loss, and bandwidth limits; send probe packets; and inspect routing, queue, LSDB, and runtime-event state.

By default, Mesh Lab adds a `220 ms` delay to each frame so that propagation and route convergence are easy to observe. Select **Realtime** in the UI or start it with `--tick-ms 0` to remove that delay:

```sh
cargo run -p mblab -- --initial 5 --port 8080 --tick-ms 0
```

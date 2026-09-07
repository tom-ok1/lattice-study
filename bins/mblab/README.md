# Mesh Lab

Mesh Lab launches multiple `mblab-node` processes on loopback TCP sockets and provides a browser UI for observing and manipulating the mesh.

```sh
cargo build --workspace
cargo run -p mblab -- --initial 5 --port 8080
```

Open <http://127.0.0.1:8080>.

The UI can add, remove, crash, and restart nodes; connect and disconnect links; inject latency, loss, and bandwidth limits; send probe packets; and inspect LSDB, route, queue, and runtime-event state.

Each node is a separate operating-system process running the production control, forwarding, and TCP transport crates. `mblab-node` is a lab-only Admin API wrapper and stores each node's LSA sequence across process restarts. Impairment is applied by lab-owned bidirectional frame proxies outside the production transport, which keeps the lab portable without requiring root privileges or platform-specific traffic-control tools.

The lab defaults to an observable `220 ms/frame` pace so LSA propagation and convergence remain visible in the UI. This adds real delay to every mesh wire frame while retaining the real TCP sockets and node processes. Select **Realtime** in the UI or pass `--tick-ms 0` to disable it.

# OPC UA -> NATS -> InfluxDB -> Grafana demo

End-to-end pipeline: simulated industrial sensors (OPC UA) flow through a
NATS leafnode hierarchy (edge -> gateway -> hub) as a chain of JetStream
streams that source each other, into InfluxDB, visualized in Grafana, with
NATS and JetStream themselves observed through Prometheus.

Auth is intentionally off/minimal everywhere - this is a POC, not a
production topology.

## Architecture

```
[Node-RED: simulated OPC UA server]
   Temperature / Pressure / RPM (ns=1;s=...)
            |  opc.tcp://opcua-server:4840
            v
[opcua-connector (Go)]  --JetStream publish (acked)-->  sensors.temperature
                                                         sensors.pressure
                                                         sensors.rpm
            |
            v
[nats-edge]  --leafnode-->  [nats-gateway]  --leafnode-->  [nats-hub]
 domain "edge"               domain "gateway"               domain "hub"
 SENSORS_EDGE                SENSORS_GATEWAY                SENSORS_HUB
 (subjects: sensors.>)  -->  (sources SENSORS_EDGE    -->   (sources SENSORS_GATEWAY
                              via $JS.edge.API)              via $JS.gateway.API)
     ^                           ^                              ^       |
 [nats-box-edge]           [nats-box-gateway]            [nats-box-hub] |
 (creates the stream on startup, then stays up for `nats` CLI use)     v
                                                       [nats-connector (Go)]
                                                        durable pull consumer
                                                        "influxdb-writer"
                                                        acks after each write
                                                                |
                                                                v
                                                     [InfluxDB bucket "sensors"]
                                                                |
                                                                v
                                                     [Grafana: OPC UA Sensor Data]

Observability (parallel path, relayed tier by tier):
[nats-edge :8222]     -> [exporter-edge]    -> [fluentbit-edge]    --forward-->
[nats-gateway :8222]  -> [exporter-gateway] -> [fluentbit-gateway] --forward-->
[nats-hub :8222]      -> [exporter-hub]     -> [fluentbit-hub] --remote write-->
   [Prometheus] -> [Grafana: NATS Servers, NATS JetStream]
```

Edge dials out to gateway, and gateway dials out to hub, exactly as you asked
for: `edge -> leaf of gateway -> leaf of hub`.

## Running it

```bash
cd nats-opcua-demo
docker compose up -d --build
```

First build takes a couple of minutes (Node-RED installs
`node-red-contrib-opcua`, the two Go services compile). Give everything
~20-30s after containers report "Up" for Node-RED to finish registering
nodes and for the leaf node mesh to settle.

Check it's actually flowing:

```bash
docker compose logs -f opcua-connector   # OPC UA -> SENSORS_EDGE publishes (with stream seq)
docker compose logs -f nats-connector    # SENSORS_HUB consumer -> InfluxDB writes

# Each tier has a nats-box pointed at its own server (NATS_URL is preset):
docker exec nats-box-edge    nats stream info SENSORS_EDGE
docker exec nats-box-gateway nats stream info SENSORS_GATEWAY   # see "Source Information" / Lag
docker exec nats-box-hub     nats stream info SENSORS_HUB
docker exec nats-box-hub     nats consumer info SENSORS_HUB influxdb-writer
```

All three streams should hold the same message count, with source lag 0.

## Where to look

| Service        | URL                          | Credentials              |
|----------------|-------------------------------|---------------------------|
| Node-RED editor| http://localhost:1880         | none                       |
| Grafana        | http://localhost:3000         | admin / admin (anon viewer also enabled) |
| InfluxDB UI    | http://localhost:8086         | admin / admin12345        |
| Prometheus     | http://localhost:9090         | none                       |

Three Grafana dashboards are provisioned automatically:

- **OPC UA Sensor Data** - Temperature / Pressure / RPM from InfluxDB (Flux).
- **NATS Servers** - throughput, connections, subscriptions, CPU/memory, slow
  consumers per server. Adapted from the upstream
  [grafana-nats-dash.json](https://github.com/nats-io/prometheus-nats-exporter/blob/main/walkthrough/grafana-nats-dash.json).
- **NATS JetStream** - per-stream messages/bytes, consumer pending/ack
  pending, storage usage, filterable by server/stream/consumer. Adapted from
  the upstream
  [grafana-jetstream-dash.json](https://github.com/nats-io/prometheus-nats-exporter/blob/main/walkthrough/grafana-jetstream-dash.json).

The two upstream dashboards are used as-is apart from pointing their
`${DS__NATS-PROMETHEUS}` import placeholder at the provisioned Prometheus
datasource (`prometheus-ds`) and giving them stable uids.

## Observability

Three `prometheus-nats-exporter` sidecars (one per NATS tier) scrape each
server's `:8222` monitoring endpoint (`-varz -connz -subz -leafz -jsz=all`)
and expose Prometheus metrics on `:7777` internally.

Prometheus does not scrape any of them. Each tier runs its own Fluent Bit
(`fluent-bit/{edge,gateway,hub}.yaml`), which scrapes the local exporter
every 10s and tags the series with `job` (`nats-edge`, `nats-gateway`,
`nats-hub`). The metrics then travel the same way the sensor data does,
one hop at a time:

- `fluentbit-edge` forwards its metrics to `fluentbit-gateway:24224`
  (Fluent Bit forward protocol).
- `fluentbit-gateway` forwards the edge's metrics plus its own to
  `fluentbit-hub:24224`.
- `fluentbit-hub` adds its own metrics and remote-writes everything into
  Prometheus, which runs with `--web.enable-remote-write-receiver` and has
  no scrape targets.

Forward outputs retry indefinitely (in memory), so a short outage of an
upstream tier delays metrics rather than dropping them. Buffered chunks are
retried independently and can arrive out of order, so Prometheus has a 1h
`out_of_order_time_window`. The exporters run
with `-use_internal_server_name`, so the `server_id` label is the readable
server name (`nats-edge`, ...) instead of the monitoring URL - the JetStream
dashboard's server filter is built from that label. Useful metrics:

- `gnatsd_varz_in_msgs` / `gnatsd_varz_out_msgs` - throughput per tier
- `gnatsd_leafz_conn_nodes_total` - leaf node count (gateway shows 2)
- `jetstream_stream_total_messages{stream_name=...}` - should match across
  `SENSORS_EDGE`, `SENSORS_GATEWAY`, `SENSORS_HUB`
- `jetstream_consumer_num_pending{consumer_name="influxdb-writer"}` - how
  far behind the InfluxDB writer is

## Stopping / cleaning up

```bash
docker compose down        # stop everything, keep volumes (JetStream/Influx data)
docker compose down -v     # also wipe JetStream store + InfluxDB data
```

# Kafka record key lost through EF bus outbox

Minimal synthetic reproduction with MassTransit **9.2.3**: a direct Kafka
producer preserves a non-null string key, while the scoped EF bus outbox
producer delivers the same key as **null**.

## Run

Requires the .NET 10 SDK, Bash, a running Docker engine, and your own valid
MassTransit license exported as `MT_LICENSE`. No license is included. Do not put
licenses or other secrets in source files, commits, or captured output.

```bash
./run.sh
echo $?
```

The script restores the committed dependency lock file, creates fresh PostgreSQL
and Kafka containers on random loopback-only ports, waits for readiness, runs the
app, and removes only its own containers and volumes on exit. Startup checks are
bounded (60 attempts; each Kafka check has a five-second timeout). The app has a
90-second cancellation deadline and a separate bounded shutdown. Initial image
pulls and dependency restore depend on network availability.

The disposable PostgreSQL instance uses trust authentication and Kafka uses
plaintext. These settings are for local reproduction only, not deployment.
Port conflicts fail the run; rerun to select new ports.

## Expected versus actual

Both calls use the same topic, message type, JSON value serializer, configured
producer, and exact string key `sample-key` (UTF-8 hex `73616D706C652D6B6579`).
Idempotence is enabled. The topic has one partition.

1. **Direct control:** public `IKafkaRider.GetProducer<string, ProbeRecord>`,
   bypassing scoped outbox interception.
2. **EF bus outbox:** scoped `ITopicProducer<string, ProbeRecord>`, explicitly
   awaited `Produce`, then `SaveChangesAsync`, then scope disposal.

**Expected:** both consumed Kafka keys equal the exact UTF-8 bytes.

**Observed on fresh containers:**

```text
direct: key bytes=73616D706C652D6B6579; partition=0; offset=0
outbox: key bytes=<null>; partition=0; offset=1
REPRODUCED: the direct key is correct, but the EF bus outbox Kafka key is null.
```

Exit codes: **1** = this specific null-key failure reproduced; **0** = both keys
correct (not reproduced); **2** = inconclusive/setup failure, timeout, invalid
control, unexpected message, or a different key mismatch. Script-level Docker or
restore failures can also return other nonzero codes; only the `REPRODUCED`
message identifies a confirmed reproduction.

Observation uses a separate Confluent Kafka consumer with **byte-array**
deserializers, explicitly assigned to partition zero from the beginning.
Synthetic run IDs and case labels correlate the two messages. The app rejects
incorrect control keys and unexpected/duplicate cases; it does not interpret
missing messages or infrastructure errors as a reproduction.

The database context contains only the standard transactional outbox entities.
The in-memory base bus only hosts the Kafka rider lifecycle: there is no RabbitMQ,
consumer, bridge, application entity, or entity lifecycle hook. The program uses
public APIs only, and does not inspect or modify vendor internals. This
demonstrates the end-to-end symptom, not its implementation-level cause.

## Versions

| Component | Version |
| --- | --- |
| .NET SDK used | 10.0.201 |
| MassTransit Kafka / EF Core | 9.2.3 |
| EF Core / Relational / Extensions.Hosting | 10.0.5 |
| Npgsql EF Core provider | 10.0.1 |
| Confluent.Kafka (transitive, locked) | 2.15.1 |
| PostgreSQL Docker image | postgres:17.6 |
| Kafka Docker image | apache/kafka:4.0.1 |

The committed `packages.lock.json` pins the complete NuGet dependency graph.
For existing disposable infrastructure, export `KAFKA_BOOTSTRAP_SERVERS` and
`POSTGRES_CONNECTION`, then run `dotnet restore --locked-mode` and
`dotnet run --no-restore`. Use an empty database: the app calls `EnsureCreatedAsync`.
This manual mode leaves database objects and a uniquely named topic behind;
`run.sh` is the self-cleaning path.

Public documentation:
[Kafka rider and transactional outbox support (v9.1+)](https://masstransit.massient.com/configuration/transports/kafka),
[EF transactional bus outbox configuration](https://masstransit.massient.com/configuration/middleware/outbox).

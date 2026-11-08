# krabka-schema-registry

[![Crates.io](https://img.shields.io/crates/v/krabka-schema-registry.svg)](https://crates.io/crates/krabka-schema-registry)
[![Docs.rs](https://docs.rs/krabka-schema-registry/badge.svg)](https://docs.rs/krabka-schema-registry)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Confluent Schema Registry-compatible REST service for Krabka (binary: krabka-schema-registry).

This crate is part of [Krabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```sh
cargo add krabka-schema-registry
```

For workspace development, use the path dependency from this repository instead.

## Usage example

Run the Schema Registry-compatible REST service and register an Avro schema:

```bash
KRABKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 \
KRABKA_SCHEMA_REGISTRY_LISTEN_ADDR=127.0.0.1:8081 \
krabka-schema-registry

curl -X POST http://127.0.0.1:8081/subjects/orders-value/versions \
  -H 'content-type: application/vnd.schemaregistry.v1+json' \
  -d '{"schema":"{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}"}'
```

## Logs and traces

The registry writes JSON log entries to stdout. `KRABKA_OTLP_ENABLED=true`, or
an endpoint in `KRABKA_OTLP_ENDPOINT` or `OTEL_EXPORTER_OTLP_ENDPOINT`, also
sends traces to an OTLP collector.

`--gcp-project-id` (`KRABKA_GCP_PROJECT_ID`) names the Google Cloud project
whose Cloud Trace holds the registry's traces. With it set and OTLP export on,
each log entry that the registry writes inside a traced span carries
`logging.googleapis.com/trace`, `logging.googleapis.com/spanId` and
`logging.googleapis.com/trace_sampled`, so Cloud Logging links the entry to its
trace. Unset, entries carry no trace keys. The broker and the gateway read the
same variable, so one value configures all three.

## Documentation

Read the API documentation at [docs.rs/krabka-schema-registry](https://docs.rs/krabka-schema-registry). The repository README contains the project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.

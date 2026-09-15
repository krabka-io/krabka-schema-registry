//! Capture Confluent JVM serializer frames from cp-schema-registry 7.4.0.
//!
//! Run only through `tools/capture-serde-fixtures.sh`; it serializes this
//! fixed-port Docker lane and fails when the named ignored test is not found.

use std::{
    net::SocketAddr,
    path::Path,
    process::Command,
    time::{Duration, Instant},
};

use krabka_broker::{Broker, BrokerConfig, NodeId};
use krabka_units::prelude::*;

const IMAGE: &str = "mirror.gcr.io/confluentinc/cp-schema-registry:7.4.0";
const CONTAINER: &str = "krabka-serde-fixture-capture";

struct Cleanup;

impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", CONTAINER])
            .output();
    }
}

fn run(command: &mut Command) -> String {
    let output = command.output().expect("run capture command");
    assert2::assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("capture output is UTF-8")
}

async fn wait_ready(base: &str) {
    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_mins(2);
    while Instant::now() < deadline {
        if client
            .get(format!("{base}/subjects"))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let logs = Command::new("docker")
        .args(["logs", CONTAINER])
        .output()
        .expect("read cp-schema-registry logs");
    panic!(
        "cp-schema-registry did not become ready: {}{}",
        String::from_utf8_lossy(&logs.stdout),
        String::from_utf8_lossy(&logs.stderr)
    );
}

const JAVA: &str = r#"
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import java.util.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

public class CaptureSerdes {
  public static class JsonOrder {
    public String id;
    public JsonOrder() {}
    JsonOrder(String id) { this.id = id; }
  }

  static String hex(byte[] bytes) {
    StringBuilder out = new StringBuilder();
    for (byte value : bytes) out.append(String.format("%02x", value));
    return out.toString();
  }

  static DescriptorProtos.FieldDescriptorProto field(
      String name, int number, DescriptorProtos.FieldDescriptorProto.Type type) {
    return DescriptorProtos.FieldDescriptorProto.newBuilder()
        .setName(name).setNumber(number).setType(type).build();
  }

  public static void main(String[] args) throws Exception {
    Map<String, Object> config = new HashMap<>();
    config.put("schema.registry.url", args[0]);

    KafkaAvroSerializer avro = new KafkaAvroSerializer();
    avro.configure(config, false);
    Schema avroSchema = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}");
    GenericData.Record avroOrder = new GenericData.Record(avroSchema);
    avroOrder.put("id", "o-1");
    System.out.println("avro=" + hex(avro.serialize("avro-orders", avroOrder)));

    KafkaJsonSchemaSerializer<JsonOrder> json = new KafkaJsonSchemaSerializer<>();
    json.configure(config, false);
    System.out.println("json=" + hex(json.serialize("json-orders", new JsonOrder("o-1"))));

    DescriptorProtos.EnumDescriptorProto kind = DescriptorProtos.EnumDescriptorProto.newBuilder()
        .setName("Kind")
        .addValue(DescriptorProtos.EnumValueDescriptorProto.newBuilder().setName("UNKNOWN").setNumber(0))
        .addValue(DescriptorProtos.EnumValueDescriptorProto.newBuilder().setName("READY").setNumber(1))
        .build();
    DescriptorProtos.DescriptorProto inner = DescriptorProtos.DescriptorProto.newBuilder()
        .setName("Inner").addField(field("id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)).build();
    DescriptorProtos.DescriptorProto outer = DescriptorProtos.DescriptorProto.newBuilder()
        .setName("Outer").addNestedType(inner).build();
    DescriptorProtos.DescriptorProto flat = DescriptorProtos.DescriptorProto.newBuilder()
        .setName("Flat").addField(field("id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)).build();
    DescriptorProtos.DescriptorProto event = DescriptorProtos.DescriptorProto.newBuilder()
        .setName("Event").addEnumType(kind)
        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder().setName("tags").setNumber(1)
            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED))
        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder().setName("kind").setNumber(2)
            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM).setTypeName(".fixture.Event.Kind"))
        .build();
    Descriptors.FileDescriptor file = Descriptors.FileDescriptor.buildFrom(
        DescriptorProtos.FileDescriptorProto.newBuilder().setName("fixture.proto")
            .setPackage("fixture").setSyntax("proto3")
            .addMessageType(flat).addMessageType(outer).addMessageType(event).build(),
        new Descriptors.FileDescriptor[] {});
    KafkaProtobufSerializer<DynamicMessage> protobuf = new KafkaProtobufSerializer<>();
    protobuf.configure(config, false);
    Descriptors.Descriptor flatDescriptor = file.findMessageTypeByName("Flat");
    DynamicMessage flatValue = DynamicMessage.newBuilder(flatDescriptor)
        .setField(flatDescriptor.findFieldByName("id"), "o-1").build();
    System.out.println("protobuf_flat=" + hex(protobuf.serialize("protobuf-flat", flatValue)));
    Descriptors.Descriptor innerDescriptor = file.findMessageTypeByName("Outer").findNestedTypeByName("Inner");
    DynamicMessage innerValue = DynamicMessage.newBuilder(innerDescriptor)
        .setField(innerDescriptor.findFieldByName("id"), "o-1").build();
    System.out.println("protobuf_nested=" + hex(protobuf.serialize("protobuf-nested", innerValue)));
    Descriptors.Descriptor eventDescriptor = file.findMessageTypeByName("Event");
    DynamicMessage eventValue = DynamicMessage.newBuilder(eventDescriptor)
        .addRepeatedField(eventDescriptor.findFieldByName("tags"), "one")
        .addRepeatedField(eventDescriptor.findFieldByName("tags"), "two")
        .setField(eventDescriptor.findFieldByName("kind"),
            eventDescriptor.findEnumTypeByName("Kind").findValueByName("READY"))
        .build();
    System.out.println("protobuf_repeated_enum=" + hex(protobuf.serialize("protobuf-event", eventValue)));
  }
}
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; regenerates Confluent JVM serializer fixtures"]
async fn capture_jvm_serde_fixtures() {
    let dir = tempfile::tempdir().unwrap();
    let listen: SocketAddr = "0.0.0.0:9092".parse().unwrap();
    let controller: SocketAddr = "0.0.0.0:9093".parse().unwrap();
    let broker = Broker::start(BrokerConfig {
        broker_id: 1,
        listen_addr: listen,
        advertised_listener: "host.docker.internal:9092".into(),
        log_dir: dir.path().to_path_buf(),
        node_id: NodeId(1),
        controller_listen_addr: controller,
        controller_quorum_voters: vec![(NodeId(1), controller.to_string())],
        heartbeat_interval: secs(3),
        heartbeat_timeout: secs(9),
        replica_lag_time_max: secs(30),
        controller_election_timeout: secs(5),
        controller_heartbeat_interval: millis(500),
        bootstrap_mode: krabka_broker::BootstrapMode::Bootstrap,
        ..BrokerConfig::default()
    })
    .await
    .unwrap();
    let _cleanup = Cleanup;
    let _ = Command::new("docker")
        .args(["rm", "-f", CONTAINER])
        .output();
    run(Command::new("docker").args([
        "run",
        "-d",
        "--add-host=host.docker.internal:host-gateway",
        "-p",
        "0:8081",
        "--name",
        CONTAINER,
        "-e",
        "SCHEMA_REGISTRY_HOST_NAME=localhost",
        "-e",
        "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://host.docker.internal:9092",
        "-e",
        "SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081",
        IMAGE,
    ]));
    let port = run(Command::new("docker").args(["port", CONTAINER, "8081/tcp"]));
    let port = port
        .trim()
        .rsplit_once(':')
        .expect("docker port contains host port")
        .1;
    let registry_url = format!("http://127.0.0.1:{port}");
    wait_ready(&registry_url).await;

    let source = dir.path().join("CaptureSerdes.java");
    std::fs::write(&source, JAVA).unwrap();
    run(Command::new("docker").args([
        "cp",
        source.to_str().unwrap(),
        &format!("{CONTAINER}:/tmp/CaptureSerdes.java"),
    ]));
    let classpath = "/usr/share/java/kafka-serde-tools/*:/usr/share/java/schema-registry/*:/usr/share/java/confluent-common/*:/usr/share/java/rest-utils/*";
    run(Command::new("docker").args([
        "exec",
        CONTAINER,
        "javac",
        "-cp",
        classpath,
        "/tmp/CaptureSerdes.java",
    ]));
    let output = run(Command::new("docker").args([
        "exec",
        CONTAINER,
        "java",
        "-cp",
        &format!("/tmp:{classpath}"),
        "CaptureSerdes",
        "http://127.0.0.1:8081",
    ]));
    let frames = output
        .lines()
        .filter(|line| {
            [
                "avro=",
                "json=",
                "protobuf_flat=",
                "protobuf_nested=",
                "protobuf_repeated_enum=",
            ]
            .iter()
            .any(|prefix| line.starts_with(prefix))
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    assert2::assert!(frames.lines().count() == 5, "captured frames:\n{frames}");
    let fixture_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/jvm-serde");
    std::fs::create_dir_all(&fixture_dir).unwrap();
    std::fs::write(fixture_dir.join("frames.txt"), frames).unwrap();
    broker.shutdown().await;
}

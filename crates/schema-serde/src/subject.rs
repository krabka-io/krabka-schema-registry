//! Subject naming.
//!
//! Confluent's default `TopicNameStrategy` maps a topic and a key/value role to
//! `<topic>-key` or `<topic>-value`.

/// Whether a serde handles the record key or value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Key,
    Value,
}

/// The schema format, used to set the registry `schemaType` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaKind {
    Avro,
    Protobuf,
    Json,
}

impl SchemaKind {
    /// Registry `schemaType` wire value.
    ///
    /// `None` omits the field from the request body, and the registry then
    /// applies the AVRO default.
    #[must_use]
    pub fn wire_name(self) -> Option<&'static str> {
        match self {
            Self::Avro => None,
            Self::Protobuf => Some("PROTOBUF"),
            Self::Json => Some("JSON"),
        }
    }

    /// The kind a registry `schemaType` field names.
    ///
    /// An absent field is AVRO, which is the registry's own default, and so is
    /// an unrecognised value: the registry only ever writes the three it knows,
    /// so anything else is a registry that has grown a format this build does
    /// not have, and Avro is the safe reading of it.
    #[must_use]
    pub fn from_wire_name(wire_name: Option<&str>) -> Self {
        match wire_name {
            Some("PROTOBUF") => Self::Protobuf,
            Some("JSON") => Self::Json,
            _ => Self::Avro,
        }
    }
}

/// Maps `(topic, role)` to a registry subject.
///
/// This trait is the seam for Record and `TopicRecord` strategies, which can come
/// later. Only `TopicNameStrategy` ships now.
pub trait SubjectStrategy: Send + Sync + 'static {
    fn subject(&self, topic: &str, role: Role) -> String;
}

/// Confluent default: `<topic>-key` / `<topic>-value`.
#[derive(Debug, Clone, Copy, Default)]
pub struct TopicNameStrategy;

impl SubjectStrategy for TopicNameStrategy {
    fn subject(&self, topic: &str, role: Role) -> String {
        match role {
            Role::Key => format!("{topic}-key"),
            Role::Value => format!("{topic}-value"),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn topic_name_strategy() {
        let s = TopicNameStrategy;
        for (name, role, expected) in [
            ("value", Role::Value, "orders-value"),
            ("key", Role::Key, "orders-key"),
        ] {
            check!(s.subject("orders", role) == expected, "case {name}");
        }
    }

    #[test]
    fn schema_kind_wire_names_round_trip() {
        for kind in [SchemaKind::Avro, SchemaKind::Protobuf, SchemaKind::Json] {
            check!(
                SchemaKind::from_wire_name(kind.wire_name()) == kind,
                "{kind:?}"
            );
        }
    }

    #[test]
    fn unknown_schema_type_reads_as_the_registry_default() {
        for wire_name in [None, Some("AVRO"), Some("SOMETHING_NEW")] {
            check!(
                SchemaKind::from_wire_name(wire_name) == SchemaKind::Avro,
                "{wire_name:?}"
            );
        }
    }

    #[test]
    fn schema_kind_wire_names() {
        for (name, kind, expected) in [
            ("avro", SchemaKind::Avro, None),
            ("protobuf", SchemaKind::Protobuf, Some("PROTOBUF")),
            ("json", SchemaKind::Json, Some("JSON")),
        ] {
            check!(kind.wire_name() == expected, "case {name}");
        }
    }
}

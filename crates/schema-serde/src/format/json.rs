//! JSON Schema serde.
//!
//! The local type gives its schema with `schemars`. Payloads are UTF-8 JSON.
//! The serde can also validate them against the writer schema.

use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use bytes::Bytes;
use schemars::JsonSchema;
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    cache::SchemaCache,
    error::SchemaSerdeError,
    format::{Binding, SchemaDeserializer, SchemaSerializer, SchemaSubject},
    registry::model::SchemaReference,
    subject::{Role, SchemaKind},
    wire,
};

/// JSON serializer and deserializer for `T: JsonSchema`, bound to a key/value
/// role.
///
/// The serde takes the subject from the topic at call time.
pub struct JsonSerde<T> {
    binding: Binding,
    validate: bool,
    _marker: PhantomData<fn() -> T>,
}

// Manual `Clone` (not derived) to avoid a spurious `T: Clone` bound;
// `add_source`/`add_sink` require `Serde<_> + Clone`.
impl<T> Clone for JsonSerde<T> {
    fn clone(&self) -> Self {
        Self {
            binding: self.binding.clone(),
            validate: self.validate,
            _marker: PhantomData,
        }
    }
}

impl<T: JsonSchema> JsonSerde<T> {
    fn make(cache: &Arc<SchemaCache>, role: Role, validate: bool) -> Self {
        // schemars 1.x: schema_for! returns schemars::Schema (newtype over serde_json::Value).
        let schema = schemars::schema_for!(T);
        let schema_text = serde_json::to_string(&schema).expect("schemars schema serializes");
        Self {
            binding: Binding {
                cache: Arc::clone(cache),
                role,
                kind: SchemaKind::Json,
                schema: schema_text,
                references: Vec::new(),
                message_type: None,
            },
            validate,
            _marker: PhantomData,
        }
    }

    /// A JSON serde for record **values**: `<topic>-value`.
    ///
    /// `validate` turns on draft validation of decoded payloads against the
    /// writer schema.
    pub fn value(cache: &Arc<SchemaCache>, validate: bool) -> Self {
        Self::make(cache, Role::Value, validate)
    }

    /// A JSON serde for record **keys**: `<topic>-key`.
    pub fn key(cache: &Arc<SchemaCache>, validate: bool) -> Self {
        Self::make(cache, Role::Key, validate)
    }

    /// Attach the references sent with register and lookup requests.
    #[must_use]
    pub fn with_references(mut self, references: Vec<SchemaReference>) -> Self {
        self.binding.references = references;
        self
    }
}

/// A value serde over the process [`default_registry`](crate::default_registry).
///
/// This serde turns payload validation off. To turn validation on, use
/// [`JsonSerde::value`].
impl<T: JsonSchema> Default for JsonSerde<T> {
    fn default() -> Self {
        let cache = crate::default_registry()
            .expect("schema-serde: call set_default_registry(cache) before a default JsonSerde");
        Self::value(&cache, false)
    }
}

impl<T: Send + Sync + 'static> SchemaSubject for JsonSerde<T> {
    fn register_subject(&self, topic: &str) {
        self.binding.register(topic);
    }
}

impl<T> SchemaSerializer<T> for JsonSerde<T>
where
    T: Serialize + JsonSchema + Send + Sync + 'static,
{
    fn serialize(&self, topic: &str, value: &T) -> Result<Bytes, SchemaSerdeError> {
        let id = self.binding.id(topic)?;
        let body =
            serde_json::to_vec(value).map_err(|e| SchemaSerdeError::Serialize(e.to_string()))?;
        Ok(wire::encode(id, &body))
    }
}

impl<T> SchemaDeserializer<T> for JsonSerde<T>
where
    T: DeserializeOwned + JsonSchema + Send + Sync + 'static,
{
    fn deserialize(&self, _topic: &str, bytes: &[u8]) -> Result<T, SchemaSerdeError> {
        let (id, body) = wire::decode(bytes)?;
        if self.validate {
            let writer_schema = self.binding.cache.writer_schema_with_references(id)?;
            let writer: serde_json::Value = serde_json::from_str(&writer_schema.schema)
                .map_err(|e| SchemaSerdeError::Schema(e.to_string()))?;
            let instance: serde_json::Value = serde_json::from_slice(body)
                .map_err(|e| SchemaSerdeError::Deserialize(e.to_string()))?;
            // jsonschema: validator_for(&Value) -> Result<Validator, ValidationError<'static>>
            let references = writer_schema
                .references
                .into_iter()
                .map(|(name, source)| {
                    serde_json::from_str(&source)
                        .map(|schema| (name, schema))
                        .map_err(|error| SchemaSerdeError::Schema(error.to_string()))
                })
                .collect::<Result<HashMap<_, _>, _>>()?;
            let validator = jsonschema::options()
                .with_retriever(CachedRetriever(references))
                .build(&writer)
                .map_err(|e| SchemaSerdeError::Schema(e.to_string()))?;
            // Validator::validate(&self, instance) -> Result<(), ValidationError<'i>>
            validator.validate(&instance).map_err(|e| {
                SchemaSerdeError::Deserialize(format!("json schema validation: {e}"))
            })?;
        }
        serde_json::from_slice(body).map_err(|e| SchemaSerdeError::Deserialize(e.to_string()))
    }
}

#[derive(Debug)]
struct CachedRetriever(HashMap<String, serde_json::Value>);

impl jsonschema::Retrieve for CachedRetriever {
    fn retrieve(
        &self,
        uri: &jsonschema::Uri<String>,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        self.0.get(uri.as_str()).cloned().ok_or_else(|| {
            format!("JSON Schema reference {uri} is not present in the registry cache").into()
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use assert2::check;
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::{
        cache::{CacheConfig, SchemaCache},
        registry::RegistryClient,
    };

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
    struct Order {
        id: String,
        total: f64,
    }

    #[test]
    fn round_trips_with_validation() {
        let cache = SchemaCache::new(RegistryClient::new("http://unused"), CacheConfig::default());
        let serde = JsonSerde::<Order>::value(&cache, true);
        serde.register_subject("orders");
        let schema_text = serde_json::to_string(&schemars::schema_for!(Order)).unwrap();
        cache.seed_subject_id("orders-value", 5);
        cache.seed_writer_schema(5, schema_text);

        let order = Order {
            id: "o-1".into(),
            total: 3.0,
        };
        let framed = serde.serialize("orders", &order).unwrap();
        check!(framed[0] == 0x00);
        let back: Order = serde.deserialize("orders", &framed).unwrap();
        check!(back == order);
    }

    #[test]
    fn validates_external_reference_from_writer_cache() {
        let cache = SchemaCache::new(RegistryClient::new("http://unused"), CacheConfig::default());
        let serde = JsonSerde::<Order>::value(&cache, true);
        cache.seed_writer_schema_with_references(
            6,
            r#"{"$ref":"https://schemas.example/order.json"}"#,
            HashMap::from([(
                "https://schemas.example/order.json".into(),
                r#"{"type":"object","required":["id","total"],"properties":{"id":{"type":"string"},"total":{"type":"number"}}}"#.into(),
            )]),
        );
        let frame = wire::encode(6, br#"{"id":"o-2","total":4.5}"#);
        let decoded = serde.deserialize("orders", &frame).unwrap();
        check!(
            decoded
                == Order {
                    id: "o-2".into(),
                    total: 4.5
                }
        );
    }
}

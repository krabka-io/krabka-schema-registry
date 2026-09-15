//! Async Confluent Schema Registry REST client.

pub mod model;

use std::collections::{HashMap, HashSet};

use model::{
    RegisterResponse, SchemaByIdResponse, SchemaPayload, SchemaReference, SubjectVersion,
    SubjectVersionResponse,
};
use reqwest::Client;
use serde::Deserialize;

use crate::{error::SchemaSerdeError, subject::SchemaKind};

const CONTENT_TYPE: &str = "application/vnd.schemaregistry.v1+json";
const MAX_REFERENCE_DEPTH: usize = 64;

/// Thin async client over the registry REST API.
///
/// The client is cloneable. Every clone shares the same underlying
/// `reqwest::Client` connection pool.
#[derive(Debug, Clone)]
pub struct RegistryClient {
    base_url: String,
    http: Client,
}

/// Schema text plus optional Krabka extension metadata fetched by global id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedSchema {
    pub schema: String,
    /// The format the registry recorded for this schema. A caller that has to
    /// decode a body against it needs this, and the id alone does not say it.
    pub kind: SchemaKind,
    pub message_type: Option<String>,
    pub references: Vec<SchemaReference>,
}

impl RegistryClient {
    /// Build a client for a registry at `base_url`, for example
    /// `http://localhost:8081`.
    #[must_use]
    pub fn new(base_url: impl Into<String>) -> Self {
        Self::with_http_client(base_url, Client::new())
    }

    /// Build a client for a registry at `base_url` over a caller-supplied
    /// `reqwest::Client`.
    ///
    /// A caller that needs a request timeout, a proxy, or its own TLS
    /// configuration builds the HTTP client itself and passes it here. A
    /// timeout matters most: without one a registry that accepts a connection
    /// and then stops answering holds the caller for as long as it likes.
    #[must_use]
    pub fn with_http_client(base_url: impl Into<String>, http: Client) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            http,
        }
    }

    /// Every subject that schema `id` is registered under, with its version
    /// in each.
    ///
    /// This is what answers "does this id belong to this topic?". A schema id
    /// resolves globally, so the id alone does not say which subject wrote it;
    /// a caller that binds a topic to a subject checks the subject against
    /// this list.
    ///
    /// Soft-deleted registrations are not included.
    ///
    /// # Errors
    /// Returns an error when the id is not registered, or when the registry is
    /// unreachable or answers with a body this client cannot decode.
    pub async fn subject_versions_for_id(
        &self,
        id: u32,
    ) -> Result<Vec<SubjectVersion>, SchemaSerdeError> {
        let url = format!("{}/schemas/ids/{id}/versions", self.base_url);
        self.get_json(&url).await
    }

    /// Register `schema` under `subject` and return its global id.
    ///
    /// This method gives the `auto.register.schemas=true` behavior.
    ///
    /// # Errors
    /// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
    pub async fn register(
        &self,
        subject: &str,
        kind: SchemaKind,
        schema: &str,
        references: &[SchemaReference],
        message_type: Option<&str>,
    ) -> Result<u32, SchemaSerdeError> {
        let url = format!("{}/subjects/{subject}/versions", self.base_url);
        let body = SchemaPayload {
            schema,
            schema_type: kind.wire_name(),
            message_type,
            references,
        };
        let resp: RegisterResponse = self.post_json(&url, &body).await?;
        Ok(resp.id)
    }

    /// Look up the id of an already-registered `schema` under `subject`.
    ///
    /// This method gives the `auto.register.schemas=false` behavior.
    ///
    /// # Errors
    /// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
    pub async fn lookup(
        &self,
        subject: &str,
        kind: SchemaKind,
        schema: &str,
        references: &[SchemaReference],
        message_type: Option<&str>,
    ) -> Result<u32, SchemaSerdeError> {
        let url = format!("{}/subjects/{subject}", self.base_url);
        let body = SchemaPayload {
            schema,
            schema_type: kind.wire_name(),
            message_type,
            references,
        };
        let resp: SubjectVersionResponse = self.post_json(&url, &body).await?;
        Ok(resp.id)
    }

    /// Fetch the latest registered version under `subject`.
    ///
    /// This method gives the `use.latest.version=true` behavior.
    ///
    /// # Errors
    /// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
    pub async fn latest(&self, subject: &str) -> Result<SubjectVersionResponse, SchemaSerdeError> {
        let url = format!("{}/subjects/{subject}/versions/latest", self.base_url);
        self.get_json(&url).await
    }

    /// Fetch the latest registered version's id under `subject`.
    ///
    /// This method gives the `use.latest.version=true` behavior.
    ///
    /// # Errors
    /// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
    pub async fn latest_id(&self, subject: &str) -> Result<u32, SchemaSerdeError> {
        Ok(self.latest(subject).await?.id)
    }

    /// Fetch a schema's text and optional metadata by global id.
    ///
    /// The deserialize path uses this method.
    ///
    /// # Errors
    /// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
    pub async fn schema_by_id(&self, id: u32) -> Result<FetchedSchema, SchemaSerdeError> {
        let url = format!("{}/schemas/ids/{id}", self.base_url);
        let resp: SchemaByIdResponse = self.get_json(&url).await?;
        Ok(FetchedSchema {
            schema: resp.schema,
            kind: SchemaKind::from_wire_name(resp.schema_type.as_deref()),
            message_type: resp.message_type,
            references: resp.references,
        })
    }

    /// Fetch a schema source by the subject and version named in a reference.
    ///
    /// # Errors
    ///
    /// Returns an error when the registry request fails. Returns an error when
    /// the client cannot decode the response.
    pub async fn schema_by_subject_version(
        &self,
        subject: &str,
        version: i32,
    ) -> Result<FetchedSchema, SchemaSerdeError> {
        let url = format!(
            "{}/subjects/{subject}/versions/{version}?deleted=true",
            self.base_url
        );
        let resp: SubjectVersionResponse = self.get_json(&url).await?;
        Ok(FetchedSchema {
            schema: resp.schema,
            kind: SchemaKind::from_wire_name(resp.schema_type.as_deref()),
            message_type: resp.message_type,
            references: resp.references,
        })
    }

    /// Resolve registry references to their exact, name-keyed source text.
    ///
    /// This method bounds the depth of the resolution and detects
    /// subject-version cycles. It never uses the filesystem or any import
    /// mechanism other than Schema Registry.
    ///
    /// # Errors
    ///
    /// Returns an error when a reference is cyclic. Returns an error when a
    /// reference exceeds the depth limit. Returns an error when the client
    /// cannot fetch or decode a reference from the registry.
    pub async fn reference_sources(
        &self,
        references: &[SchemaReference],
    ) -> Result<HashMap<String, String>, SchemaSerdeError> {
        self.reference_sources_ordered(references)
            .await
            .map(|(sources, _)| sources)
    }

    pub(crate) async fn reference_sources_ordered(
        &self,
        references: &[SchemaReference],
    ) -> Result<(HashMap<String, String>, Vec<String>), SchemaSerdeError> {
        let mut sources = HashMap::new();
        let mut order = Vec::new();
        let mut resolving = HashSet::new();
        self.resolve_reference_sources(references, &mut sources, &mut order, &mut resolving, 0)
            .await?;
        Ok((sources, order))
    }

    async fn resolve_reference_sources(
        &self,
        references: &[SchemaReference],
        sources: &mut HashMap<String, String>,
        order: &mut Vec<String>,
        resolving: &mut HashSet<(String, i32)>,
        depth: usize,
    ) -> Result<(), SchemaSerdeError> {
        if depth >= MAX_REFERENCE_DEPTH {
            return Err(SchemaSerdeError::Schema(format!(
                "schema reference depth exceeds {MAX_REFERENCE_DEPTH}"
            )));
        }

        for reference in references {
            if sources.contains_key(&reference.name) {
                continue;
            }
            let reference_key = (reference.subject.clone(), reference.version);
            if !resolving.insert(reference_key.clone()) {
                return Err(SchemaSerdeError::Schema(format!(
                    "cyclic schema reference at {} version {}",
                    reference.subject, reference.version
                )));
            }
            let referenced_schema = self
                .schema_by_subject_version(&reference.subject, reference.version)
                .await?;
            Box::pin(self.resolve_reference_sources(
                &referenced_schema.references,
                sources,
                order,
                resolving,
                depth + 1,
            ))
            .await?;
            resolving.remove(&reference_key);
            sources.insert(reference.name.clone(), referenced_schema.schema);
            order.push(reference.name.clone());
        }
        Ok(())
    }

    async fn post_json<B: serde::Serialize, R: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        body: &B,
    ) -> Result<R, SchemaSerdeError> {
        let resp = self
            .http
            .post(url)
            .header("Content-Type", CONTENT_TYPE)
            .json(body)
            .send()
            .await
            .map_err(|e| SchemaSerdeError::RegistryTransport(e.to_string()))?;
        Self::parse(resp).await
    }

    async fn get_json<R: serde::de::DeserializeOwned>(
        &self,
        url: &str,
    ) -> Result<R, SchemaSerdeError> {
        let resp = self
            .http
            .get(url)
            .header("Accept", CONTENT_TYPE)
            .send()
            .await
            .map_err(|e| SchemaSerdeError::RegistryTransport(e.to_string()))?;
        Self::parse(resp).await
    }

    async fn parse<R: serde::de::DeserializeOwned>(
        resp: reqwest::Response,
    ) -> Result<R, SchemaSerdeError> {
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| SchemaSerdeError::RegistryTransport(e.to_string()))?;
        if !status.is_success() {
            #[derive(Deserialize)]
            struct ErrorBody {
                error_code: i32,
                message: String,
            }

            let parsed = serde_json::from_str::<ErrorBody>(&text).ok();
            return Err(SchemaSerdeError::RegistryStatus {
                status: status.as_u16(),
                error_code: parsed
                    .as_ref()
                    .map_or_else(|| i32::from(status.as_u16()), |body| body.error_code),
                message: parsed.map_or(text, |body| body.message),
            });
        }
        serde_json::from_str(&text).map_err(|e| SchemaSerdeError::RegistryDecode(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{body_json, method, path, query_param},
    };

    use super::{model::SchemaPayload, *};

    #[test]
    fn base_url_trims_trailing_slash() {
        let c = RegistryClient::new("http://localhost:8081/");
        check!(c.base_url == "http://localhost:8081");
    }

    #[test]
    fn payload_omits_avro_type_and_empty_refs() {
        let p = SchemaPayload {
            schema: "\"string\"",
            schema_type: SchemaKind::Avro.wire_name(),
            message_type: None,
            references: &[],
        };
        let j = serde_json::to_string(&p).unwrap();
        check!(j == r#"{"schema":"\"string\""}"#);
    }

    #[test]
    fn payload_includes_protobuf_type() {
        let p = SchemaPayload {
            schema: "syntax = \"proto3\";",
            schema_type: SchemaKind::Protobuf.wire_name(),
            message_type: Some("demo.Order"),
            references: &[],
        };
        let j = serde_json::to_string(&p).unwrap();
        check!(
            (
                j.contains(r#""schemaType":"PROTOBUF""#),
                j.contains(r#""messageType":"demo.Order""#),
            ) == (true, true)
        );
    }

    #[tokio::test]
    async fn register_posts_schema_payload_and_returns_id() {
        let server = MockServer::start().await;
        let references = [SchemaReference {
            name: "money.proto".into(),
            subject: "money-value".into(),
            version: 3,
        }];
        Mock::given(method("POST"))
            .and(path("/subjects/orders-value/versions"))
            .and(body_json(serde_json::json!({
                "schema": "syntax = \"proto3\";",
                "schemaType": "PROTOBUF",
                "messageType": "demo.Order",
                "references": [{
                    "name": "money.proto",
                    "subject": "money-value",
                    "version": 3
                }]
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": 42
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = RegistryClient::new(server.uri());
        let id = client
            .register(
                "orders-value",
                SchemaKind::Protobuf,
                "syntax = \"proto3\";",
                &references,
                Some("demo.Order"),
            )
            .await
            .unwrap();

        check!(id == 42);
    }

    #[tokio::test]
    async fn lookup_posts_schema_payload_and_returns_existing_id() {
        let server = MockServer::start().await;
        let references = [SchemaReference {
            name: "common.json".into(),
            subject: "common-value".into(),
            version: 2,
        }];
        Mock::given(method("POST"))
            .and(path("/subjects/orders-value"))
            .and(body_json(serde_json::json!({
                "schema": r#"{"type":"object"}"#,
                "schemaType": "JSON",
                "references": [{
                    "name": "common.json",
                    "subject": "common-value",
                    "version": 2
                }]
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": 43,
                "version": 7,
                "schema": r#"{"type":"object"}"#,
                "schemaType": "JSON"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = RegistryClient::new(server.uri());
        let id = client
            .lookup(
                "orders-value",
                SchemaKind::Json,
                r#"{"type":"object"}"#,
                &references,
                None,
            )
            .await
            .unwrap();

        check!(id == 43);
    }

    #[tokio::test]
    async fn latest_id_fetches_latest_subject_version() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/subjects/orders-value/versions/latest"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": 44,
                "version": 9,
                "schema": "syntax = \"proto3\";",
                "schemaType": "PROTOBUF",
                "messageType": "demo.Order"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = RegistryClient::new(server.uri());
        check!(client.latest_id("orders-value").await.unwrap() == 44);
    }

    #[tokio::test]
    async fn subject_versions_for_id_returns_every_binding() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/42/versions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {"subject": "orders-value", "version": 3},
                {"subject": "orders-archive-value", "version": 1}
            ])))
            .expect(1)
            .mount(&server)
            .await;

        let client = RegistryClient::new(server.uri());
        let got = client.subject_versions_for_id(42).await.unwrap();

        check!(
            got == vec![
                SubjectVersion {
                    subject: "orders-value".to_owned(),
                    version: 3,
                },
                SubjectVersion {
                    subject: "orders-archive-value".to_owned(),
                    version: 1,
                },
            ]
        );
    }

    #[tokio::test]
    async fn subject_versions_for_id_reports_an_unregistered_id() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/99/versions"))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "error_code": 40403,
                "message": "Schema not found"
            })))
            .mount(&server)
            .await;

        let client = RegistryClient::new(server.uri());
        let error = client.subject_versions_for_id(99).await.unwrap_err();

        check!(
            matches!(
                error,
                SchemaSerdeError::RegistryStatus {
                    status: 404,
                    error_code: 40403,
                    ..
                }
            ),
            "{error}"
        );
    }

    #[tokio::test]
    async fn confluent_errors_are_typed_and_non_json_errors_remain_usable() {
        let cases = [
            (
                404,
                include_str!(
                    "../../../schema-registry/tests/fixtures/rest_err_subject_not_found.json"
                ),
                40401,
                true,
                false,
            ),
            (
                422,
                include_str!(
                    "../../../schema-registry/tests/fixtures/rest_err_invalid_schema.json"
                ),
                42201,
                false,
                false,
            ),
            (500, "forward failed", 500, false, true),
        ];

        for (status, fixture, error_code, subject_not_found, transient) in cases {
            let body = serde_json::from_str::<serde_json::Value>(fixture)
                .ok()
                .and_then(|fixture| fixture.get("_body")?.as_str().map(str::to_owned))
                .unwrap_or_else(|| fixture.to_owned());
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/schemas/ids/9/versions"))
                .respond_with(ResponseTemplate::new(status).set_body_string(body))
                .mount(&server)
                .await;
            let error = RegistryClient::new(server.uri())
                .subject_versions_for_id(9)
                .await
                .unwrap_err();
            assert2::assert!(let SchemaSerdeError::RegistryStatus {
                status: actual_status,
                error_code: actual_code,
                message,
            } = &error);
            check!(*actual_status == status);
            check!(*actual_code == error_code);
            check!(!message.is_empty());
            check!(error.is_subject_not_found() == subject_not_found);
            check!(error.is_transient_registry_failure() == transient);
        }
    }

    #[tokio::test]
    async fn references_are_fetched_deleted_and_returned_dependency_first() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/subjects/b-value/versions/2"))
            .and(query_param("deleted", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": 2,
                "version": 2,
                "schema": "B",
                "references": [{"name": "a.avsc", "subject": "a-value", "version": 1}]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/subjects/a-value/versions/1"))
            .and(query_param("deleted", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": 1,
                "version": 1,
                "schema": "A"
            })))
            .expect(1)
            .mount(&server)
            .await;
        let references = [SchemaReference {
            name: "b.avsc".into(),
            subject: "b-value".into(),
            version: 2,
        }];

        let (sources, order) = RegistryClient::new(server.uri())
            .reference_sources_ordered(&references)
            .await
            .unwrap();

        check!(order == vec!["a.avsc", "b.avsc"]);
        check!(sources["a.avsc"] == "A");
        check!(sources["b.avsc"] == "B");
    }

    #[test]
    fn registry_error_predicates_use_confluent_codes() {
        for (code, subject, schema, incompatible) in [
            (40401, true, false, false),
            (40403, false, true, false),
            (409, false, false, true),
        ] {
            let error = SchemaSerdeError::RegistryStatus {
                status: 400,
                error_code: code,
                message: "fixture".into(),
            };
            check!(error.is_subject_not_found() == subject);
            check!(error.is_schema_not_found() == schema);
            check!(error.is_incompatible() == incompatible);
        }
    }
}

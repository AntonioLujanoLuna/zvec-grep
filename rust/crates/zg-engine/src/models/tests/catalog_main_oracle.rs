use serde_json::{Value, json};

use crate::{
    domain::model::Metric,
    models::catalog::{
        ArtifactSpec, EmbeddingCatalogEntry, LlamaCppConfig, Model2VecConfig, ModelSource,
        QwenConfig, SourceKind, TransformersConfig, list_embedding_models,
    },
};

#[test]
fn catalog_matches_main_typescript_field_for_field() {
    let expected: Value = serde_json::from_str(include_str!("fixtures/catalog-main-oracle.json"))
        .expect("checked-in TypeScript catalog oracle must be valid JSON");
    let actual = Value::Array(
        list_embedding_models()
            .into_iter()
            .map(entry_value)
            .collect(),
    );
    assert_eq!(actual, expected);
}

#[allow(clippy::too_many_lines)]
fn entry_value(entry: EmbeddingCatalogEntry) -> Value {
    match entry {
        EmbeddingCatalogEntry::LlamaCpp(LlamaCppConfig {
            reference,
            provider,
            model,
            uri,
            dimension,
            metric,
            format,
            context_size,
            max_batch_size,
            artifacts,
            sources,
        }) => json!({
            "backend": "llama-cpp",
            "reference": reference,
            "provider": provider,
            "model": model,
            "uri": uri,
            "dimension": dimension,
            "metric": metric_name(metric),
            "format": format,
            "contextSize": context_size,
            "maxBatchSize": max_batch_size,
            "artifacts": artifacts_value(artifacts),
            "sources": sources_value(sources),
        }),
        EmbeddingCatalogEntry::Qwen(QwenConfig {
            kind,
            reference,
            provider,
            model,
            dimension,
            metric,
            default_endpoint,
            max_batch_size,
            max_input_tokens,
            max_image_bytes,
        }) => {
            let mut value = json!({
                "backend": "qwen",
                "kind": kind,
                "reference": reference,
                "provider": provider,
                "model": model,
                "dimension": dimension,
                "metric": metric_name(metric),
                "defaultEndpoint": default_endpoint,
                "maxBatchSize": max_batch_size,
                "maxInputTokens": max_input_tokens,
            });
            if let Some(maximum) = max_image_bytes {
                value
                    .as_object_mut()
                    .expect("catalog JSON must be an object")
                    .insert("maxImageBytes".to_owned(), json!(maximum));
            }
            value
        }
        EmbeddingCatalogEntry::TransformersJs(TransformersConfig {
            reference,
            provider,
            model,
            repo,
            revision,
            dtype,
            dimension,
            metric,
            pooling,
            normalize,
            query_prefix,
            document_prefix,
            max_input_tokens,
            max_batch_size,
            artifacts,
            sources,
        }) => {
            let mut value = json!({
                "backend": "transformers-js",
                "reference": reference,
                "provider": provider,
                "model": model,
                "repo": repo,
                "revision": revision,
                "dtype": dtype,
                "dimension": dimension,
                "metric": metric_name(metric),
                "pooling": pooling,
                "normalize": normalize,
                "maxInputTokens": max_input_tokens,
                "maxBatchSize": max_batch_size,
                "artifacts": artifacts_value(artifacts),
                "sources": sources_value(sources),
            });
            let object = value
                .as_object_mut()
                .expect("catalog JSON must be an object");
            if let Some(prefix) = query_prefix {
                object.insert("queryPrefix".to_owned(), json!(prefix));
            }
            if let Some(prefix) = document_prefix {
                object.insert("documentPrefix".to_owned(), json!(prefix));
            }
            value
        }
        EmbeddingCatalogEntry::Model2Vec(Model2VecConfig {
            reference,
            provider,
            model,
            repo,
            revision,
            model_file,
            embedding_tensor,
            tokenizer_file,
            dimension,
            metric,
            normalize,
            max_input_tokens,
            max_batch_size,
            default_concurrency,
            artifacts,
            sources,
            ..
        }) => json!({
            "backend": "model2vec",
            "reference": reference,
            "provider": provider,
            "model": model,
            "repo": repo,
            "revision": revision,
            "modelFile": model_file,
            "embeddingTensor": embedding_tensor,
            "tokenizerFile": tokenizer_file,
            "dimension": dimension,
            "metric": metric_name(metric),
            "normalize": normalize,
            "maxInputTokens": max_input_tokens,
            "maxBatchSize": max_batch_size,
            "defaultConcurrency": default_concurrency,
            "artifacts": artifacts_value(artifacts),
            "sources": sources_value(sources),
        }),
    }
}

#[test]
fn local_models_pin_revisions_and_record_artifacts() {
    for entry in list_embedding_models() {
        let (reference, artifacts, sources) = match entry {
            EmbeddingCatalogEntry::LlamaCpp(config) => {
                assert_pinned_uri(config.uri, config.reference);
                (config.reference, config.artifacts, config.sources)
            }
            EmbeddingCatalogEntry::TransformersJs(config) => {
                (config.reference, config.artifacts, config.sources)
            }
            EmbeddingCatalogEntry::Model2Vec(config) => {
                (config.reference, config.artifacts, config.sources)
            }
            EmbeddingCatalogEntry::Qwen(_) => continue,
        };
        assert!(
            !artifacts.is_empty(),
            "{reference} must record the artifacts it downloads"
        );
        for artifact in artifacts {
            assert!(
                artifact.size > 0,
                "{reference} artifact {} must record its size",
                artifact.path
            );
            assert_eq!(
                artifact.sha256.len(),
                64,
                "{reference} artifact {} must record a SHA-256",
                artifact.path
            );
            assert!(
                artifact.sha256.chars().all(|c| c.is_ascii_hexdigit()),
                "{reference} artifact {} must record a hexadecimal SHA-256",
                artifact.path
            );
        }
        assert!(
            !sources.is_empty(),
            "{reference} must record its download sources"
        );
        for source in sources {
            assert_eq!(
                source.revision.len(),
                40,
                "{reference} must pin a full source revision"
            );
        }
        assert!(
            sources
                .iter()
                .any(|source| source.kind == SourceKind::HuggingFace),
            "{reference} must record its Hugging Face source"
        );
    }
}

fn assert_pinned_uri(uri: &str, reference: &str) {
    let (_, revision) = uri
        .rsplit_once('#')
        .unwrap_or_else(|| panic!("{reference} URI must pin a revision: {uri}"));
    assert_eq!(
        revision.len(),
        40,
        "{reference} URI must pin a full revision: {uri}"
    );
    assert!(
        revision.chars().all(|c| c.is_ascii_hexdigit()),
        "{reference} URI must pin a hexadecimal revision: {uri}"
    );
}

fn artifacts_value(artifacts: &[ArtifactSpec]) -> Value {
    Value::Array(
        artifacts
            .iter()
            .map(|artifact| {
                json!({
                    "path": artifact.path,
                    "size": artifact.size,
                    "sha256": artifact.sha256,
                })
            })
            .collect(),
    )
}

fn sources_value(sources: &[ModelSource]) -> Value {
    Value::Array(
        sources
            .iter()
            .map(|source| {
                let kind = match source.kind {
                    SourceKind::HuggingFace => "hugging-face",
                    SourceKind::ModelScope => "model-scope",
                };
                json!({
                    "kind": kind,
                    "repo": source.repo,
                    "revision": source.revision,
                })
            })
            .collect(),
    )
}

const fn metric_name(metric: Metric) -> &'static str {
    match metric {
        Metric::Cosine => "cosine",
        Metric::DotProduct => "dot",
        Metric::Euclidean => "euclidean",
    }
}

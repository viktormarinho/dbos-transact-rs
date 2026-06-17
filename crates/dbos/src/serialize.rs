//! Payload serialization for the system database.
//!
//! Every DBOS payload column (`inputs`, `output`, `error`, message/event values, …) has a sibling
//! `serialization` column recording the format the bytes are in. Decoders dispatch on it. This
//! lets formats from different SDKs coexist and migrate row-by-row.
//!
//! Formats:
//! - **`portable_json`** — plain compact JSON. The cross-language lingua franca, and this crate's
//!   **default**, so Rust-written payloads are directly readable by the Python/TS/Go SDKs (in
//!   portable mode) with no migration.
//! - **`DBOS_JSON`** — base64(JSON). The Go SDK's native default. We can read and write it, mainly
//!   so a Rust app can ingest a Go-written database out of the box.
//! - **`js_superjson`** (read-only, planned) — the TypeScript SDK's native default. A reader is
//!   planned for the serialization milestone so a Rust app can read an existing TS-DBOS database.
//! - **`py_pickle`** — not supported (Python pickle is not portable); migrate such rows to
//!   `portable_json` on the Python side first.
//!
//! Workflow inputs in portable mode use the cross-language envelope
//! `{"positionalArgs":[arg], "namedArgs":{}}`; errors use `{"name","message","code?","data?"}`.

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;

use crate::error::{DbosError, Result};

/// Marker stored in `DBOS_JSON` columns for a nil/`null` value (Go `__DBOS_NIL`).
pub const NIL_MARKER: &str = "__DBOS_NIL";
pub const PORTABLE_NAME: &str = "portable_json";
pub const DBOS_JSON_NAME: &str = "DBOS_JSON";
pub const SUPERJSON_NAME: &str = "js_superjson";
pub const PY_PICKLE_NAME: &str = "py_pickle";

/// A serialization format this crate can *write*.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Format {
    /// Plain compact JSON (cross-language). The crate default.
    #[default]
    Portable,
    /// base64(JSON) — the Go SDK native format.
    DbosJson,
}

impl Format {
    pub fn name(self) -> &'static str {
        match self {
            Format::Portable => PORTABLE_NAME,
            Format::DbosJson => DBOS_JSON_NAME,
        }
    }

    /// The writable format matching a stored `serialization` name. Unknown/`None` falls back to the
    /// crate default (`portable_json`); only `DBOS_JSON` maps to the base64 format.
    pub fn from_name(name: Option<&str>) -> Format {
        match name {
            Some(DBOS_JSON_NAME) => Format::DbosJson,
            _ => Format::Portable,
        }
    }
}

// ---- single-value encode/decode (outputs, step outputs, messages, events) ---------------------

/// Encode a value in the given format. Values serializing to JSON `null` (e.g. `Option::None`) are
/// stored as `"null"` (portable) or the `__DBOS_NIL` marker (`DBOS_JSON`), matching the Go SDK.
pub fn encode_value<T: Serialize>(value: &T, fmt: Format) -> Result<String> {
    let v = serde_json::to_value(value)?;
    Ok(encode_json_value(&v, fmt))
}

fn encode_json_value(v: &Value, fmt: Format) -> String {
    match fmt {
        Format::Portable => {
            if v.is_null() {
                "null".to_string()
            } else {
                // serde_json::to_string is compact; a Value never fails to serialize.
                serde_json::to_string(v).unwrap_or_else(|_| "null".to_string())
            }
        }
        Format::DbosJson => {
            if v.is_null() {
                NIL_MARKER.to_string()
            } else {
                use base64::{engine::general_purpose::STANDARD, Engine as _};
                STANDARD.encode(serde_json::to_vec(v).unwrap_or_default())
            }
        }
    }
}

/// Decode a stored value, dispatching on the recorded `serialization` format name. A `None` format
/// (legacy/unset) is treated as `DBOS_JSON`, matching the Go SDK's decode default.
pub fn decode_value<T: DeserializeOwned>(data: Option<&str>, format: Option<&str>) -> Result<T> {
    let json = decode_to_json(data, format)?;
    Ok(serde_json::from_value(json)?)
}

fn decode_to_json(data: Option<&str>, format: Option<&str>) -> Result<Value> {
    let fmt_name = format.unwrap_or(DBOS_JSON_NAME);
    match fmt_name {
        PORTABLE_NAME => match data {
            None => Ok(Value::Null),
            Some("null") => Ok(Value::Null),
            Some(s) => Ok(serde_json::from_str(s)?),
        },
        DBOS_JSON_NAME | "" => match data {
            None => Ok(Value::Null),
            Some(s) if s == NIL_MARKER => Ok(Value::Null),
            Some(s) => {
                use base64::{engine::general_purpose::STANDARD, Engine as _};
                let bytes = STANDARD.decode(s)?;
                Ok(serde_json::from_slice(&bytes)?)
            }
        },
        SUPERJSON_NAME => Err(DbosError::other(
            "js_superjson deserialization is not yet supported (planned for the serialization milestone)",
        )),
        PY_PICKLE_NAME => Err(DbosError::other(
            "py_pickle is not portable; re-serialize such rows to portable_json on the Python side",
        )),
        other => Err(DbosError::other(format!(
            "unknown serialization format {other:?}"
        ))),
    }
}

// ---- workflow inputs (portable envelope) ------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct PortableArgs {
    #[serde(rename = "positionalArgs", default)]
    positional_args: Vec<Value>,
    #[serde(rename = "namedArgs", default)]
    named_args: serde_json::Map<String, Value>,
}

/// Encode a workflow input. In portable mode the value is wrapped in the cross-language
/// `{positionalArgs:[value], namedArgs:{}}` envelope; otherwise it is encoded as a bare value.
pub fn encode_input<T: Serialize>(value: &T, fmt: Format) -> Result<String> {
    match fmt {
        Format::Portable => {
            let envelope = PortableArgs {
                positional_args: vec![serde_json::to_value(value)?],
                named_args: serde_json::Map::new(),
            };
            Ok(serde_json::to_string(&envelope)?)
        }
        Format::DbosJson => encode_value(value, fmt),
    }
}

/// Decode a workflow input, unwrapping the portable envelope's first positional argument.
pub fn decode_input<T: DeserializeOwned>(data: Option<&str>, format: Option<&str>) -> Result<T> {
    if format == Some(PORTABLE_NAME) {
        let value = match data {
            None | Some("null") => Value::Null,
            Some(s) => {
                let envelope: PortableArgs = serde_json::from_str(s)?;
                envelope.positional_args.into_iter().next().unwrap_or(Value::Null)
            }
        };
        return Ok(serde_json::from_value(value)?);
    }
    decode_value(data, format)
}

// ---- errors -----------------------------------------------------------------------------------

/// The cross-language error envelope (`portable_json`). Identical across the Go/Python/TS SDKs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PortableWorkflowError {
    pub name: String,
    pub message: String,
    /// Application-specific code: a JSON number or string.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub code: Option<Value>,
    /// Structured error details.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub data: Option<Value>,
}

impl std::fmt::Display for PortableWorkflowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for PortableWorkflowError {}

/// A workflow error decoded from storage.
#[derive(Debug, Clone, PartialEq)]
pub enum DecodedError {
    /// A plain error string (native/non-portable formats).
    Plain(String),
    /// A structured portable error.
    Portable(PortableWorkflowError),
}

/// Serialize a workflow error for storage. For portable workflows this produces the
/// `{name,message,code?,data?}` envelope (wrapping a plain message as `name:"Portable Error"`);
/// otherwise it stores the plain message string. Matches Go `serializeWorkflowError`.
pub fn serialize_workflow_error(
    message: &str,
    portable: Option<&PortableWorkflowError>,
    fmt: Format,
) -> String {
    if fmt != Format::Portable {
        return message.to_string();
    }
    let payload = match portable {
        Some(pe) => pe.clone(),
        None => PortableWorkflowError {
            name: "Portable Error".to_string(),
            message: message.to_string(),
            code: None,
            data: None,
        },
    };
    serde_json::to_string(&payload).unwrap_or_else(|_| message.to_string())
}

/// Deserialize a stored workflow error. Portable rows parse into a [`PortableWorkflowError`];
/// everything else (and parse failures) yields the plain string. Matches Go `deserializeWorkflowError`.
pub fn deserialize_workflow_error(
    err_str: Option<&str>,
    format: Option<&str>,
) -> Option<DecodedError> {
    let s = err_str?;
    if s.is_empty() {
        return None;
    }
    if format != Some(PORTABLE_NAME) {
        return Some(DecodedError::Plain(s.to_string()));
    }
    match serde_json::from_str::<PortableWorkflowError>(s) {
        Ok(pe) => Some(DecodedError::Portable(pe)),
        Err(_) => Some(DecodedError::Plain(s.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::collections::BTreeMap;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Nested {
        deep: bool,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Sample {
        s: String,
        num: i64,
        arr: Vec<String>,
        obj: BTreeMap<String, i64>,
        nested: Nested,
        nullable: Option<i32>,
    }

    fn sample() -> Sample {
        let mut obj = BTreeMap::new();
        obj.insert("k".to_string(), 99);
        Sample {
            s: "hello".to_string(),
            num: 42,
            arr: vec!["a".to_string(), "b".to_string()],
            obj,
            nested: Nested { deep: true },
            nullable: None,
        }
    }

    fn roundtrip_value<T>(value: &T, fmt: Format)
    where
        T: Serialize + DeserializeOwned + std::fmt::Debug + PartialEq,
    {
        let encoded = encode_value(value, fmt).unwrap();
        let decoded: T = decode_value(Some(&encoded), Some(fmt.name())).unwrap();
        assert_eq!(&decoded, value, "value roundtrip via {}", fmt.name());
    }

    #[test]
    fn value_roundtrip_matrix_both_formats() {
        for fmt in [Format::Portable, Format::DbosJson] {
            roundtrip_value(&7i64, fmt);
            roundtrip_value(&String::new(), fmt);
            roundtrip_value(&"text".to_string(), fmt);
            roundtrip_value(&vec![1, 2, 3], fmt);
            roundtrip_value(&Some(123i32), fmt);
            roundtrip_value(&Option::<i32>::None, fmt);
            roundtrip_value(&sample(), fmt);
            let mut m = BTreeMap::new();
            m.insert("x".to_string(), 1);
            roundtrip_value(&m, fmt);
        }
    }

    #[test]
    fn input_roundtrip_both_formats() {
        for fmt in [Format::Portable, Format::DbosJson] {
            let encoded = encode_input(&sample(), fmt).unwrap();
            let decoded: Sample = decode_input(Some(&encoded), Some(fmt.name())).unwrap();
            assert_eq!(decoded, sample());
        }
    }

    #[test]
    fn portable_input_uses_cross_language_envelope() {
        let encoded = encode_input(&42i32, Format::Portable).unwrap();
        let v: Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(v["positionalArgs"], serde_json::json!([42]));
        assert_eq!(v["namedArgs"], serde_json::json!({}));
    }

    #[test]
    fn nil_markers() {
        // Portable null is plain "null".
        assert_eq!(encode_value(&Option::<i32>::None, Format::Portable).unwrap(), "null");
        // DBOS_JSON null is the __DBOS_NIL marker.
        assert_eq!(
            encode_value(&Option::<i32>::None, Format::DbosJson).unwrap(),
            NIL_MARKER
        );
        // Reading the marker back yields None.
        let v: Option<i32> = decode_value(Some(NIL_MARKER), Some(DBOS_JSON_NAME)).unwrap();
        assert_eq!(v, None);
        // Non-nil zero values are NOT treated as nil.
        assert_ne!(encode_value(&0i32, Format::DbosJson).unwrap(), NIL_MARKER);
        assert_ne!(encode_value(&String::new(), Format::Portable).unwrap(), "null");
    }

    #[test]
    fn reads_go_dbos_json_value() {
        // base64 of {"k":99} — the shape a Go SDK app would write under DBOS_JSON.
        use base64::{engine::general_purpose::STANDARD, Engine as _};
        let stored = STANDARD.encode(br#"{"k":99}"#);
        let decoded: BTreeMap<String, i64> = decode_value(Some(&stored), Some(DBOS_JSON_NAME)).unwrap();
        assert_eq!(decoded.get("k"), Some(&99));
    }

    #[test]
    fn legacy_unset_format_decodes_as_dbos_json() {
        use base64::{engine::general_purpose::STANDARD, Engine as _};
        let stored = STANDARD.encode(b"123");
        let decoded: i32 = decode_value(Some(&stored), None).unwrap();
        assert_eq!(decoded, 123);
    }

    #[test]
    fn portable_error_envelope_number_and_string_codes() {
        let with_num = PortableWorkflowError {
            name: "ValidationError".to_string(),
            message: "invalid input".to_string(),
            code: Some(serde_json::json!(400)),
            data: Some(serde_json::json!({"field": "input"})),
        };
        let s = serialize_workflow_error("ignored", Some(&with_num), Format::Portable);
        let v: Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["name"], "ValidationError");
        assert_eq!(v["message"], "invalid input");
        assert_eq!(v["code"], serde_json::json!(400));
        assert_eq!(v["data"]["field"], "input");

        // String code is preserved.
        let with_str = PortableWorkflowError {
            name: "E".to_string(),
            message: "m".to_string(),
            code: Some(serde_json::json!("NOT_FOUND")),
            data: None,
        };
        let s = serialize_workflow_error("ignored", Some(&with_str), Format::Portable);
        let v: Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["code"], "NOT_FOUND");
        assert!(v.get("data").is_none(), "omitempty drops null data");
    }

    #[test]
    fn native_error_is_plain_string() {
        let s = serialize_workflow_error("boom", None, Format::DbosJson);
        assert_eq!(s, "boom");
        match deserialize_workflow_error(Some("boom"), Some(DBOS_JSON_NAME)) {
            Some(DecodedError::Plain(p)) => assert_eq!(p, "boom"),
            other => panic!("expected plain, got {other:?}"),
        }
    }

    #[test]
    fn portable_plain_error_best_effort_wrap() {
        let s = serialize_workflow_error("something went wrong", None, Format::Portable);
        match deserialize_workflow_error(Some(&s), Some(PORTABLE_NAME)) {
            Some(DecodedError::Portable(pe)) => {
                assert_eq!(pe.name, "Portable Error");
                assert_eq!(pe.message, "something went wrong");
            }
            other => panic!("expected portable, got {other:?}"),
        }
    }

    #[test]
    fn unsupported_formats_error_clearly() {
        let err = decode_value::<Value>(Some("x"), Some(SUPERJSON_NAME)).unwrap_err();
        assert!(err.to_string().contains("js_superjson"));
        let err = decode_value::<Value>(Some("x"), Some(PY_PICKLE_NAME)).unwrap_err();
        assert!(err.to_string().contains("py_pickle"));
        let err = decode_value::<Value>(Some("x"), Some("weird")).unwrap_err();
        assert!(err.to_string().contains("unknown serialization format"));
    }
}

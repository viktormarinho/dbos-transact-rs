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
//! - **`js_superjson`** (read-only) — the TypeScript SDK's native default. We can *read* it (both
//!   the SuperJSON envelope and the legacy `DBOSJSON` format) so a Rust app can ingest an existing
//!   TS-DBOS database; we never write it.
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
/// (a `serialization = NULL` row, e.g. legacy or cross-SDK) is decoded **best-effort** rather than
/// assumed to be Go's base64 `DBOS_JSON`: a TS-origin NULL row holds plain JSON / SuperJSON, and
/// base64-decoding that would corrupt the data.
pub fn decode_value<T: DeserializeOwned>(data: Option<&str>, format: Option<&str>) -> Result<T> {
    let json = decode_to_json(data, format)?;
    Ok(serde_json::from_value(json)?)
}

fn decode_base64_json(s: &str) -> Result<Value> {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    let bytes = STANDARD.decode(s)?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn decode_to_json(data: Option<&str>, format: Option<&str>) -> Result<Value> {
    match format {
        Some(PORTABLE_NAME) => match data {
            None | Some("null") => Ok(Value::Null),
            Some(s) => Ok(serde_json::from_str(s)?),
        },
        Some(DBOS_JSON_NAME) | Some("") => match data {
            None => Ok(Value::Null),
            Some(s) if s == NIL_MARKER => Ok(Value::Null),
            Some(s) => decode_base64_json(s),
        },
        Some(SUPERJSON_NAME) => match data {
            None => Ok(Value::Null),
            Some(s) => decode_superjson(s),
        },
        Some(PY_PICKLE_NAME) => Err(DbosError::other(
            "py_pickle is not portable; re-serialize such rows to portable_json on the Python side",
        )),
        Some(other) => Err(DbosError::other(format!(
            "unknown serialization format {other:?}"
        ))),
        // No recorded format: the origin SDK is unknown. Decode best-effort, covering
        // Rust/TS (plain JSON, SuperJSON, legacy DBOSJSON wrappers) AND Go (base64 DBOS_JSON).
        // SuperJSON/plain JSON is tried first; base64 strings are not valid JSON, so they fall
        // through to the base64 branch, while a TS plain-JSON row is decoded correctly.
        None => match data {
            None | Some("null") => Ok(Value::Null),
            Some(s) if s == NIL_MARKER => Ok(Value::Null),
            Some(s) => decode_superjson(s).or_else(|_| decode_base64_json(s)),
        },
    }
}

// ---- js_superjson (read-only: TypeScript SDK interop) -----------------------------------------

const SUPERJSON_MARKER: &str = "\"__dbos_serializer\":\"superjson\"";

/// Decode a TypeScript-SDK `js_superjson` payload into a plain JSON value. Handles both the modern
/// SuperJSON envelope (`{json, meta, __dbos_serializer:"superjson"}`) and the legacy `DBOSJSON`
/// format (plain JSON with `{dbos_type:"dbos_Date"|"dbos_BigInt"}` wrappers). Rich JS types are
/// lowered to JSON-native equivalents (Date → ISO string, BigInt → number, Map → object,
/// undefined → null) so the result deserializes with serde.
fn decode_superjson(data: &str) -> Result<Value> {
    if data.contains(SUPERJSON_MARKER) {
        let mut outer: Value = serde_json::from_str(data)?;
        let obj = outer
            .as_object_mut()
            .ok_or_else(|| DbosError::other("invalid js_superjson payload: not an object"))?;
        let mut json = obj.remove("json").unwrap_or(Value::Null);
        if let Some(values) = obj.get("meta").and_then(|m| m.get("values")) {
            apply_superjson_meta(&mut json, values);
        }
        Ok(json)
    } else {
        // Legacy DBOSJSON: plain JSON with type wrappers.
        let mut v: Value = serde_json::from_str(data)?;
        apply_legacy_revival(&mut v);
        Ok(v)
    }
}

/// Walk SuperJSON's `meta.values` annotation tree, transforming the matching JSON nodes.
fn apply_superjson_meta(json: &mut Value, annotation: &Value) {
    match annotation {
        // A leaf type annotation applies to this node.
        Value::String(tag) => apply_superjson_tag(json, tag),
        // Compound annotation `[type, sub-annotations]` (e.g. a Map with typed values).
        Value::Array(parts) => {
            if let Some(Value::String(tag)) = parts.first() {
                if let Some(sub) = parts.get(1) {
                    apply_superjson_meta(json, sub);
                }
                apply_superjson_tag(json, tag);
            }
        }
        // A branch: keys navigate into the JSON.
        Value::Object(children) => {
            for (key, child) in children {
                if let Some(node) = navigate_mut(json, key) {
                    apply_superjson_meta(node, child);
                }
            }
        }
        _ => {}
    }
}

fn apply_superjson_tag(json: &mut Value, tag: &str) {
    match tag {
        // BigInt is stored as a string; lower to a JSON number when it fits.
        "bigint" => {
            if let Value::String(s) = json {
                if let Ok(n) = s.parse::<i64>() {
                    *json = Value::Number(n.into());
                } else if let Ok(n) = s.parse::<u64>() {
                    *json = Value::Number(n.into());
                }
            }
        }
        // A Map is stored as an array of [key, value] pairs; lower to an object (string keys).
        "map" => {
            if let Value::Array(pairs) = json {
                let mut obj = serde_json::Map::new();
                let mut ok = true;
                for pair in pairs.iter() {
                    match pair {
                        Value::Array(kv) if kv.len() == 2 => match &kv[0] {
                            Value::String(k) => {
                                obj.insert(k.clone(), kv[1].clone());
                            }
                            other => {
                                obj.insert(other.to_string(), kv[1].clone());
                            }
                        },
                        _ => {
                            ok = false;
                            break;
                        }
                    }
                }
                if ok {
                    *json = Value::Object(obj);
                }
            }
        }
        // Date → ISO string, Set → array, undefined → null, regexp/URL/Error → string/object:
        // already JSON-native, leave as-is.
        _ => {}
    }
}

fn navigate_mut<'a>(json: &'a mut Value, key: &str) -> Option<&'a mut Value> {
    match json {
        Value::Object(map) => map.get_mut(key),
        Value::Array(arr) => key.parse::<usize>().ok().and_then(move |i| arr.get_mut(i)),
        _ => None,
    }
}

/// Revive the legacy `DBOSJSON` type wrappers in place: `{dbos_type:"dbos_Date", dbos_data}` → the
/// ISO string, `{dbos_type:"dbos_BigInt", dbos_data}` → a number.
fn apply_legacy_revival(value: &mut Value) {
    match value {
        Value::Object(map) => {
            if let Some(Value::String(t)) = map.get("dbos_type") {
                match t.as_str() {
                    "dbos_Date" => {
                        if let Some(data) = map.get("dbos_data").cloned() {
                            *value = data;
                            return;
                        }
                    }
                    "dbos_BigInt" => {
                        if let Some(Value::String(s)) = map.get("dbos_data") {
                            if let Ok(n) = s.parse::<i64>() {
                                *value = Value::Number(n.into());
                                return;
                            } else if let Ok(n) = s.parse::<u64>() {
                                *value = Value::Number(n.into());
                                return;
                            }
                        }
                    }
                    _ => {}
                }
            }
            for child in map.values_mut() {
                apply_legacy_revival(child);
            }
        }
        Value::Array(arr) => {
            for child in arr.iter_mut() {
                apply_legacy_revival(child);
            }
        }
        _ => {}
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

/// Decode a workflow input, unwrapping the first positional argument. Portable inputs use the
/// `{positionalArgs,namedArgs}` envelope; TS-native (`js_superjson`) inputs are the bare
/// positional-args array.
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
    if format == Some(SUPERJSON_NAME) {
        let value = match data {
            None => Value::Null,
            Some(s) => match decode_superjson(s)? {
                Value::Array(arr) => arr.into_iter().next().unwrap_or(Value::Null),
                other => other,
            },
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
        // A Go-origin `serialization = NULL` row (base64 DBOS_JSON) still decodes via the fallback.
        use base64::{engine::general_purpose::STANDARD, Engine as _};
        let stored = STANDARD.encode(b"123");
        let decoded: i32 = decode_value(Some(&stored), None).unwrap();
        assert_eq!(decoded, 123);
    }

    #[test]
    fn null_serialization_decodes_ts_origin_rows_not_as_base64() {
        // The corruption trap: a TS-origin `serialization = NULL` row holds plain JSON / SuperJSON,
        // and must NOT be base64-decoded.
        let n: i64 = decode_value(Some("42"), None).unwrap();
        assert_eq!(n, 42);
        let s: String = decode_value(Some("\"hello\""), None).unwrap();
        assert_eq!(s, "hello");

        #[derive(Debug, PartialEq, Deserialize)]
        struct V {
            a: i64,
            b: String,
        }
        let obj: V = decode_value(Some(r#"{"a":1,"b":"x"}"#), None).unwrap();
        assert_eq!(obj, V { a: 1, b: "x".to_string() });

        // Legacy DBOSJSON wrappers (Date/BigInt) under NULL serialization are revived too.
        let big: i64 =
            decode_value(Some(r#"{"dbos_type":"dbos_BigInt","dbos_data":"123"}"#), None).unwrap();
        assert_eq!(big, 123);

        // And a Go base64 row under NULL still decodes (the fallback covers both origins).
        use base64::{engine::general_purpose::STANDARD, Engine as _};
        let go = STANDARD.encode(br#"{"k":99}"#);
        let map: BTreeMap<String, i64> = decode_value(Some(&go), None).unwrap();
        assert_eq!(map.get("k"), Some(&99));
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
        let err = decode_value::<Value>(Some("x"), Some(PY_PICKLE_NAME)).unwrap_err();
        assert!(err.to_string().contains("py_pickle"));
        let err = decode_value::<Value>(Some("x"), Some("weird")).unwrap_err();
        assert!(err.to_string().contains("unknown serialization format"));
    }

    // ---- js_superjson reader (TypeScript SDK interop) -----------------------------------------

    fn sj<T: DeserializeOwned>(stored: &str) -> T {
        decode_value::<T>(Some(stored), Some(SUPERJSON_NAME)).unwrap()
    }

    #[test]
    fn reads_plain_superjson() {
        #[derive(Debug, PartialEq, Deserialize)]
        struct V {
            a: i64,
            b: String,
        }
        // superjson.serialize({a:1,b:"x"}) has no meta.
        let stored = r#"{"json":{"a":1,"b":"x"},"__dbos_serializer":"superjson"}"#;
        assert_eq!(sj::<V>(stored), V { a: 1, b: "x".to_string() });
    }

    #[test]
    fn reads_superjson_rich_types() {
        // Date → ISO string; BigInt → number; undefined → null (None).
        #[derive(Debug, PartialEq, Deserialize)]
        struct V {
            when: String,
            big: i64,
            missing: Option<i32>,
        }
        let stored = r#"{"json":{"when":"2024-01-02T03:04:05.000Z","big":"9007199254740993","missing":null},
            "meta":{"values":{"when":"Date","big":"bigint","missing":"undefined"}},
            "__dbos_serializer":"superjson"}"#;
        assert_eq!(
            sj::<V>(stored),
            V {
                when: "2024-01-02T03:04:05.000Z".to_string(),
                big: 9007199254740993,
                missing: None,
            }
        );
    }

    #[test]
    fn reads_superjson_root_bigint() {
        let stored = r#"{"json":"99","meta":{"values":"bigint"},"__dbos_serializer":"superjson"}"#;
        assert_eq!(sj::<i64>(stored), 99);
    }

    #[test]
    fn reads_superjson_map() {
        // A JS Map serializes to an array of [k,v] pairs annotated "map".
        let stored = r#"{"json":[["a",1],["b",2]],"meta":{"values":"map"},"__dbos_serializer":"superjson"}"#;
        let m: BTreeMap<String, i64> = sj(stored);
        assert_eq!(m.get("a"), Some(&1));
        assert_eq!(m.get("b"), Some(&2));
    }

    #[test]
    fn reads_legacy_dbosjson() {
        // Legacy DBOSJSON (no superjson marker): Date/BigInt wrappers.
        #[derive(Debug, PartialEq, Deserialize)]
        struct V {
            when: String,
            big: i64,
            plain: i32,
        }
        let stored = r#"{"when":{"dbos_type":"dbos_Date","dbos_data":"2024-01-02T03:04:05.000Z"},
            "big":{"dbos_type":"dbos_BigInt","dbos_data":"123456789012345"},"plain":7}"#;
        assert_eq!(
            sj::<V>(stored),
            V {
                when: "2024-01-02T03:04:05.000Z".to_string(),
                big: 123456789012345,
                plain: 7,
            }
        );
    }
}

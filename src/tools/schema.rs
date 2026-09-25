//! Lower tool input schemas to the subset every MCP client's model provider
//! accepts.
//!
//! schemars emits idiomatic JSON Schema 2020-12: `Option<T>` becomes
//! `"type": ["T", "null"]`, nested structs become `$ref`s into `$defs`, and an
//! `Option<Struct>` becomes `anyOf: [{$ref}, {type: null}]`. Claude and Codex
//! accept all of that, but Gemini's function-declaration validator does not,
//! and clients that translate for it make things worse: opencode rewrites
//! `"type": ["array", "null"]` into `anyOf: [{type: array}]` with the `items`
//! left outside the `anyOf`, which Gemini rejects as an array without
//! `items`, failing the whole request — every tool of every server, not just
//! ours.
//!
//! None of those constructs carries information a caller needs: an optional
//! field is already optional by being absent from `required`, and the server
//! deserialises an explicit `null` the same as an omitted field. So every
//! input schema is rewritten once into plain single-typed properties:
//!
//! - `"type": [T, "null"]` becomes `"type": T`,
//! - `null` branches are dropped from `anyOf` / `oneOf`, and a single
//!   remaining branch is merged into its parent,
//! - `$ref`s are inlined from `$defs`, which is then removed,
//! - a `"default": null` (no longer a valid value) is removed,
//! - every array has an `items` schema.
//!
//! Output schemas are left alone: they describe results, are never part of a
//! function declaration, and changing them would change what clients that do
//! validate structured output check against.

use rmcp::model::JsonObject;
use serde_json::{Map, Value};

/// How deep `$ref` inlining may go before giving up. The bus has no
/// recursive input types; the bound only keeps a future one from looping.
const MAX_INLINE_DEPTH: usize = 16;

/// Rewrite one tool input schema into the portable subset described above.
pub fn portable_input_schema(schema: &JsonObject) -> JsonObject {
    let mut root = schema.clone();
    let mut defs = Map::new();
    for key in ["$defs", "definitions"] {
        if let Some(Value::Object(d)) = root.remove(key) {
            defs.extend(d);
        }
    }
    let mut value = Value::Object(root);
    lower(&mut value, &defs, 0);
    match value {
        Value::Object(map) => map,
        // `lower` never changes an object into anything else.
        _ => unreachable!("an object schema stays an object"),
    }
}

fn is_null_schema(v: &Value) -> bool {
    v.get("type").and_then(Value::as_str) == Some("null")
}

fn resolve_ref<'a>(reference: &str, defs: &'a Map<String, Value>) -> Option<&'a Value> {
    let name = reference
        .strip_prefix("#/$defs/")
        .or_else(|| reference.strip_prefix("#/definitions/"))?;
    defs.get(name)
}

fn lower(node: &mut Value, defs: &Map<String, Value>, depth: usize) {
    match node {
        Value::Array(items) => {
            for item in items {
                lower(item, defs, depth);
            }
        }
        Value::Object(map) => lower_object(map, defs, depth),
        _ => {}
    }
}

fn lower_object(map: &mut Map<String, Value>, defs: &Map<String, Value>, depth: usize) {
    // $ref: splice the definition in; sibling keywords (a field's own
    // description) win over the definition's.
    if let Some(Value::String(reference)) = map.get("$ref").cloned() {
        map.remove("$ref");
        match resolve_ref(&reference, defs) {
            Some(Value::Object(def)) if depth < MAX_INLINE_DEPTH => {
                for (k, v) in def {
                    map.entry(k.clone()).or_insert_with(|| v.clone());
                }
            }
            // Unresolvable or too deep: an open object is the honest fallback.
            _ => {
                map.entry("type").or_insert_with(|| Value::from("object"));
            }
        }
        // The spliced definition may itself use $ref.
        return lower_object(map, defs, depth + 1);
    }

    // anyOf / oneOf with null branches: drop them; one survivor is merged in.
    for key in ["anyOf", "oneOf"] {
        let Some(Value::Array(branches)) = map.get(key).cloned() else {
            continue;
        };
        let kept: Vec<Value> = branches
            .into_iter()
            .filter(|b| !is_null_schema(b))
            .collect();
        map.remove(key);
        match kept.len() {
            0 => {}
            1 => {
                if let Value::Object(only) = &kept[0] {
                    for (k, v) in only {
                        map.entry(k.clone()).or_insert_with(|| v.clone());
                    }
                }
            }
            _ => {
                map.insert(key.to_owned(), Value::Array(kept));
            }
        }
    }
    // Merging a branch may have brought in a $ref.
    if map.contains_key("$ref") {
        return lower_object(map, defs, depth + 1);
    }

    // "type": [T, "null"] -> "type": T.
    if let Some(Value::Array(types)) = map.get("type") {
        let non_null: Vec<Value> = types
            .iter()
            .filter(|t| t.as_str() != Some("null"))
            .cloned()
            .collect();
        match non_null.len() {
            0 => {
                map.remove("type");
            }
            1 => {
                map.insert("type".into(), non_null[0].clone());
            }
            _ => {
                map.insert("type".into(), Value::Array(non_null));
            }
        }
    }

    if map.get("default").is_some_and(Value::is_null) {
        map.remove("default");
    }

    if map.get("type").and_then(Value::as_str) == Some("array") && !map.contains_key("items") {
        map.insert("items".into(), serde_json::json!({ "type": "string" }));
    }

    // Recurse into every subschema position.
    if let Some(Value::Object(props)) = map.get_mut("properties") {
        for prop in props.values_mut() {
            lower(prop, defs, depth);
        }
    }
    for key in [
        "items",
        "additionalProperties",
        "anyOf",
        "oneOf",
        "allOf",
        "prefixItems",
    ] {
        if let Some(sub) = map.get_mut(key) {
            lower(sub, defs, depth);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn obj(v: Value) -> JsonObject {
        match v {
            Value::Object(m) => m,
            _ => panic!("not an object"),
        }
    }

    #[test]
    fn nullable_array_becomes_a_plain_array_with_items() {
        let out = portable_input_schema(&obj(json!({
            "type": "object",
            "properties": {
                "tags": {"type": ["array", "null"], "items": {"type": "string"}, "default": null},
                "limit": {"type": ["integer", "null"], "format": "int64"}
            }
        })));
        assert_eq!(
            Value::Object(out),
            json!({
                "type": "object",
                "properties": {
                    "tags": {"type": "array", "items": {"type": "string"}},
                    "limit": {"type": "integer", "format": "int64"}
                }
            })
        );
    }

    #[test]
    fn refs_are_inlined_and_defs_removed() {
        let out = portable_input_schema(&obj(json!({
            "$defs": {"File": {"type": "object", "properties": {
                "name": {"type": "string"},
                "mime": {"type": ["string", "null"], "default": null}
            }, "required": ["name"]}},
            "type": "object",
            "properties": {
                "files": {"type": ["array", "null"], "items": {"$ref": "#/$defs/File"},
                          "description": "the files"},
                "one": {"anyOf": [{"$ref": "#/$defs/File"}, {"type": "null"}]}
            }
        })));
        let file = json!({"type": "object", "properties": {
            "name": {"type": "string"}, "mime": {"type": "string"}
        }, "required": ["name"]});
        assert_eq!(
            Value::Object(out),
            json!({
                "type": "object",
                "properties": {
                    "files": {"type": "array", "items": file, "description": "the files"},
                    "one": file
                }
            })
        );
    }

    #[test]
    fn an_array_without_items_gets_one() {
        let out = portable_input_schema(&obj(json!({
            "type": "object",
            "properties": {"kinds": {"type": ["array", "null"]}}
        })));
        assert_eq!(
            out["properties"]["kinds"]["items"],
            json!({"type": "string"})
        );
    }

    #[test]
    fn a_recursive_ref_terminates() {
        let out = portable_input_schema(&obj(json!({
            "$defs": {"Node": {"type": "object", "properties": {
                "child": {"$ref": "#/$defs/Node"}
            }}},
            "type": "object",
            "properties": {"root": {"$ref": "#/$defs/Node"}}
        })));
        assert!(!serde_json::to_string(&out).unwrap().contains("$ref"));
    }
}

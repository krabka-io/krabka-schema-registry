//! Structural diff between two JSON Schema documents, each a
//! `serde_json::Value`, which mirrors Confluent's json.diff. `compat.rs`
//! classifies the result. This module has no direction logic. The engine swaps
//! (reader, writer) per level.

use std::collections::{BTreeSet, HashSet};

use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kind {
    // --- Type ---
    TypeNarrowed,
    TypeExtended,
    TypeChanged,
    // --- Properties ---
    PropertyAddedToOpenContentModel,
    PropertyRemovedFromOpenContentModel,
    PropertyAddedToClosedContentModel,
    PropertyRemovedFromClosedContentModel,
    PropertyAddedCoveredByPartiallyOpenContentModel,
    PropertyAddedNotCoveredByPartiallyOpenContentModel,
    PropertyRemovedCoveredByPartiallyOpenContentModel,
    PropertyRemovedNotCoveredByPartiallyOpenContentModel,
    PropertyWithEmptySchemaAddedToOpenContentModel,
    // --- Required ---
    RequiredAttributeAdded,
    RequiredAttributeRemoved,
    RequiredAttributeWithDefaultAdded,
    RequiredPropertyWithDefaultAddedToClosedContentModel,
    // --- AdditionalProperties ---
    AdditionalPropertiesRemoved,
    AdditionalPropertiesAdded,
    AdditionalPropertiesNarrowed,
    AdditionalPropertiesExtended,
    // --- Enum / const ---
    EnumArrayNarrowed,
    EnumArrayExtended,
    EnumArrayChanged,
    // --- Numeric bounds ---
    MaximumAdded,
    MaximumRemoved,
    MaximumDecreased,
    MaximumIncreased,
    MinimumAdded,
    MinimumRemoved,
    MinimumDecreased,
    MinimumIncreased,
    ExclusiveMaximumAdded,
    ExclusiveMaximumRemoved,
    ExclusiveMaximumDecreased,
    ExclusiveMaximumIncreased,
    ExclusiveMinimumAdded,
    ExclusiveMinimumRemoved,
    ExclusiveMinimumDecreased,
    ExclusiveMinimumIncreased,
    MultipleOfAdded,
    MultipleOfRemoved,
    MultipleOfReduced,
    MultipleOfExpanded,
    MultipleOfChanged,
    // --- String ---
    MaxLengthAdded,
    MaxLengthRemoved,
    MaxLengthDecreased,
    MaxLengthIncreased,
    MinLengthAdded,
    MinLengthRemoved,
    MinLengthDecreased,
    MinLengthIncreased,
    PatternAdded,
    PatternRemoved,
    PatternChanged,
    // --- Array ---
    MaxItemsAdded,
    MaxItemsRemoved,
    MaxItemsDecreased,
    MaxItemsIncreased,
    MinItemsAdded,
    MinItemsRemoved,
    MinItemsDecreased,
    MinItemsIncreased,
    AdditionalItemsRemoved,
    AdditionalItemsAdded,
    AdditionalItemsNarrowed,
    AdditionalItemsExtended,
    UniqueItemsAdded,
    UniqueItemsRemoved,
    // --- Object size ---
    MaxPropertiesAdded,
    MaxPropertiesRemoved,
    MaxPropertiesDecreased,
    MaxPropertiesIncreased,
    MinPropertiesAdded,
    MinPropertiesRemoved,
    MinPropertiesDecreased,
    MinPropertiesIncreased,
    // --- Combinators ---
    CombinedTypeChanged,
    CombinedTypeExtended,
    ProductTypeExtended,
    ProductTypeNarrowed,
    SumTypeExtended,
    SumTypeNarrowed,
    #[allow(dead_code)] // retained for wider not-schema diagnostics
    NotTypeExtended,
    NotTypeNarrowed,
    CombinedTypeSubschemasChanged,
    // --- $ref / dependencies / conditionals ---
    DependencyArrayAdded,
    DependencyArrayRemoved,
    DependencyArrayExtended,
    DependencyArrayNarrowed,
    DependencyArrayChanged,
    DependencySchemaAdded,
    DependencySchemaRemoved,
    ConditionalChanged,
}

#[derive(Debug, Clone)]
pub struct Difference {
    pub kind: Kind,
    pub path: String,
}

fn d(kind: Kind, path: &str) -> Difference {
    Difference {
        kind,
        path: path.to_string(),
    }
}

/// A side's registry references as `(name, document)` pairs. `name` is the
/// `$ref` target string a referring schema uses to point at that document.
type RefMap = [(String, Value)];

/// Context for $ref resolution. It carries each side's document root and
/// registry ref-map, and a cycle-guard set of the `(orig_ptr, upd_ptr)` pairs
/// already visited.
struct Ctx<'a> {
    orig_root: &'a Value,
    upd_root: &'a Value,
    orig_refs: &'a RefMap,
    upd_refs: &'a RefMap,
    visited: HashSet<(String, String)>,
}

impl<'a> Ctx<'a> {
    fn new(
        orig_root: &'a Value,
        upd_root: &'a Value,
        orig_refs: &'a RefMap,
        upd_refs: &'a RefMap,
    ) -> Self {
        Ctx {
            orig_root,
            upd_root,
            orig_refs,
            upd_refs,
            visited: HashSet::new(),
        }
    }
}

/// Diff two JSON Schema documents. Each side carries a registry ref-map so a
/// `$ref` whose target is not an intra-document `#/...` pointer can resolve
/// against a registered reference's document. With empty ref-maps an unmatched
/// non-`#` `$ref` stays permissive.
#[must_use]
pub fn compare_with_refs(
    original: &Value,
    update: &Value,
    original_refs: &RefMap,
    update_refs: &RefMap,
) -> Vec<Difference> {
    let mut out = Vec::new();
    let mut ctx = Ctx::new(original, update, original_refs, update_refs);
    compare_schema("#", original, update, &mut ctx, &mut out);
    out
}

fn compare_schema(
    path: &str,
    orig: &Value,
    upd: &Value,
    ctx: &mut Ctx<'_>,
    out: &mut Vec<Difference>,
) {
    if compare_refs(path, orig, upd, ctx, out) {
        return;
    }
    compare_type(path, orig, upd, out);
    compare_enum(path, orig, upd, out);
    compare_properties(path, orig, upd, ctx, out);
    compare_required(path, orig, upd, out);
    compare_additional_properties(path, orig, upd, ctx, out);
    compare_numeric(path, orig, upd, out);
    compare_string_constraints(path, orig, upd, out);
    compare_array_constraints(path, orig, upd, ctx, out);
    compare_object_size(path, orig, upd, out);
    compare_combinators(path, orig, upd, ctx, out);
    compare_dependencies(path, orig, upd, ctx, out);
    compare_conditionals(path, orig, upd, ctx, out);
}

fn types_of(schema: &Value) -> BTreeSet<String> {
    match schema.get("type") {
        Some(Value::String(s)) => BTreeSet::from([s.clone()]),
        Some(Value::Array(a)) => a
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect(),
        _ => BTreeSet::new(),
    }
}

fn compare_type(path: &str, orig: &Value, upd: &Value, out: &mut Vec<Difference>) {
    let (ot, ut) = (types_of(orig), types_of(upd));
    if ot == ut {
        return;
    }
    if ot == BTreeSet::from(["integer".into()]) && ut == BTreeSet::from(["number".into()]) {
        out.push(d(Kind::TypeExtended, path));
    } else if (ot == BTreeSet::from(["number".into()]) && ut == BTreeSet::from(["integer".into()]))
        || (ot.is_empty() && !ut.is_empty())
    {
        out.push(d(Kind::TypeNarrowed, path));
    } else if ut.is_empty() && !ot.is_empty() {
        out.push(d(Kind::TypeExtended, path));
    } else if ut.is_subset(&ot) {
        out.push(d(Kind::TypeNarrowed, path));
    } else if ot.is_subset(&ut) {
        out.push(d(Kind::TypeExtended, path));
    } else {
        out.push(d(Kind::TypeChanged, path));
    }
}

enum ContentModel<'a> {
    Open,
    Partial,
    Closed,
    Schema(&'a Value),
}

fn content_model(schema: &Value) -> ContentModel<'_> {
    match schema.get("additionalProperties") {
        Some(Value::Bool(false)) => ContentModel::Closed,
        Some(Value::Bool(true)) => ContentModel::Open,
        Some(value) if value.is_object() => ContentModel::Schema(value),
        _ if schema
            .get("patternProperties")
            .is_some_and(Value::is_object) =>
        {
            ContentModel::Partial
        }
        _ => ContentModel::Open,
    }
}

fn covering_schemas<'a>(schema: &'a Value, property: &str) -> Vec<&'a Value> {
    let mut result = Vec::new();
    if let ContentModel::Schema(value) = content_model(schema) {
        result.push(value);
    }
    if let Some(patterns) = schema.get("patternProperties").and_then(Value::as_object) {
        result.extend(patterns.iter().filter_map(|(pattern, value)| {
            regex::Regex::new(pattern)
                .ok()
                .filter(|regex| regex.is_match(property))
                .map(|_| value)
        }));
    }
    result
}

fn props(schema: &Value) -> Option<&serde_json::Map<String, Value>> {
    schema.get("properties").and_then(Value::as_object)
}

fn required_set(schema: &Value) -> BTreeSet<String> {
    schema
        .get("required")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

fn compare_properties(
    path: &str,
    orig: &Value,
    upd: &Value,
    ctx: &mut Ctx<'_>,
    out: &mut Vec<Difference>,
) {
    let empty = serde_json::Map::new();
    let op = props(orig).unwrap_or(&empty);
    let up = props(upd).unwrap_or(&empty);
    for name in op.keys() {
        if !up.contains_key(name) {
            let property_path = format!("{path}/properties/{name}");
            let covering = covering_schemas(upd, name);
            match content_model(upd) {
                ContentModel::Open => {
                    out.push(d(Kind::PropertyRemovedFromOpenContentModel, &property_path));
                }
                ContentModel::Closed => out.push(d(
                    Kind::PropertyRemovedFromClosedContentModel,
                    &property_path,
                )),
                ContentModel::Partial | ContentModel::Schema(_) if covering.is_empty() => {
                    out.push(d(
                        Kind::PropertyRemovedNotCoveredByPartiallyOpenContentModel,
                        &property_path,
                    ));
                }
                ContentModel::Partial | ContentModel::Schema(_) => {
                    out.push(d(
                        Kind::PropertyRemovedCoveredByPartiallyOpenContentModel,
                        &property_path,
                    ));
                    for allowed in covering {
                        compare_schema(&property_path, &op[name], allowed, ctx, out);
                    }
                }
            }
        }
    }
    for (name, uschema) in up {
        match op.get(name) {
            None => {
                let property_path = format!("{path}/properties/{name}");
                if required_set(upd).contains(name)
                    && uschema.get("default").is_some()
                    && !matches!(content_model(orig), ContentModel::Open)
                {
                    out.push(d(
                        Kind::RequiredPropertyWithDefaultAddedToClosedContentModel,
                        &property_path,
                    ));
                    continue;
                }
                if required_set(upd).contains(name) && uschema.get("default").is_none() {
                    out.push(d(Kind::RequiredAttributeAdded, &property_path));
                }
                let covering = covering_schemas(orig, name);
                match content_model(orig) {
                    ContentModel::Open
                        if uschema.as_object().is_some_and(serde_json::Map::is_empty) =>
                    {
                        out.push(d(
                            Kind::PropertyWithEmptySchemaAddedToOpenContentModel,
                            &property_path,
                        ));
                    }
                    ContentModel::Open => {
                        out.push(d(Kind::PropertyAddedToOpenContentModel, &property_path));
                    }
                    ContentModel::Closed => {
                        out.push(d(Kind::PropertyAddedToClosedContentModel, &property_path));
                    }
                    ContentModel::Partial | ContentModel::Schema(_) if covering.is_empty() => out
                        .push(d(
                            Kind::PropertyAddedNotCoveredByPartiallyOpenContentModel,
                            &property_path,
                        )),
                    ContentModel::Partial | ContentModel::Schema(_) => {
                        out.push(d(
                            Kind::PropertyAddedCoveredByPartiallyOpenContentModel,
                            &property_path,
                        ));
                        for allowed in covering {
                            compare_schema(&property_path, allowed, uschema, ctx, out);
                        }
                    }
                }
            }
            Some(oschema) => {
                compare_schema(
                    &format!("{path}/properties/{name}"),
                    oschema,
                    uschema,
                    ctx,
                    out,
                );
            }
        }
    }
}

fn compare_required(path: &str, orig: &Value, upd: &Value, out: &mut Vec<Difference>) {
    let (orq, urq) = (required_set(orig), required_set(upd));
    let empty = serde_json::Map::new();
    let op = props(orig).unwrap_or(&empty);
    let up = props(upd).unwrap_or(&empty);
    for name in urq
        .difference(&orq)
        .filter(|name| op.contains_key(*name) && up.contains_key(*name))
    {
        out.push(d(
            if up[name].get("default").is_some() {
                Kind::RequiredAttributeWithDefaultAdded
            } else {
                Kind::RequiredAttributeAdded
            },
            &format!("{path}/required/{name}"),
        ));
    }
    for name in orq.difference(&urq) {
        out.push(d(
            Kind::RequiredAttributeRemoved,
            &format!("{path}/required/{name}"),
        ));
    }
}

fn compare_additional_properties(
    path: &str,
    orig: &Value,
    upd: &Value,
    ctx: &mut Ctx<'_>,
    out: &mut Vec<Difference>,
) {
    let oa = orig.get("additionalProperties");
    let ua = upd.get("additionalProperties");
    let o_false = matches!(oa, Some(Value::Bool(false)));
    let u_false = matches!(ua, Some(Value::Bool(false)));
    if o_false && !u_false {
        out.push(d(Kind::AdditionalPropertiesAdded, path));
    } else if !o_false && u_false {
        out.push(d(Kind::AdditionalPropertiesRemoved, path));
    } else if oa.is_none() && ua.is_some_and(|value| !value.is_boolean()) {
        out.push(d(Kind::AdditionalPropertiesNarrowed, path));
    } else if ua.is_none() && oa.is_some_and(|value| !value.is_boolean()) {
        out.push(d(Kind::AdditionalPropertiesExtended, path));
    } else if let (Some(oa), Some(ua)) = (oa, ua)
        && !oa.is_boolean()
        && !ua.is_boolean()
    {
        compare_schema(&format!("{path}/additionalProperties"), oa, ua, ctx, out);
    }
}

// ---------------------------------------------------------------------------
// Enum / const
// ---------------------------------------------------------------------------

fn canonical_value(v: &Value) -> String {
    crate::format::json::canonicalize(v)
}

fn compare_enum(path: &str, orig: &Value, upd: &Value, out: &mut Vec<Difference>) {
    let oe = enum_set(orig);
    let ue = enum_set(upd);
    if oe == ue {
        return;
    }
    match (oe, ue) {
        (Some(os), Some(us)) => {
            if us.is_subset(&os) {
                out.push(d(Kind::EnumArrayNarrowed, path));
            } else if os.is_subset(&us) {
                out.push(d(Kind::EnumArrayExtended, path));
            } else {
                out.push(d(Kind::EnumArrayChanged, path));
            }
        }
        (None, Some(_)) => out.push(d(Kind::EnumArrayNarrowed, path)),
        (Some(_), None) => out.push(d(Kind::EnumArrayExtended, path)),
        (None, None) => {}
    }
}

fn enum_set(schema: &Value) -> Option<BTreeSet<String>> {
    // support both `enum` array and `const` (treated as single-element enum)
    if let Some(arr) = schema.get("enum").and_then(Value::as_array) {
        Some(arr.iter().map(canonical_value).collect())
    } else {
        schema
            .get("const")
            .map(|c| BTreeSet::from([canonical_value(c)]))
    }
}

// ---------------------------------------------------------------------------
// Numeric bounds
// ---------------------------------------------------------------------------

fn num(s: &Value, k: &str) -> Option<f64> {
    s.get(k).and_then(Value::as_f64)
}

fn compare_numeric(path: &str, orig: &Value, upd: &Value, out: &mut Vec<Difference>) {
    compare_bound(
        path,
        orig,
        upd,
        out,
        "maximum",
        (
            Kind::MaximumAdded,
            Kind::MaximumRemoved,
            Kind::MaximumDecreased,
            Kind::MaximumIncreased,
        ),
    );
    compare_bound(
        path,
        orig,
        upd,
        out,
        "minimum",
        (
            Kind::MinimumAdded,
            Kind::MinimumRemoved,
            Kind::MinimumDecreased,
            Kind::MinimumIncreased,
        ),
    );
    compare_bound(
        path,
        orig,
        upd,
        out,
        "exclusiveMaximum",
        (
            Kind::ExclusiveMaximumAdded,
            Kind::ExclusiveMaximumRemoved,
            Kind::ExclusiveMaximumDecreased,
            Kind::ExclusiveMaximumIncreased,
        ),
    );
    compare_bound(
        path,
        orig,
        upd,
        out,
        "exclusiveMinimum",
        (
            Kind::ExclusiveMinimumAdded,
            Kind::ExclusiveMinimumRemoved,
            Kind::ExclusiveMinimumDecreased,
            Kind::ExclusiveMinimumIncreased,
        ),
    );
    match (num(orig, "multipleOf"), num(upd, "multipleOf")) {
        (None, Some(_)) => out.push(d(Kind::MultipleOfAdded, path)),
        (Some(_), None) => out.push(d(Kind::MultipleOfRemoved, path)),
        (Some(o), Some(u)) if (o - u).abs() > f64::EPSILON => {
            let divisible = |larger: f64, smaller: f64| {
                let quotient = larger / smaller;
                (quotient - quotient.round()).abs() <= f64::EPSILON * quotient.abs().max(1.0)
            };
            out.push(d(
                if divisible(o, u) {
                    Kind::MultipleOfReduced
                } else if divisible(u, o) {
                    Kind::MultipleOfExpanded
                } else {
                    Kind::MultipleOfChanged
                },
                path,
            ));
        }
        _ => {}
    }
}

fn compare_bound(
    path: &str,
    orig: &Value,
    upd: &Value,
    out: &mut Vec<Difference>,
    key: &str,
    kinds: (Kind, Kind, Kind, Kind),
) {
    let (kind_added, kind_removed, kind_decreased, kind_increased) = kinds;
    match (num(orig, key), num(upd, key)) {
        (None, Some(_)) => out.push(d(kind_added, path)),
        (Some(_), None) => out.push(d(kind_removed, path)),
        (Some(o), Some(u)) => {
            if (o - u).abs() > f64::EPSILON {
                if u < o {
                    out.push(d(kind_decreased, path));
                } else {
                    out.push(d(kind_increased, path));
                }
            }
        }
        (None, None) => {}
    }
}

// ---------------------------------------------------------------------------
// String constraints
// ---------------------------------------------------------------------------

fn compare_string_constraints(path: &str, orig: &Value, upd: &Value, out: &mut Vec<Difference>) {
    compare_bound(
        path,
        orig,
        upd,
        out,
        "maxLength",
        (
            Kind::MaxLengthAdded,
            Kind::MaxLengthRemoved,
            Kind::MaxLengthDecreased,
            Kind::MaxLengthIncreased,
        ),
    );
    compare_bound(
        path,
        orig,
        upd,
        out,
        "minLength",
        (
            Kind::MinLengthAdded,
            Kind::MinLengthRemoved,
            Kind::MinLengthDecreased,
            Kind::MinLengthIncreased,
        ),
    );
    // pattern
    let op = orig.get("pattern").and_then(Value::as_str);
    let up = upd.get("pattern").and_then(Value::as_str);
    match (op, up) {
        (None, Some(_)) => out.push(d(Kind::PatternAdded, path)),
        (Some(_), None) => out.push(d(Kind::PatternRemoved, path)),
        (Some(o), Some(u)) if o != u => out.push(d(Kind::PatternChanged, path)),
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Array constraints
// ---------------------------------------------------------------------------

fn compare_array_constraints(
    path: &str,
    orig: &Value,
    upd: &Value,
    ctx: &mut Ctx<'_>,
    out: &mut Vec<Difference>,
) {
    let oi = orig.get("items");
    let ui = upd.get("items");
    match (oi, ui) {
        (Some(oi), Some(ui)) if oi.is_object() && ui.is_object() => {
            compare_schema(&format!("{path}/items"), oi, ui, ctx, out);
        }
        (Some(Value::Array(oi)), Some(Value::Array(ui))) => {
            for (index, (oi, ui)) in oi.iter().zip(ui).enumerate() {
                compare_schema(&format!("{path}/items/{index}"), oi, ui, ctx, out);
            }
            if ui.len() > oi.len() {
                out.push(d(Kind::AdditionalItemsRemoved, &format!("{path}/items")));
            } else if oi.len() > ui.len() {
                out.push(d(Kind::AdditionalItemsAdded, &format!("{path}/items")));
            }
        }
        (Some(_), Some(_)) => out.push(d(Kind::TypeChanged, &format!("{path}/items"))),
        _ => {}
    }

    compare_bound(
        path,
        orig,
        upd,
        out,
        "maxItems",
        (
            Kind::MaxItemsAdded,
            Kind::MaxItemsRemoved,
            Kind::MaxItemsDecreased,
            Kind::MaxItemsIncreased,
        ),
    );
    compare_bound(
        path,
        orig,
        upd,
        out,
        "minItems",
        (
            Kind::MinItemsAdded,
            Kind::MinItemsRemoved,
            Kind::MinItemsDecreased,
            Kind::MinItemsIncreased,
        ),
    );

    // additionalItems: false in update = tighter
    let oa = orig.get("additionalItems");
    let ua = upd.get("additionalItems");
    let o_false = matches!(oa, Some(Value::Bool(false)));
    let u_false = matches!(ua, Some(Value::Bool(false)));
    if !o_false && u_false {
        out.push(d(Kind::AdditionalItemsRemoved, path));
    } else if o_false && !u_false {
        out.push(d(Kind::AdditionalItemsAdded, path));
    } else if oa.is_none() && ua.is_some_and(|value| !value.is_boolean()) {
        out.push(d(Kind::AdditionalItemsNarrowed, path));
    } else if ua.is_none() && oa.is_some_and(|value| !value.is_boolean()) {
        out.push(d(Kind::AdditionalItemsExtended, path));
    } else if let (Some(oa), Some(ua)) = (oa, ua)
        && !oa.is_boolean()
        && !ua.is_boolean()
    {
        compare_schema(&format!("{path}/additionalItems"), oa, ua, ctx, out);
    }

    match (
        orig.get("uniqueItems")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        upd.get("uniqueItems")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    ) {
        (false, true) => out.push(d(Kind::UniqueItemsAdded, path)),
        (true, false) => out.push(d(Kind::UniqueItemsRemoved, path)),
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Object size
// ---------------------------------------------------------------------------

fn compare_object_size(path: &str, orig: &Value, upd: &Value, out: &mut Vec<Difference>) {
    compare_bound(
        path,
        orig,
        upd,
        out,
        "maxProperties",
        (
            Kind::MaxPropertiesAdded,
            Kind::MaxPropertiesRemoved,
            Kind::MaxPropertiesDecreased,
            Kind::MaxPropertiesIncreased,
        ),
    );
    compare_bound(
        path,
        orig,
        upd,
        out,
        "minProperties",
        (
            Kind::MinPropertiesAdded,
            Kind::MinPropertiesRemoved,
            Kind::MinPropertiesDecreased,
            Kind::MinPropertiesIncreased,
        ),
    );
}

// ---------------------------------------------------------------------------
// Combinators: allOf / anyOf / oneOf / not
// ---------------------------------------------------------------------------

fn branches<'a>(schema: &'a Value, keyword: &str) -> Option<&'a [Value]> {
    schema
        .get(keyword)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
}

fn branch_compatible(orig: &Value, upd: &Value, ctx: &Ctx<'_>) -> bool {
    let mut diffs = Vec::new();
    let mut branch_ctx = Ctx::new(ctx.orig_root, ctx.upd_root, ctx.orig_refs, ctx.upd_refs);
    compare_schema("#", orig, upd, &mut branch_ctx, &mut diffs);
    diffs
        .iter()
        .all(|difference| super::compat::is_backward_compatible(&difference.kind))
}

fn maximum_matching(orig: &[Value], upd: &[Value], ctx: &Ctx<'_>) -> usize {
    fn augment(
        index: usize,
        edges: &[Vec<usize>],
        seen: &mut [bool],
        matched: &mut [Option<usize>],
    ) -> bool {
        for &candidate in &edges[index] {
            if seen[candidate] {
                continue;
            }
            seen[candidate] = true;
            if matched[candidate].is_none()
                || augment(matched[candidate].unwrap(), edges, seen, matched)
            {
                matched[candidate] = Some(index);
                return true;
            }
        }
        false
    }

    let edges: Vec<Vec<usize>> = orig
        .iter()
        .map(|old| {
            upd.iter()
                .enumerate()
                .filter_map(|(index, new)| branch_compatible(old, new, ctx).then_some(index))
                .collect()
        })
        .collect();
    let mut matched = vec![None; upd.len()];
    (0..orig.len())
        .filter(|&index| augment(index, &edges, &mut vec![false; upd.len()], &mut matched))
        .count()
}

fn compare_combinators(
    path: &str,
    orig: &Value,
    upd: &Value,
    ctx: &mut Ctx<'_>,
    out: &mut Vec<Difference>,
) {
    match (branches(orig, "allOf"), branches(upd, "allOf")) {
        (Some(old), Some(new)) if old != new => {
            let matched = maximum_matching(old, new, ctx);
            if matched < old.len().min(new.len()) {
                out.push(d(
                    Kind::CombinedTypeSubschemasChanged,
                    &format!("{path}/allOf"),
                ));
            } else if new.len() > old.len() {
                out.push(d(Kind::ProductTypeExtended, &format!("{path}/allOf")));
            } else if new.len() < old.len() {
                out.push(d(Kind::ProductTypeNarrowed, &format!("{path}/allOf")));
            }
        }
        (Some(_), None) | (None, Some(_)) => out.push(d(Kind::CombinedTypeChanged, path)),
        _ => {}
    }

    let orig_sum = branches(orig, "anyOf")
        .map(|value| ("anyOf", value))
        .or_else(|| branches(orig, "oneOf").map(|value| ("oneOf", value)));
    let upd_sum = branches(upd, "anyOf")
        .map(|value| ("anyOf", value))
        .or_else(|| branches(upd, "oneOf").map(|value| ("oneOf", value)));
    match (orig_sum, upd_sum) {
        (Some((old_kind, _)), Some((new_kind, _))) if old_kind != new_kind => {
            out.push(d(
                if new_kind == "anyOf" {
                    Kind::CombinedTypeExtended
                } else {
                    Kind::CombinedTypeChanged
                },
                path,
            ));
        }
        (Some((kind, old)), Some((_, new))) if old != new => {
            let matched = maximum_matching(old, new, ctx);
            if matched < old.len().min(new.len()) {
                out.push(d(
                    Kind::CombinedTypeSubschemasChanged,
                    &format!("{path}/{kind}"),
                ));
            } else if new.len() > old.len() {
                out.push(d(Kind::SumTypeExtended, &format!("{path}/{kind}")));
            } else if new.len() < old.len() {
                out.push(d(Kind::SumTypeNarrowed, &format!("{path}/{kind}")));
            }
        }
        (None, Some((kind, new))) => out.push(d(
            if new
                .iter()
                .any(|branch| branch_compatible(orig, branch, ctx))
            {
                Kind::SumTypeExtended
            } else {
                Kind::CombinedTypeChanged
            },
            &format!("{path}/{kind}"),
        )),
        (Some((kind, old)), None) => out.push(d(
            if old.iter().all(|branch| branch_compatible(branch, upd, ctx)) {
                Kind::SumTypeNarrowed
            } else {
                Kind::CombinedTypeChanged
            },
            &format!("{path}/{kind}"),
        )),
        _ => {}
    }

    match (orig.get("not"), upd.get("not")) {
        (Some(old), Some(new)) if old != new => {
            out.push(d(
                if branch_compatible(new, old, ctx) {
                    Kind::NotTypeNarrowed
                } else {
                    Kind::NotTypeExtended
                },
                &format!("{path}/not"),
            ));
        }
        (Some(_), None) | (None, Some(_)) => out.push(d(Kind::CombinedTypeChanged, path)),
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// $ref resolution
// ---------------------------------------------------------------------------

/// Resolve a `$ref`. An intra-document `#/...` pointer resolves against `root`,
/// which does not change. A non-`#` ref resolves against `refs` if its string
/// matches a registered reference's `name`. Otherwise it stays permissive and
/// gives `None`. Both branches borrow for the same lifetime, so the result is a
/// plain `&Value`.
fn resolve_ref<'a>(schema: &Value, root: &'a Value, refs: &'a RefMap) -> Option<&'a Value> {
    let ref_str = schema.get("$ref").and_then(Value::as_str)?;
    if let Some(ptr) = ref_str.strip_prefix('#') {
        return if ptr.is_empty() {
            Some(root)
        } else {
            root.pointer(ptr)
        };
    }
    // Non-local ref: resolve against the registry ref-map by name, else permissive.
    refs.iter().find(|(n, _)| n == ref_str).map(|(_, v)| v)
}

fn compare_refs(
    path: &str,
    orig: &Value,
    upd: &Value,
    ctx: &mut Ctx<'_>,
    out: &mut Vec<Difference>,
) -> bool {
    let o_ref = orig.get("$ref").and_then(Value::as_str).map(String::from);
    let u_ref = upd.get("$ref").and_then(Value::as_str).map(String::from);

    if let (None, None) = (&o_ref, &u_ref) {
        return false;
    }

    // Build cycle-guard key from the two ref strings (or a sentinel for absent)
    let key = (
        o_ref.clone().unwrap_or_default(),
        u_ref.clone().unwrap_or_default(),
    );
    if ctx.visited.contains(&key) {
        return true; // already walking this pair — cycle, stop
    }
    ctx.visited.insert(key.clone());

    // Resolve each side against its own root + ref-map; an unmatched non-local
    // ref leaves that side permissive (None). Don't cross the streams.
    let o_resolved = o_ref
        .as_deref()
        .and_then(|_| resolve_ref(orig, ctx.orig_root, ctx.orig_refs));
    let u_resolved = u_ref
        .as_deref()
        .and_then(|_| resolve_ref(upd, ctx.upd_root, ctx.upd_refs));

    match (o_resolved, u_resolved) {
        (Some(ores), Some(ures)) => {
            // Both resolve — diff the targets; clone to avoid borrow issues
            let ores = ores.clone();
            let ures = ures.clone();
            compare_schema(&format!("{path}/$ref"), &ores, &ures, ctx, out);
        }
        (Some(ores), None) => {
            // orig had a $ref, update doesn't — diff resolved orig vs update directly
            let ores = ores.clone();
            compare_schema(path, &ores, upd, ctx, out);
        }
        (None, Some(ures)) => {
            // update has a $ref, orig doesn't — diff orig vs resolved update
            let ures = ures.clone();
            compare_schema(path, orig, &ures, ctx, out);
        }
        (None, None) => {
            // neither resolved (remote refs or unresolvable) — treat permissively
        }
    }

    ctx.visited.remove(&key);
    true
}

// ---------------------------------------------------------------------------
// Dependencies
// ---------------------------------------------------------------------------

fn dependency_kind(value: &Value, added: bool) -> Kind {
    if value.is_array() {
        if added {
            Kind::DependencyArrayAdded
        } else {
            Kind::DependencyArrayRemoved
        }
    } else if added {
        Kind::DependencySchemaAdded
    } else {
        Kind::DependencySchemaRemoved
    }
}

fn compare_dependencies(
    path: &str,
    orig: &Value,
    upd: &Value,
    ctx: &mut Ctx<'_>,
    out: &mut Vec<Difference>,
) {
    // cp 7.4 loads draft-07 here; newer dependent* keywords are ignored.
    for keyword in ["dependencies"] {
        let empty = serde_json::Map::new();
        let old = orig
            .get(keyword)
            .and_then(Value::as_object)
            .unwrap_or(&empty);
        let new = upd
            .get(keyword)
            .and_then(Value::as_object)
            .unwrap_or(&empty);
        for (name, value) in old {
            let dependency_path = format!("{path}/{keyword}/{name}");
            let Some(update) = new.get(name) else {
                out.push(d(dependency_kind(value, false), &dependency_path));
                continue;
            };
            match (value.as_array(), update.as_array()) {
                (Some(old), Some(new)) => {
                    let old: BTreeSet<_> = old.iter().filter_map(Value::as_str).collect();
                    let new: BTreeSet<_> = new.iter().filter_map(Value::as_str).collect();
                    if new.is_superset(&old) && new != old {
                        out.push(d(Kind::DependencyArrayExtended, &dependency_path));
                    } else if old.is_superset(&new) && new != old {
                        out.push(d(Kind::DependencyArrayNarrowed, &dependency_path));
                    } else if new != old {
                        out.push(d(Kind::DependencyArrayChanged, &dependency_path));
                    }
                }
                (None, None) if value.is_object() && update.is_object() => {
                    compare_schema(&dependency_path, value, update, ctx, out);
                }
                _ if value != update => out.push(d(Kind::DependencyArrayChanged, &dependency_path)),
                _ => {}
            }
        }
        for (name, value) in new {
            if !old.contains_key(name) {
                out.push(d(
                    dependency_kind(value, true),
                    &format!("{path}/{keyword}/{name}"),
                ));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Conditionals: if / then / else
// ---------------------------------------------------------------------------

fn compare_conditionals(
    path: &str,
    orig: &Value,
    upd: &Value,
    ctx: &mut Ctx<'_>,
    out: &mut Vec<Difference>,
) {
    // Check if any of the conditional keywords are present in either schema
    let has_cond_orig =
        orig.get("if").is_some() || orig.get("then").is_some() || orig.get("else").is_some();
    let has_cond_upd =
        upd.get("if").is_some() || upd.get("then").is_some() || upd.get("else").is_some();

    if !has_cond_orig && !has_cond_upd {
        return;
    }

    // If both sides have the same structure, recurse into the branches; otherwise flag changed.
    for kw in &["if", "then", "else"] {
        let ov = orig.get(kw);
        let uv = upd.get(kw);
        match (ov, uv) {
            (Some(ov), Some(uv)) => {
                let oc = canonical_value(ov);
                let uc = canonical_value(uv);
                if oc != uc {
                    out.push(d(Kind::ConditionalChanged, &format!("{path}/{kw}")));
                    // Also recurse to surface detailed diffs inside the branch
                    let ov = ov.clone();
                    let uv = uv.clone();
                    compare_schema(&format!("{path}/{kw}"), &ov, &uv, ctx, out);
                }
            }
            (None, Some(_)) | (Some(_), None) => {
                out.push(d(Kind::ConditionalChanged, &format!("{path}/{kw}")));
            }
            (None, None) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{Ctx, branch_compatible, compare_with_refs, maximum_matching};

    #[test]
    fn references_resolve_before_diffing_and_cycles_terminate() {
        let referenced = json!({"$ref": "#/$defs/T", "$defs": {"T": {"type": "integer"}}});
        let inline = json!({"type": "integer"});
        assert2::assert!(compare_with_refs(&referenced, &inline, &[], &[]).is_empty());
        assert2::assert!(compare_with_refs(&inline, &referenced, &[], &[]).is_empty());
        let recursive = json!({"$ref": "#"});
        assert2::assert!(compare_with_refs(&recursive, &recursive, &[], &[]).is_empty());
    }

    #[test]
    fn branch_matching_requires_compatible_coverage() {
        let root = json!({});
        let ctx = Ctx::new(&root, &root, &[], &[]);
        let old = vec![json!({"type": "integer"}), json!({"type": "string"})];
        let permuted = vec![json!({"type": "string"}), json!({"type": "number"})];
        let missing = vec![json!({"type": "boolean"}), json!({"type": "number"})];
        assert2::assert!(maximum_matching(&old, &permuted, &ctx) == 2);
        assert2::assert!(maximum_matching(&old, &missing, &ctx) == 1);
        assert2::assert!(branch_compatible(&old[0], &permuted[1], &ctx));
    }
}

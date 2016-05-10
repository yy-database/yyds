//! Catalog publishing surface for the node CLI.

use napi::bindgen_prelude::*;
use napi_derive::napi;
use yyds_catalog::Catalog;

/// Publishes a VOS schema document into a `.yyds` catalog file.
///
/// Creates the file when it is absent and rejects a version or document that
/// conflicts with the already published schema.
#[napi]
pub fn catalog_publish_schema(catalog_path: String, schema_version: u32, schema_document: String) -> Result<()> {
    let mut catalog = Catalog::open(&catalog_path).map_err(|error| Error::from_reason(format!("open catalog: {error}")))?;
    catalog
        .ensure_schema(schema_version, &schema_document)
        .map_err(|error| Error::from_reason(format!("publish schema: {error}")))?;
    catalog.flush().map_err(|error| Error::from_reason(format!("flush catalog: {error}")))?;
    Ok(())
}

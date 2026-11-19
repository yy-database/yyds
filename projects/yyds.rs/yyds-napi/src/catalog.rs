//! Catalog publishing surface for the node CLI.

use napi::bindgen_prelude::*;
use napi_derive::napi;
use yyds::execution::cluster::CatalogSchemaService;

/// Publishes a VOS schema document into a `.yyds` catalog file.
///
/// Delegates to `CatalogSchemaService`, the single official entry for catalog
/// schema changes: lock, validate a candidate copy, atomically publish the
/// file, then swap the in-memory state. A failed change leaves both the file
/// and the memory untouched, so the local entry shares the wire facade's
/// error and publishing semantics.
#[napi]
pub fn catalog_publish_schema(catalog_path: String, schema_version: u32, schema_document: String) -> Result<()> {
    let service =
        CatalogSchemaService::open(&catalog_path).map_err(|error| Error::from_reason(format!("open catalog: {error}")))?;
    service
        .ensure_schema(schema_version, &schema_document)
        .map_err(|error| Error::from_reason(format!("publish schema: {error}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    const VOS: &str = "table User {\n    @@id: uuid,\n}\n";
    const VOS_OTHER: &str = "table Post {\n    @@id: uuid,\n}\n";

    fn scratch_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("yyds-napi-catalog-{}-{name}", std::process::id()));
        fs::create_dir_all(&dir).expect("tempdir");
        dir
    }

    #[test]
    fn conflict_keeps_file_and_memory_untouched() {
        let path = scratch_dir("conflict").join("cluster.yyds");
        let _ = fs::remove_file(&path);
        let path_string = path.to_string_lossy().into_owned();

        catalog_publish_schema(path_string.clone(), 1, VOS.to_owned()).expect("first publish");
        let published = fs::read(&path).expect("read after first");

        let error = catalog_publish_schema(path_string.clone(), 2, VOS.to_owned()).expect_err("version bump must conflict");
        assert!(error.reason.contains("publish schema"), "unexpected error: {}", error.reason);
        let error =
            catalog_publish_schema(path_string.clone(), 1, VOS_OTHER.to_owned()).expect_err("different document must conflict");
        assert!(error.reason.contains("publish schema"), "unexpected error: {}", error.reason);

        assert_eq!(fs::read(&path).expect("read after conflicts"), published, "failed changes must not rewrite the file");
        let service = CatalogSchemaService::open(&path).expect("reopen");
        let schema = service.schema().expect("schema after conflicts");
        assert_eq!((schema.version, schema.document.as_str()), (1, VOS), "failed changes must not swap memory state");

        catalog_publish_schema(path_string, 1, VOS.to_owned()).expect("replay must be a no-op");
        assert_eq!(fs::read(&path).expect("read after replay"), published);
    }

    #[test]
    fn failed_publish_leaves_file_and_memory_untouched() {
        // The parent is an existing regular file, so the atomic publish step
        // fails before any directory entry changes.
        let blocker = scratch_dir("publish-failure").join("blocker");
        let _ = fs::remove_dir_all(&blocker);
        fs::write(&blocker, b"do not touch").expect("seed blocker");
        let path = blocker.join("cluster.yyds");

        let error = catalog_publish_schema(path.to_string_lossy().into_owned(), 1, VOS.to_owned())
            .expect_err("publish into a blocked parent must fail");
        assert!(error.reason.contains("publish schema"), "unexpected error: {}", error.reason);
        assert_eq!(fs::read(&blocker).expect("read blocker"), b"do not touch", "the blocker file must survive untouched");
        assert!(!path.exists(), "a failed publish must not leave a half-published catalog");

        // The same service the wire facade delegates to: a failed publish
        // never swaps the in-memory state.
        let service = CatalogSchemaService::open(&path).expect("open against blocked path");
        assert!(service.ensure_schema(1, VOS).is_err(), "publish failure must surface");
        assert!(service.schema().is_none(), "failed publish must not swap memory state");
    }
}

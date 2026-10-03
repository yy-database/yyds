use std::path::{Path, PathBuf};

use yyds_types::{adopt_catalog_schema, CatalogSchema, Error, Result, ShardId, CATALOG_SUFFIX};

use crate::format;

/// In-memory or file-backed `.yyds` catalog state.
#[derive(Debug, Clone, PartialEq)]
pub struct Catalog {
    path: Option<PathBuf>,
    schema: Option<CatalogSchema>,
    identity: Option<vos::ast::CatalogSnapshot>,
    shards: Vec<ShardId>,
}

impl Catalog {
    /// Creates an empty in-memory catalog.
    pub fn open_memory() -> Self {
        Self {
            path: None,
            schema: None,
            identity: None,
            shards: Vec::new(),
        }
    }

    /// Opens an existing catalog file or starts an empty catalog at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if path.exists() {
            let bytes = std::fs::read(&path)?;
            let (schema, identity, shards) = format::decode(&bytes)?;
            Ok(Self {
                path: Some(path),
                schema,
                identity,
                shards,
            })
        } else {
            Ok(Self {
                path: Some(path),
                schema: None,
                identity: None,
                shards: Vec::new(),
            })
        }
    }

    /// Returns the backing path when this catalog is file-backed.
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// Current catalog schema, if any.
    pub fn schema(&self) -> Option<&CatalogSchema> {
        self.schema.as_ref()
    }

    /// Published VOS identity snapshot used by gateways and distributed planners.
    pub fn identity(&self) -> Option<&vos::ast::CatalogSnapshot> {
        self.identity.as_ref()
    }

    /// Registered shard ids in catalog order.
    pub fn shards(&self) -> &[ShardId] {
        &self.shards
    }

    /// Stores catalog schema truth when empty and rejects mismatched versions thereafter.
    pub fn ensure_schema(&mut self, version: u32, document: &str) -> Result<()> {
        match &self.schema {
            Some(schema) if schema.version != version => Err(Error::SchemaConflict {
                expected: version,
                found: schema.version,
            }),
            Some(schema) if schema.document != document => Err(Error::SchemaConflict {
                expected: version,
                found: schema.version,
            }),
            Some(_) => Ok(()),
            None => {
                self.schema = Some(adopt_catalog_schema(version, document)?);
                let parsed = vos::parser::parse_document(document)
                    .map_err(|diagnostics| Error::Schema { message: diagnostics.to_string() })?;
                self.identity = Some(vos::catalog_from_document(&parsed).map_err(|message| {
                    Error::Schema { message }
                })?);
                Ok(())
            }
        }
    }

    /// Explicitly publishes an identity snapshot for a legacy catalog that lacks one.
    pub fn initialize_identity(&mut self) -> Result<()> {
        if self.identity.is_some() {
            return Ok(());
        }
        let schema = self
            .schema
            .as_ref()
            .ok_or(Error::Unsupported("catalog schema must be published before identity"))?;
        let parsed = vos::parser::parse_document(&schema.document)
            .map_err(|diagnostics| Error::Schema { message: diagnostics.to_string() })?;
        self.identity = Some(
            vos::catalog_from_document(&parsed).map_err(|message| Error::Schema { message })?,
        );
        Ok(())
    }

    /// Registers a shard id exactly once.
    pub fn register_shard(&mut self, shard: ShardId) -> Result<()> {
        if self.shards.iter().any(|existing| existing == &shard) {
            return Err(Error::Unsupported("shard already registered in catalog"));
        }
        self.shards.push(shard);
        Ok(())
    }

    /// Persists the catalog when file-backed.
    pub fn flush(&self) -> Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let bytes = format::encode(self.schema.as_ref(), self.identity.as_ref(), &self.shards)?;
        std::fs::write(path, bytes)?;
        Ok(())
    }
}

/// Returns true when `path` uses the `.yyds` catalog suffix.
pub fn is_catalog_path(path: &Path) -> bool {
    path.extension().is_some_and(|ext| ext == CATALOG_SUFFIX)
}

/// Builds a catalog path with the `.yyds` suffix.
pub fn catalog_path(base: impl AsRef<Path>) -> PathBuf {
    base.as_ref().with_extension(CATALOG_SUFFIX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_path_uses_yyds_suffix() {
        let path = catalog_path("cluster-a");
        assert_eq!(path.extension().and_then(|ext| ext.to_str()), Some(CATALOG_SUFFIX));
    }
}

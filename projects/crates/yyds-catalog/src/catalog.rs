use std::path::{Path, PathBuf};

use yyds_types::{
    adopt_catalog_schema, CatalogSchema, Error, Result, ShardEpoch, ShardId, ShardMap, CATALOG_SUFFIX,
};

use crate::format;

/// In-memory or file-backed `.yyds` catalog state.
#[derive(Debug, Clone, PartialEq)]
pub struct Catalog {
    path: Option<PathBuf>,
    schema: Option<CatalogSchema>,
    identity: Option<vos::ast::CatalogSnapshot>,
    resolved_contract: Option<vos::ResolvedContract>,
    shards: Vec<ShardId>,
    routing_epoch: Option<ShardEpoch>,
}

impl Catalog {
    /// Creates an empty in-memory catalog.
    pub fn open_memory() -> Self {
        Self {
            path: None,
            schema: None,
            identity: None,
            resolved_contract: None,
            shards: Vec::new(),
            routing_epoch: None,
        }
    }

    /// Opens an existing catalog file or starts an empty catalog at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if path.exists() {
            let bytes = std::fs::read(&path)?;
            let (schema, identity, routing_epoch, shards, resolved_contract) = format::decode(&bytes)?;
            Ok(Self {
                path: Some(path),
                schema,
                identity,
                resolved_contract,
                shards,
                routing_epoch,
            })
        } else {
            Ok(Self {
                path: Some(path),
                schema: None,
                identity: None,
                resolved_contract: None,
                shards: Vec::new(),
                routing_epoch: None,
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

    /// Returns the strict resolved VOS contract published for this catalog.
    pub fn resolved_contract(&self) -> Option<&vos::ResolvedContract> {
        self.resolved_contract.as_ref()
    }

    /// Publishes a validated resolved VOS contract for downstream consumers.
    pub fn publish_resolved_contract(&mut self, contract: vos::ResolvedContract) -> Result<()> {
        contract.validate().map_err(|error| Error::Schema {
            message: format!("invalid resolved VOS contract {}: {}", error.code, error.message),
        })?;
        if let Some(current) = &self.resolved_contract {
            if current != &contract {
                return Err(Error::SchemaConflict { expected: 1, found: 2 });
            }
        }
        self.resolved_contract = Some(contract);
        Ok(())
    }

    /// Registered shard ids in catalog order.
    pub fn shards(&self) -> &[ShardId] {
        &self.shards
    }

    /// Returns the currently published routing map, if one exists.
    pub fn routing(&self) -> Result<Option<ShardMap>> {
        self.routing_epoch
            .map(|epoch| ShardMap::new(epoch, self.shards.clone()).map_err(|_| Error::Corrupt("invalid catalog routing map")))
            .transpose()
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
        if self.routing_epoch.is_some() {
            return Err(Error::Unsupported("publish a newer routing epoch to change shard membership"));
        }
        if self.shards.iter().any(|existing| existing == &shard) {
            return Err(Error::Unsupported("shard already registered in catalog"));
        }
        self.shards.push(shard);
        Ok(())
    }

    /// Publishes a newer complete shard map and routing epoch atomically in catalog state.
    pub fn publish_routing(&mut self, epoch: ShardEpoch, shards: Vec<ShardId>) -> Result<()> {
        if let Some(current) = self.routing_epoch {
            if epoch <= current {
                return Err(Error::RoutingConflict { expected: current.0, found: epoch.0 });
            }
        }
        let map = ShardMap::new(epoch, shards).map_err(|_| Error::Unsupported("invalid routing map"))?;
        self.shards = map.shards().to_vec();
        self.routing_epoch = Some(epoch);
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
        let bytes = format::encode(
            self.schema.as_ref(),
            self.identity.as_ref(),
            self.routing_epoch,
            &self.shards,
            self.resolved_contract.as_ref(),
        )?;
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

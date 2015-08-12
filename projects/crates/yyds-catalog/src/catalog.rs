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
            let (schema, identity, routing_epoch, shards, mut resolved_contract) = format::decode(&bytes)?;
            if resolved_contract.is_none() {
                if let (Some(schema), Some(identity)) = (schema.as_ref(), identity.as_ref()) {
                    resolved_contract = Some(resolved_contract_for_schema(schema, Some(identity))?);
                }
            }
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

    /// Legacy identity ledger retained only for migration of older catalog files.
    pub fn identity(&self) -> Option<&vos::ast::CatalogSnapshot> {
        self.identity.as_ref()
    }

    /// Returns the strict resolved VOS contract published for this catalog.
    pub fn resolved_contract(&self) -> Option<&vos::ResolvedContract> {
        self.resolved_contract.as_ref()
    }

    /// Publishes a validated resolved VOS contract for downstream consumers.
    pub fn publish_resolved_contract(&mut self, contract: vos::ResolvedContract) -> Result<()> {
        validate_contract(self.schema.as_ref(), self.identity.as_ref(), &contract)?;
        if let Some(current) = &self.resolved_contract {
            if current != &contract {
                return Err(Error::Unsupported("resolved contract evolution requires an explicit identity migration"));
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
            Some(_) => {
                if self.resolved_contract.is_none() {
                    self.resolved_contract = Some(resolved_contract_for_schema(
                        self.schema.as_ref().expect("schema exists"),
                        self.identity.as_ref(),
                    )?);
                }
                Ok(())
            }
            None => {
                let schema = adopt_catalog_schema(version, document)?;
                let contract = resolved_contract_for_schema(&schema, None)?;
                self.schema = Some(schema);
                self.resolved_contract = Some(contract);
                Ok(())
            }
        }
    }

    /// Initializes a resolved contract for a schema-only legacy catalog.
    pub fn initialize_identity(&mut self) -> Result<()> {
        if self.resolved_contract.is_some() {
            return Ok(());
        }
        let schema = self
            .schema
            .as_ref()
            .ok_or(Error::Unsupported("catalog schema must be published before identity"))?;
        self.resolved_contract = Some(resolved_contract_for_schema(schema, self.identity.as_ref())?);
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

fn resolved_contract_for_schema(
    schema: &CatalogSchema,
    identity: Option<&vos::ast::CatalogSnapshot>,
) -> Result<vos::ResolvedContract> {
    let projection = vos::parse_oak(&schema.document)
        .map_err(|message| Error::Schema { message })?
        .project_schema()
        .map_err(|diagnostics| Error::Schema {
            message: format!("VOS semantic projection failed: {diagnostics:?}"),
        })?;
    let mut used_type_ids = std::collections::BTreeSet::new();
    let mut used_field_ids = std::collections::BTreeSet::new();
    if let Some(identity) = identity {
        used_type_ids.extend(identity.types.iter().map(|entry| entry.type_id.0));
        used_field_ids.extend(
            identity
                .types
                .iter()
                .flat_map(|entry| entry.fields.iter().map(|field| field.field_id.0)),
        );
    }
    let mut next_type_id = 1u64;
    let mut next_field_id = 1u64;
    let types = projection
        .types
        .iter()
        .map(|projected| {
            let name = projected
                .canonical_path
                .last()
                .cloned()
                .ok_or(Error::Corrupt("VOS type has an empty canonical path"))?;
            let legacy = identity.and_then(|catalog| {
                catalog.types.iter().find(|entry| {
                    entry.name == name
                        && matches!(
                            (entry.kind, projected.kind),
                            (vos::ast::TypeKind::Table, vos::contract::TypeContractKind::Table)
                                | (vos::ast::TypeKind::Class, vos::contract::TypeContractKind::Class)
                        )
                })
            });
            if identity.is_some() && legacy.is_none() {
                return Err(Error::Corrupt("legacy catalog is missing a schema type"));
            }
            let type_id = if let Some(entry) = legacy {
                entry.type_id.0
            } else {
                while used_type_ids.contains(&next_type_id) {
                    next_type_id += 1;
                }
                let id = next_type_id;
                used_type_ids.insert(id);
                next_type_id += 1;
                id
            };
            let fields = projected
                .fields
                .iter()
                .enumerate()
                .map(|(field_index, field)| {
                    let legacy_field = legacy.and_then(|entry| {
                        entry.fields.iter().find(|candidate| {
                            candidate.current_name == field.canonical_name
                        })
                    });
                    if legacy.is_some() && legacy_field.is_none() {
                        return Err(Error::Corrupt("legacy catalog is missing a schema field"));
                    }
                    let field_id = if let Some(field) = legacy_field {
                        field.field_id.0
                    } else {
                        while used_field_ids.contains(&next_field_id) {
                            next_field_id += 1;
                        }
                        let id = next_field_id;
                        used_field_ids.insert(id);
                        next_field_id += 1;
                        id
                    };
                    let virtual_field_index = legacy_field
                        .map(|field| field.virtual_field)
                        .unwrap_or(field_index as u32);
                    Ok(vos::contract::FieldIdentity {
                        canonical_name: field.canonical_name.clone(),
                        field_id,
                        virtual_field_index,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(vos::contract::TypeIdentity {
                canonical_path: projected.canonical_path.clone(),
                type_id,
                kind: projected.kind,
                fields,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let manifest = vos::contract::IdentityManifest {
        format_version: vos::contract::IDENTITY_MANIFEST_VERSION.to_owned(),
        types,
    };
    let contract = vos::resolve_contract(&projection, &manifest).map_err(|diagnostics| Error::Schema {
        message: format!("VOS resolved contract failed: {diagnostics:?}"),
    })?;
    validate_contract(Some(schema), identity, &contract)?;
    Ok(contract)
}

pub(crate) fn validate_contract(
    schema: Option<&CatalogSchema>,
    identity: Option<&vos::ast::CatalogSnapshot>,
    contract: &vos::ResolvedContract,
) -> Result<()> {
    let artifact = contract.to_json().map_err(|_| Error::Corrupt("resolved contract encode"))?;
    vos::ResolvedContract::from_json(&artifact).map_err(|error| Error::Schema {
        message: format!("invalid resolved VOS contract {}: {}", error.code, error.message),
    })?;
    let schema = schema.ok_or(Error::Unsupported("publish schema before resolved contract"))?;
    let projection = vos::validate_schema(&schema.document).map_err(|message| Error::Schema { message })?;
    let manifest = vos::contract::IdentityManifest {
        format_version: contract.identity_manifest_version.clone(),
        types: contract.types.iter().map(|entry| vos::contract::TypeIdentity {
            canonical_path: entry.canonical_path.clone(),
            type_id: entry.type_id,
            kind: entry.kind,
            fields: entry.fields.iter().map(|field| vos::contract::FieldIdentity {
                canonical_name: field.canonical_name.clone(),
                field_id: field.field_id,
                virtual_field_index: field.virtual_field_index,
            }).collect(),
        }).collect(),
    };
    let expected = vos::resolve_contract(&projection, &manifest).map_err(|diagnostics| Error::Schema {
        message: format!("resolved contract does not match schema: {diagnostics:?}"),
    })?;
    if &expected != contract {
        return Err(Error::Corrupt("resolved contract does not match schema"));
    }
    if let Some(identity) = identity {
        let mut type_ids = std::collections::BTreeSet::new();
        let mut field_ids = std::collections::BTreeSet::new();
        for entry in &identity.types {
            if entry.type_id.0 == 0 || !type_ids.insert(entry.type_id.0) {
                return Err(Error::Corrupt("invalid legacy type identity"));
            }
            for field in &entry.fields {
                if field.field_id.0 == 0 || !field_ids.insert(field.field_id.0) {
                    return Err(Error::Corrupt("invalid legacy field identity"));
                }
            }
        }
        if identity.types.len() != contract.types.len() {
            return Err(Error::Corrupt("resolved contract does not match identity ledger"));
        }
        for entry in &identity.types {
            let resolved = contract.types.iter().find(|item| item.type_id == entry.type_id.0)
                .ok_or(Error::Corrupt("resolved contract type identity mismatch"))?;
            let kind = match entry.kind {
                vos::ast::TypeKind::Table => vos::contract::TypeContractKind::Table,
                vos::ast::TypeKind::Class => vos::contract::TypeContractKind::Class,
            };
            if resolved.canonical_path.last() != Some(&entry.name) || resolved.kind != kind
                || resolved.fields.len() != entry.fields.len() {
                return Err(Error::Corrupt("resolved contract type does not match identity ledger"));
            }
            for field in &entry.fields {
                if !resolved.fields.iter().any(|item| item.field_id == field.field_id.0
                    && item.canonical_name == field.current_name
                    && item.virtual_field_index == field.virtual_field) {
                    return Err(Error::Corrupt("resolved contract field identity mismatch"));
                }
            }
        }
    }
    Ok(())
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

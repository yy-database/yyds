# Maintenance

Maintainer-only. Not part of the public product docs surface. Do not link internal design or decision records from here.

| Path | Purpose |
|------|---------|
| `author-github.json` | Nifty commit author → GitHub login map |
| `releases/vX.Y.Z.md` | **Published** GitHub Release bodies (committed; edit here then sync to GH) |

Local draft index only (gitignored): `releases/vX.Y.Z.reference.md` via `nifty change-logs --version X.Y.Z --write`.

## Release notes flow

```bash
nifty change-logs --version X.Y.Z --write
# hand-edit into documentation/maintenance/releases/vX.Y.Z.md
# Title is the tag only (`vX.Y.Z`). Body: short prose summary + Features / Bug Fixes / Breaking / Contributors. No emoji.
gh release create vX.Y.Z --title vX.Y.Z --notes-file documentation/maintenance/releases/vX.Y.Z.md
# or update an existing release:
gh release edit vX.Y.Z --title vX.Y.Z --notes-file documentation/maintenance/releases/vX.Y.Z.md
```

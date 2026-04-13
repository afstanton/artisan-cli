# artisan-cli

`artisan-cli` is the operator-facing entry point for importing format data,
writing generic `artisan-core` catalogs, reviewing reconciliation candidates,
and eventually converting content across ecosystems.

## Conversion And Reconciliation Rule

Conversions are only trustworthy when they run through reconciled canonical
records, not when they simply parse one format and immediately emit another.

That means `artisan-cli` should be developed with these assumptions:

- `EntityType` reconciliation must persist, including cross-format
  `ExternalId` coverage when two format-specific types are truly the same
  canonical type.
- `Entity` reconciliation must also persist. Conversion cannot be stable if we
  only know that "spell" maps across formats but do not know whether a
  specific spell record does.
- `mapping_records` are required for non-identity cases, especially when one
  format is broader or narrower than another.

## Finest-Grain Canonical Model

The canonical catalog written and consumed by `artisan-cli` must exist at the
finest semantic grain present in any supported format.

This is important because:

- if one format distinguishes two concepts and another collapses them, the
  canonical layer must still preserve the distinction
- coarse formats may import into a partial or collapsed view of the canonical
  graph
- exporting from a coarse format into a finer one is then a projection-choice
  problem over existing canonical records, not a reason to erase the finer
  distinction

In practice, when adding or updating commands:

- prefer preserving hints from imports rather than collapsing them away
- treat ambiguous reverse projection as a reconciliation or mapping problem,
  not as permission to guess silently
- allow multiple canonical records to project to one external record when a
  target format is coarser
- expect reverse projection to rely on persisted hints, mapping records, or
  explicit review decisions

## Directional Mappings

Mappings are not always symmetric.

`many -> one` projection is usually straightforward. `one -> many` projection
often requires:

- source or game-system hints
- preserved subtype or classification fields
- prior reconciliation decisions
- explicit loss notes or ambiguity review when the choice is not safe

`artisan-cli` should therefore treat conversion as:

1. import into canonical `artisan-core`
2. reconcile `EntityType` and `Entity` identities
3. apply directional mapping records where identity is insufficient
4. project to the target format with explicit handling for ambiguity/loss

## Local Corpus Manifests

Curated overlap corpora should live under
`code/rust/apps/artisan-cli/local/`, which is ignored by git.

This gives us a place to keep concrete file/path lists for local
reconciliation work without checking those paths into the repository.

For example, a local Pathfinder 1e overlap manifest can drive both
imports and review generation from the same curated slice.

## Example PF1 Workflow

Import a curated Pathfinder 1e PCGen slice:

```bash
cargo run -p artisan-cli -- import-pcgen \
  --corpus-manifest code/rust/apps/artisan-cli/local/pf1e_reconciliation_corpus.toml \
  --corpus-group "Monster Focus Skeletons"
```

Import the matching HeroLab slice:

```bash
cargo run -p artisan-cli -- import-herolab \
  --corpus-manifest code/rust/apps/artisan-cli/local/pf1e_reconciliation_corpus.toml \
  --corpus-group "Monster Focus Skeletons"
```

Generate or refresh reconciliation review state from the same curated
PCGen corpus:

```bash
cargo run -p artisan-cli -- reconcile-review \
  --corpus-manifest code/rust/apps/artisan-cli/local/pf1e_reconciliation_corpus.toml \
  --corpus-group "Monster Focus Skeletons" \
  --from-core-toml /tmp/pf1_core.toml \
  --state-file /tmp/pf1_review.json
```

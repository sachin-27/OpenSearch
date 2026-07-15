# Nested Documents: Row-ID / Child-Identity Analysis

**Status:** Working notes for design closure (HLD task #2: "Lucene–Parquet DocID/RowID Correspondence — Approach Closure")
**Companion to:** [Nested Document Support — High Level Design](https://quip-amazon.com/EYVuAS2rZR5K/Nested-Document-Support-High-Level-Design)
**Branches referenced:**
- `nested-docs-ingestion-poc` (this repo — **now implements Scheme C**, converted from the original Scheme B; see §8a)
- `AnshMohta-cloud:anmohta-injest` ("root-only row-id" implementation)

> **CONVERSION NOTE (branch state):** this branch originally implemented Scheme B
> (stored packed keys). It has since been **converted to Scheme C** — sequential
> docId-space row ids on every physical doc (I1 preserved literally), block structure
> recovered from `_nested_path` at merge time, per HLD §5.4 and Task 7. Sections 2–7
> below are the original comparative analysis and remain accurate as analysis; §8a
> records what the conversion changed. `BlockRowIdCodec` no longer exists in the
> branch (the merge computes final positions directly, making even transient packed
> keys unnecessary — see §8a).

---

## 1. Problem statement (one paragraph)

Mustang's cross-format contract assumes one logical document = one Lucene docId = one
Parquet row, tied by a shared `__row_id__`. The `nested` field type breaks this: one
logical document becomes N+1 Lucene docs (a contiguous block, parent last) but stays
one Parquet row (children folded into a `LIST<STRUCT>` column, per approach N1). Every
child therefore needs an answer to "which logical document do you belong to, and which
position within it?" — and the open design question is **where that answer lives**:
stored on the document, or derived at read time from segment geometry (parent/path
bitsets).

---

## 2. The three candidate schemes

Running example — logical doc 0 (`{title, comments:[alice, dave]}`) and childless
logical doc 1, as one segment:

```
                     A: root-only          B: stored packed keys       C: sequential + derive
                     (Ansh's branch)       (our branch)                (HLD §5.4 / authors' intent)
docId | doc    |     __row_id__            __row_id__                  __row_id__
------+--------+---------------------------------------------------------------------------
  0   | alice  |     (absent)              encodeChild(0,0) = 0        0   (= docId)
  1   | dave   |     (absent)              encodeChild(0,1) = 1        1   (= docId)
  2   | root0  |     0  (= Parquet row)    encodeParent(0)  = 4194303  2   (= docId)
  3   | root1  |     1  (= Parquet row)    encodeParent(1)  = 8388607  3   (= docId)
```

- **A — root-only logical row-id.** Children carry only `_nested_path`. Block
  integrity via `setParentField`. All child identity computed at runtime.
- **B — stored packed keys.** Every doc carries `(logicalRow << 22) | ordinal`;
  parent gets a reserved sentinel ordinal (all ordinal bits set), making it the
  block's highest key. Natural long ordering of keys == block-join layout, so the
  existing `SortedNumericSortField(__row_id__)` index sort reconstructs blocks through
  every merge. Implemented as `BlockRowIdCodec` (22 ordinal bits ≈ 4.19M children/doc;
  41 row bits; sign bit clear).
- **C — sequential row-ids + derivation.** Every doc keeps `__row_id__ == docId`
  (invariant I1 literally preserved). Nothing in Lucene equals the Parquet row;
  translation is always derived (Parquet row = rank of parent in the parent bitset;
  positional paths via `_nested_path` bitsets, HLD §4.1.1). Merge synthesizes packed
  sort keys *transiently*, then rewrites sequential.

**Key observation:** even the derivational scheme (C) stamps a stored value on every
child (its sequential row-id). "Store nothing on children" (A) appears in **no**
section of the HLD. The real debate is *which* value children carry, not *whether*.

---

## 3. What the HLD actually says (the internal contradiction)

The HLD is not self-consistent on this question — three passages, three positions:

| Passage | Says | Scheme |
|---|---|---|
| §4.1.1 "Proposed Approach for ID mapping" | "docId↔rowId **derived** via the root bitset + per-level `_nested_path` bitsets. **No new columns**." Worked rank/select examples both directions. | Derivational |
| §5.4 "How we solve it" (merge) | "**every physical doc, parent and child, keeps a sequential row-id**, preserving I1"; synthetic packed sort keys **derived** at merge (parent's new logical position high-order + intra-block index low-order). | C exactly |
| §4.1.2.1 (merge flow changes) | "decode `oldParentLogical`/`intraIndex` **from the old composite key**"; invariant relaxes to "parent logicalRow **high-bits** are 0..P-1". | Stored packed keys (B) |

Corroborating evidence for authorial intent:
- Slack (doc author): *"This invariant [docId == row-id] is maintained only in Lucene.
  In parquet, docId will be per parent document"* and *"we will have to get row id from
  child doc id for each query"* → matches §5.4 / Scheme C.
- Doc comment thread: Parquet always returns **logical** rowIds (parent-level);
  *"at Lucene side we will expand it to include the child docIds"* → establishes the
  **currency contract**: cross-format wire = logical rows; expansion is Lucene's job
  at the boundary. (Compatible with all three schemes; constrains the interface, not
  the storage.)
- 07/10 meeting: encoding explicitly left **open** ("explore ordinal-based encoding
  or doc values approach" — action item, not decision).

**Conclusion on intent:** the authors' center of gravity is **Scheme C**. §4.1.2.1's
stored-key language is most plausibly a drafting inconsistency (merge mechanics worked
out in packed-key arithmetic mid-draft; §5.4 is the later, cleaner statement making
the keys transient). Our branch (B) faithfully implements the outlier passage; it
should be presented as *implementation findings challenging the mainline reading*,
not as "what the doc says."

**Important:** Ansh's branch (A) implements **none** of the three passages — children
keep nothing (violates §5.4's "every physical doc"), parents keep *logical* rows
(violates "sequential row-id ... preserving I1" and the Slack statement "invariant
maintained in Lucene"). The fact that his diff had to **delete** `assertRowIdsSequential`
is machine-checkable evidence: under real Scheme C that assert passes unchanged.

---

## 4. The hard floor: updates and deletes

This is the constraint that eliminates Scheme A and is independent of performance.

### 4.1 Deletes

`DELETE /blogs/_doc/7` must flip liveDocs on the **whole block** (invariant I5).
Lucene deletes are *descriptions* (Term or Query), re-executed at apply time — they
must re-find the right docs after merges, on replicas, during translog replay
(docIds are not stable identities across any of those). Under Scheme A there is no
value the children carry that isolates them:
- `_id = "7"` → matches root only (his `nestedContext` change removed vanilla's
  `_id`-on-children).
- `__row_id__` → children have none.
- `_nested_path` → matches every comment of every document.

Result: **orphaned children** — alive forever, matching nested queries, walking
block-joins into the *next unrelated parent*, while Parquet tombstoned the whole row.

*Partial escape:* deletes alone are implementable without stored child values via
`deleteDocuments(ToChildBlockJoinQuery(term(_id,7), parentsFilter))` — the block walk
re-derives at apply time, so it is merge/replay-safe. Custom machinery on a critical
path, but workable.

*Rollback special case:* the cross-format rollback (`LuceneWriter.rollbackTo`) can be
fixed without stored identity because it always targets the **last block** of a
**private, append-only, unmerged** writer — known docId range, `tryDeleteDocument` or
a docId-range query works there. His current code does neither → rolled-back nested
docs orphan children *today*.

### 4.2 Updates — the unfixable part

`_update` / reindex-over-`_id` = delete old block + add new block, **atomically**
(no refresh may observe the in-between). Lucene's *only* atomic delete-and-add is:

```java
IndexWriter.updateDocuments(Term delTerm, Iterable<Document> newDocs)
```

It takes a **Term**. There is no Query variant — the atomicity machinery only works
for terms, and a term delete only kills docs that **carry the term**. This is exactly
why vanilla OpenSearch stamps the parent's `_id` on every nested child (a
*block-membership tag* — same value across the block; it is NOT per-child identity
and does not settle the ordinal debate). Under Scheme A, atomic replace is
**unbuildable**; the alternatives are a non-atomic delete+add (visibility windows,
hand-rolled crash-recovery story) or adding a term back to children — which is no
longer Scheme A.

**Consequences:**
- Scheme A is not a candidate. The minimum viable form of the derivational position
  is **C + child `_id` tag** (which is just vanilla behavior restored).
- Child `_id` restoration is scheme-neutral and should be uncontroversial: B wants it
  as belt-and-suspenders; C needs it for atomic updates.

---

## 5. Case matrix (derivational scheme, with mandatory repairs)

Verdicts for "C + child `_id`" — i.e., the strongest form of the derivational camp:

| # | Case | Verdict |
|---|---|---|
| 1 | Ingest (flat / nested / multi-level / sibling arrays) | ✅ demonstrated (both branches) |
| 2 | Flush / sort / addIndexes block integrity | ⚠️ plausible via `setParentField`; unverified |
| 3 | Cross-format rollback | 🔧 needs docId-range patch |
| 4 | Parent-level update/delete | ✅ with child `_id` (mandatory repair) |
| 5 | Merge, happy path | ⚠️ plausible (transient key synthesis); unbuilt |
| 6 | Merge after whole-block deletes | ⚠️ dependent on #4 |
| 7 | Search collector (docId → logical row) | ⚠️ running rank over parent bitset; unbuilt |
| 8 | Fetch by row-ids (random access) | ⚠️ select() — no O(1) select on Lucene bitsets; cost unknown (open benchmark) |
| 9 | Child addressing (inner_hits, overlay key) | ⚠️ §4.1.1 rank/select + sub-range narrowing — hardest math; unbuilt |
| 10 | Child-only deletes + overlay | ☠️ **the trap**: derivation must count *deleted* children until merge compacts both formats, or indices silently misalign with Parquet |
| 11 | Multi-level nesting | ⚠️ feasible; every consumer must re-derive identically |
| 12 | Empty arrays / single-object nested | ✅ trivially |
| 13 | Recovery / replicas | ✅ by architecture (replay is per logical doc) |
| 14 | Cross-format verification (I2 audits) | ❌ **structural**: derivation can only be checked by re-running the derivation |

Under B (stored keys), rows 1–5 are demonstrated with verified invariants
(`assertBlockRowIds`); the remaining rows consume the same tested decode primitive.

---

## 6. Tradeoffs: derive (C) vs store (B)

| Dimension | Derive (C + `_id`) | Store (B) |
|---|---|---|
| Write cost / storage | ~tie — both stamp one doc-value per child; both compress to ~nothing | ~tie |
| Read cost | rank/select per translation; ordered scans cheap (running rank), random access pays select each time | shift+mask on a value already being read; O(1), uniform |
| Complexity location | spread across every consumer (collector, fetch, overlay, merge) — each a correctness surface | concentrated in one codec class |
| Correctness model | **emergent** — recomputed from mutable physical state | **declared** — identity is data |
| Verifiability | ~none (only re-derivation checks a derivation) | high — independent invariants fail loudly at flush/merge |
| Killer edge case | pending child deletes (must count dead docs) — silent when wrong | doesn't exist |
| Merge | synthesize keys transiently + sequential rewrite (existing machinery survives) | decode→remap→re-encode (§4.1.2.1 as written; sort key == block layout) |
| Ecosystem churn | none — `__row_id__` stays a plain number; existing invariants hold | field semantics fork on nested indices; all consumers (all in-repo, all touched for nested anyway) need codec |
| Reversibility | cheap to upgrade to B later | expensive to downgrade (on-disk migration) |
| Debuggability | a doc tells you nothing; identity reconstructed by hand | dump value, decode, know row/position/role |

**Scheme A's residual (vs both):** cheapest ingest and zero-translation parent reads —
purchased by unaddressable children (fatal for updates), zero verifiability, and no
design-doc cover.

---

## 7. Review of the `anmohta-injest` branch

**Genuinely valuable:** working `VSRManager` `LIST<STRUCT>` write (offsets, recursion,
`reconcileSchema` child preservation); `setParentField` on ingest writer **and**
committer; four real integration blockers discovered (nestedContext `_id` assert,
`canDeriveSource`, two cross-format row-count checks). Unblocks DataFusion-CLI-on-files
experimentation (plan shapes; don't trust result values for multi-valued child fields).

**Defects (wrong behavior on existing paths):**
1. Rollback orphans children (purge by `row_id` hits root only).
2. Sorted-flush path unguarded — sequential row-id rewrite would scramble roots-only
   scheme silently (`mapping.size() <= docCount` relaxation lets it run).
3. Multi-valued fields inside children silently overwritten (`setLeafValue` by
   elemIndex, dedup removed).
4. `canDeriveSource` bypass → derived `_source` fabricates wrong docs for nested.
5. Capability filter bypassed for nested fields (routing precedes the check).
6. `setLeafValue` `value.toString()` on `byte[]` writes garbage.

**Structural:** no child identity (see §4); **all I2 verification removed** —
sequential assert deleted, `verifyPerSegmentCrossFormatRowCountParity` downgraded to a
warning, `getDocCountOfCommit` takes min across formats. The branch cannot detect
cross-format divergence, including divergence caused by its own defects.

**Search unblocking is partial:** engine-path search (delegation collector,
§4.1.2.2) is unimplemented under *every* branch — testing composite search against any
current nested segments produces silently misaligned results (bitset indices are
docId-based; Parquet expects logical rows).

---

## 8. State of our branch (`nested-docs-ingestion-poc`)

Implemented (all unit-tested, plain-mode regression green):
1. `FeatureFlags` — `pluggable.dataformat.nested.enabled` (off by default).
2. `ObjectMapper` — nested gate now flag-controlled (fail-fast default preserved).
3. `NestedScope` / `NestedScopeTracker` (server, shared) — parse-time child identity;
   single ordinal authority; positional paths (`comments[0].replies[1]`).
4. `DocumentInput.beginChild/endChild` (default no-ops) + `DocumentParser` signals at
   vanilla's nested hook points (null-guarded; vanilla untouched) +
   `CompositeDocumentInput` broadcast.
5. `FieldValuePair` scope qualifier; `ParquetDocumentInput` per-scope dedup (two-comment
   crash fixed); `VSRManager` explicit `TODO(nested)` skip for scoped pairs.
6. `BlockRowIdCodec` — the packed-key layout (contract C2 candidate).
7. `LuceneDocumentInput` — builds blocks (children in endChild/post-order, root last,
   `_nested_path` stamped) with composite-key stamping; plain mode byte-identical.
8. `LuceneWriter` — block routing via `addDocuments`, two-space accounting
   (`logicalRows` vs `docCount`), block-range rollback purge, sorted-flush rejection
   for composite mode, `assertBlockRowIds` invariant.
9. `LuceneIndexingExecutionEngine` — per-index key-scheme decision via
   `mapperService.hasNested()` (mid-generation mapping-update freeze = known TODO).
10. `ArrowSchemaBuilder` — recursive `LIST<STRUCT>` schema from nested mappings.
11. `NestedVectorWriter` + `VSRManager` integration — scope-labeled pairs written into
    `LIST<STRUCT>` vectors; element positions == parse-time scope ordinals (identity
    contract); fieldless children keep their positions; absent nested = null list;
    `byte[]`/`BytesRef` text handled as UTF-8 (fixes the `toString()` bug);
    `reconcileSchema` preserves struct children (Ansh's fix adopted). Verified through
    the native Rust writer: `LIST<STRUCT>` round-trips to a real Parquet file
    (note: `RustBridge.readAsJson` cannot render list values — file-level structure
    verified there; element values verified at the vector layer).

12. Composite E2E wiring verified (`CompositeNestedBroadcastTests`): one broadcast
    script → identical child identity in both formats' buffers and on disk; post-parse
    metadata lands on the root scope by construction (no code change needed).
13. Refresh/addIndexes block survival: `setParentField(__nested_parent)` on the ingest
    writer (composite mode) and the secondary committer; `numRows` semantics fixed to
    LOGICAL rows in every format (behavior-neutral for flat mode where logical ==
    physical; required for the engine's cross-format parity assertions —
    `assertRetiredSegmentInvariants` compares formats' numRows for equality); flush now
    stamps the row-id IndexSort declaration on composite segments too (they are sorted
    by construction — keys ascend in insertion order), which the test proved is
    REQUIRED: the sorted committer rejects undeclared segments at addIndexes.
    Verified end-to-end: two nested generations → addIndexes into a committer-shaped
    sorted writer → forceMerge → block layout intact in global logical-row order.
14. Merge expansion (decode→remap→re-encode, §4.1.2.1 arithmetic): segments flushed in
    composite mode are marked with a persisted `composite_row_ids` segment attribute
    (stamped in `LuceneWriter.flush` before the sort rewrite persists it to the `.si`;
    FieldInfos-based parent-field detection was insufficient — childless composite
    segments have no blocks). `RowIdRemappingOneMerge` reads the attribute off the
    source segments (throwing on mixed composite/plain sources — the schemes are not
    mutually sortable) and stamps it onto the merged segment so subsequent merges
    decode correctly. `RowIdRemappingDocValuesProducer` expands per-key: decode the
    old composite key, remap only the logical-row component through the primary's
    logical-space `RowIdMapping`, re-encode with the ordinal (or parent sentinel)
    untouched — every doc of a block lands on the same new logical row, so the
    existing index sort lays merged blocks out contiguously with zero block-size
    recovery. `LuceneMerger.buildMergedFileSet` reports LOGICAL rows for composite
    merges (`mapping.size()`, one entry per surviving logical row) instead of maxDoc,
    keeping cross-format numRows parity. Verified end-to-end in
    `testMergeExpandsCompositeKeysAndPreservesBlocks`: two composite generations
    (blocks + a childless parent) merged with an interleaving logical mapping →
    merged layout `[c(0,0), c(0,1), p(0), c(1,0), p(1), p(2)]` with stored fields
    following, attribute inherited, numRows == 3 logical rows.

15. Golden segment+parquet file pairs (`CompositeNestedGoldenFilesTests`): a
    deterministic 3-row nested dataset (two-comment doc, flat doc, one-comment doc)
    written through the real composite broadcast and both real writers — Lucene
    segment with composite block keys + `segments_N` (opens standalone with a plain
    `DirectoryReader`) and a native-Rust-written Parquet file with the `LIST<STRUCT>`
    column. The test verifies the cross-format contract on the artifacts (3 Parquet
    rows == 3 logical rows over 6 physical docs; per-author decoded
    (logicalRow, ordinal) == Parquet (row index, list position); flat row has a NULL
    list). Export for the query-side POC: run with `-Dtests.leaveTemporary=true` and
    copy the pair from the retained test temp dir (the test security policy forbids
    writing outside the sandbox, so there is no copy-to-arbitrary-path option).
    Enabling this at the composite level required wiring the native Rust bridge and
    Arrow test deps into composite-engine's unit `test` task (previously only its
    `internalClusterTest` had them).

Remaining: search handoff contract with the query-side POC (docId→logicalRow in
`LuceneFilterDelegationHandle` — belongs to the query-side POC, consuming the golden
pair above).

---

## 8a. Scheme B → Scheme C conversion (branch state after design pivot)

The branch was converted from stored packed keys (B) to the HLD §5.4 derivational
scheme (C): **every physical doc, parent and child, keeps a plain sequential
docId-space `__row_id__`** (invariant I1 holds literally in every segment, including
merged ones). All tests green after conversion.

What changed:

1. **`BlockRowIdCodec` deleted** (server). Nothing stores packed keys anymore — and
   the merge doesn't even need them transiently (see #5).
2. **`LuceneDocumentInput`** — nested mode (`nestedBlocks`, renamed from
   `compositeRowIds`) no longer stamps row ids at all; it records the logical rowId
   for validation. Plain (flat-index) mode is byte-identical to before.
3. **`LuceneWriter`** — stamps sequential ids itself in `addDoc` (`docCount + i` per
   block doc; only the writer knows the global offset). Rollback purges the failed
   block by a plain id range `[docCount − lastBlockDocCount, docCount)`. Flush asserts
   `assertRowIdsSequential` (again universal) plus a new nested cross-check
   `assertNestedBlockStructure`: parent count (complement of `_nested_path` docs)
   == `logicalRows` == Parquet's row count. That count check is the strongest
   flush-time invariant available under derivation — per-child correspondence is
   positional and only re-derivable (the verifiability cost of C, accepted knowingly).
   Segment attribute renamed `composite_row_ids` → `nested_blocks`; it now means
   "may contain multi-doc blocks; merge must expand the mapping block-aware."
4. **Mapping-update freeze problem dissolved** — B's worst unsolved edge. Sequential
   ids are the same scheme whether or not blocks exist, so flat and nested segments
   are mutually sortable; the merge's mixed-sources throw is gone (a flat segment
   scans as all blocks-of-one), and an index gaining its first nested field mid-life
   is a non-event for row ids.
5. **Merge: `NestedBlockExpansion`** (new) replaces decode→remap→re-encode. Per source
   segment (eager scan in `wrapForMerge`): recover block structure from
   `_nested_path` complement → per-doc (oldLogical, intra); remap oldLogical through
   the primary's logical mapping. Globally (lazy finalize on first value read, after
   all merge readers are wrapped): prefix-sum block sizes over new logical rows →
   block start offsets. Each doc's produced value is `blockStart[newLogical] + intra`
   — its **final merged position**, which is simultaneously the correct index-sort key
   (blocks land contiguous, children first, parent last) and the correct stored value
   (sequential 0..maxDoc−1, I1 restored). This collapses HLD §5.4's "synthetic packed
   sort keys + sequential rewrite" two-step into one number — no transient packed keys
   and no post-merge rewrite pass needed. The producer also asserts I1 on source
   segments (stored id == docId) as it reads. Flat-only merges keep the legacy
   one-row-id-per-doc path untouched (no flat regression).
6. **Tests converted** (~15 across the three plugins + merger/golden/broadcast):
   expectations moved from packed-key values to sequential ids + positional identity;
   `BlockRowIdCodecTests` deleted. Golden file pair regenerates under C
   (`CompositeNestedGoldenFilesTests` — layout `[c,c,p,p,c,p]`, ids `0..5`).

What C gives up vs B (recorded for the design discussion, from §7): flush/merge can
verify counts and structure but not per-child correspondence (derivation can only be
re-derived); the pending-child-delete counting trap returns as a future consumer
obligation (every derivation site must count dead docs between a child delete and
compaction of BOTH formats). What C gains: literal I1, no field-semantics fork, no
scheme-freeze problem, doc-faithful (§5.4, Task 7), cheap future upgrade path to B if
deletes ever force it.

---

## 8b. Live server verification (end-to-end, real REST path)

The full flow was exercised on a live single-node server (`./gradlew run` with the
arrow-base, arrow-flight-rpc, analytics-engine, composite-engine,
analytics-backend-lucene, analytics-backend-datafusion, parquet-data-format plugins;
both experimental flags via `-Dtests.opensearch.…`): create nested composite index →
ingest 3 nested docs → refresh/flush → inspect both files → second generation →
`_forcemerge` → re-inspect. **All stages verified on-disk** (jshell + Lucene reader
for the segment; pyarrow for the Parquet files).

Verified live: 3 logical docs == 3 Parquet rows == 6 physical Lucene docs
(`__row_id__ == docId` on every doc, children included); `LIST<STRUCT>` rows with
per-element nulls (dave's missing score) vs null lists (flat doc) correctly
distinguished; capability routing (title/author terms in Lucene only; views/score
values in Parquet only; `_id` on parents only — live evidence for the child-`_id`
delete follow-up); post-merge: 4 Parquet rows / 8 Lucene docs in one leaf, row ids
re-sequenced 0..7 through `NestedBlockExpansion`, blocks contiguous, gen-1 data
byte-identical. Note: the composite engine reports LOGICAL rows everywhere REST-visible
(`_cat`, `_segments` num_docs) — physical N+1 counts are only visible on disk.

Four gaps were found and fixed that only the live path exercises (all off the
unit-test path — POC scope had excluded internalClusterTest):

1. `ObjectMapper.canDeriveSource` — composite indices force derived-source validation,
   which rejected nested mappings at index creation. Fixed: flag-gated pass-through
   (vanilla fail-fast unchanged; real nested derived-source remains future work).
2. `DocumentParser.nestedContext` — threw "root document should have an _id field"
   because composite mode routes `_id` to the DocumentInput, not the vanilla document.
   Fixed: bypass in pluggable mode only (child-`_id` restoration still tracked).
3. `FeatureFlagSettings.BUILT_IN_FEATURE_FLAGS` — the nested feature flag was never
   registered with the node settings validator, so `opensearch.yml` rejected it
   (unit tests set flags via system properties, bypassing validation). Fixed.
4. **`LuceneIndexingExecutionEngine.refresh` reported PHYSICAL docs** — after
   `addIndexes` it rebuilt filesets with `segReader.maxDoc()`. The Scheme B→C
   conversion fixed flush-side `numRows` but missed this refresh-side rebuild.
   **Caught live by `verifyPerSegmentCrossFormatRowCountParity`** ("parquet has 4 rows
   but lucene has 8 rows") on the first real flush — the exact invariant class the
   competing branch downgraded to a warning, catching a real bug within hours of
   existing. Fixed: subtract `_nested_path` child docs for nested-attribute segments.

---

## 9. Recommendation and how to argue it

**Recommendation: Scheme B (stored packed keys), hardened with:**
- `setParentField` everywhere (from Ansh's branch — structural insurance),
- vanilla's `_id`-on-children restored (atomic updates; scheme-neutral),
- the logical-rows wire contract kept (Parquet side stays plain; per the comment thread).

Primary reasons, in order: (1) in a two-store system whose defining risk is *silent
divergence*, auditability of correspondence outweighs field-semantics stability — B is
the only scheme whose correctness is checkable by independent invariants; (2) the
performance intuition favoring derivation is mostly optical — storage/write costs are
a tie, and derivation moves cost to read-always; (3) B implements deletes/merge with
the least custom machinery (range queries; §4.1.2.1 arithmetic).

**Honest posture for the closure meeting:** the HLD's center of gravity is C, so
present B as *implementation findings challenging the mainline reading* — not as
doc-lawyering. The intent in §4.1.1/§5.4 was formed before the delete/atomicity floor,
the counting-deleted-children trap, and the unverifiability problem were understood.

**What would change the recommendation:** (a) bitset benchmark (open action item)
showing random-access rank/select is effectively free at scale AND the team weights
`__row_id__` semantic stability above auditability → C is defensible; (b) the
"Lucene children exist only for text fields" idea (Slack) becoming real scope → a
different architecture, decide it as such.

**If the team lands on C anyway, preserve three things:** child `_id`; verification
instruments *adapted, never deleted*; one shared, adversarially-tested derivation
library (counting trap, multi-level narrowing) rather than per-consumer reimplementation.
Note that our POC's parse-layer (scope/tracker), codec arithmetic, and invariant
patterns all transfer to C nearly unchanged — the codec simply runs at merge time
instead of write time.

---

## 10. Open items ledger

| Item | Owner / status |
|---|---|
| Bitset memory + rank/select benchmark | open (meeting action item) — the one measurement gating B-vs-C |
| Deletes/updates support status in Mustang | open (meeting action item) — determines how hard the §4 floor binds today |
| Empty array vs absent field encoding in Parquet | decide at VSR write time |
| Buffer memory bound for huge child arrays | open (HLD open item; applies to all branches) |
| Mixed key-scheme on first-nested-field mapping update | TODO in code (freeze per writer generation) |
| `include_in_parent` / `include_in_root` in pluggable mode | silently ignored today (both branches) |
| Derived source + nested | must be explicitly rejected (not bypassed) until reconstruction exists |
| HLD updates | reconcile §4.1.1 / §4.1.2.1 / §5.4 into one scheme post-closure; add parse-layer identity subsection (5.1); record codec layout if B adopted |

# catdiff

Turn two catalog releases into a queryable change log, in the browser, with no backend.

Status: spec (RDD Type A). Gate-0 not run yet. License: MIT.

## Purpose

**Problem.** Every catalog release reprocesses the sky. Sources appear, vanish, split, merge, or move, and IDs change between releases. Gaia published one-off DR2-to-DR3 neighbourhood tables. STILTS can do proper-motion-corrected matches with group IDs given enough scripting. What does not exist is a zero-install, shareable, provenance-carrying diff with an event taxonomy, for any catalog pair.

**Solution.** A static GitHub Pages app. You pick two catalogs (HATS URLs or local Parquet files) and a sky region. The browser fetches only the HEALPix partitions it needs, propagates both catalogs to a common epoch, builds a match graph, and classifies every connected component. The result is an event log you can query with SQL, inspect source by source, share as a link, and export with full provenance.

**Who benefits.** Astronomers moving pipelines and papers from one release to the next. Survey teams validating a new release. Students learning what a data release actually changes.

**Timing.** Gaia DR4 is planned for 2 December 2026. DR4 will not be in HATS on day one, so the release-day path is TAP cone → local Parquet via `catdiff-fetch` (feature 10). v1 must be solid on DR2 to DR3 before then.

## Core concepts

| Term | Meaning |
|---|---|
| Release A / B | Older and newer catalog. A's epoch is the common epoch: B is propagated back with B's (better) proper motions, as Gaia did for `dr2_neighbourhood`. |
| HATS | Hierarchical Adaptive Tiling Scheme. Parquet partitioned by HEALPix pixel. |
| Component | Connected group of sources linked by candidate pairs within the match radius. |
| Event | Classification of one component (see table below). |
| Oracle | An independent published cross-release match, used to score catdiff (Gaia `dr2_neighbourhood`). |
| Run manifest | JSON describing inputs, parameters, and tool version. Makes a diff reproducible. |

### Event classes

| (n_A, n_B) | Event | Rule |
|---|---|---|
| (1, 0) | `vanished` | |
| (0, 1) | `appeared` | |
| (1, 1) | `kept` | `sep <= max(offset_min_arcsec, offset_nsigma * sigma_pos)` |
| (1, 1) | `offset` | Otherwise. Astrometric inconsistency after propagation, not motion (see below). |
| (1, N>1) | `split` | |
| (N>1, 1) | `merged` | |
| (N>1, M>1) | `ambiguous` | |

Seven classes. `offset` is deliberately not called "moved": when B is propagated with its own proper motion, real motion is removed, so a residual separation means the two releases' astrometry disagrees (a data-quality signal). Only for pairs where neither side has proper motion does `offset` include real motion; those carry `no_pm`.

### Two-radius matching

A single radius links chains of unrelated neighbours in crowded fields into one `ambiguous` blob. Matching runs in two passes:

1. **Tight pass** at `r_tight` (default 0.2″): mutual nearest neighbours within `r_tight` become 1:1 components (`kept` or `offset`) and are removed from both sides.
2. **Wide pass** at `mr` (default 1.0″): union-find over the remaining sources gives `split`, `merged`, `ambiguous`, `vanished`, `appeared`.

Both radii are in the manifest. Gate-0 item 7 sets the defaults from the measured DR2→DR3 separation distribution.

`sigma_pos` is the combined positional error of the pair at the common epoch: quadrature sum of A's `ra_error`/`dec_error` and B's position errors grown by `pm_error × |Δt|`. Gaia errors are in mas; convert to arcsec. v1 ignores `ra_dec_corr` and other correlations (AIDEV-TODO). If either side lacks errors, only `offset_min_arcsec` applies and the event gets the flag `no_errors`. Gaia errors are sub-mas to a few mas, so with a 100 mas floor the nσ branch never fires; the floor default is therefore set at Gate-0, expected in the 20–30 mas range.

## Architecture

```
URL state ──► app.js ──► hats.js (properties, partition_info) ──► region → partition list
                 │                                                  │
                 │       db.js (DuckDB-Wasm, HTTP range reads) ◄────┘
                 │          │ Arrow tables A, B (region + margin)
                 ▼          ▼
            match.worker.js: epoch.js → candidate pairs → union-find → classify.js
                 │
                 ▼
            events table (DuckDB) ──► plot / lineage card / SQL console / exports
```

Data flow, step by step:

1. **Resolve inputs.** Each catalog is either a HATS root URL or a local Parquet file (drag and drop). Each has a column map (preset or user-edited).
2. **Plan fetch.** Expand the region by the margin `mr + pm_cap × |Δt|` (`pm_cap` default 10 000 mas/yr; sources above it get the flag `pm_over_cap`). Read `partition_info.csv` and select partitions whose HEALPix pixel intersects the expanded region.
3. **Fetch.** DuckDB-Wasm reads only the mapped columns from those partitions, filtered by `_healpix_29 BETWEEN` ranges for the region's nested pixels (HATS partitions are sorted by `_healpix_29`, so Parquet row-group statistics prune on that column, not on `ra`/`dec`). Local files without `_healpix_29` get it computed in `db.js` and use the RA/Dec box. Cache the fetched Arrow result per (partition URL, ETag, columns, pixel range) in OPFS, never raw partitions.
4. **Propagate.** Move B to A's `ref_epoch` using B's proper motions (ESA 1997 rigorous formula, see References). If a B row has no proper motion, it stays put; A's proper motion is not used for it. Unpropagated rows get the flag `no_pm`.
5. **Match.** Two-radius matching as defined above (optional `|dG| < dmag_max` on candidate pairs). Grid hashing on a tangent plane; no full cross product.
6. **Classify.** Union-find over pairs gives components. Classify each component. Keep only components whose centroid lies inside the original (unexpanded) region.
7. **Present.** Write events to a DuckDB table. Render the plot, lineage cards, SQL console, oracle panel, and exports.

## Recommended stack

| Layer | Chosen | Why | Rejected |
|---|---|---|---|
| Query engine | DuckDB-Wasm, latest stable (1.5.x at spec time), loaded from jsdelivr | SQL console, Parquet range reads, Arrow out | hyparquet (no SQL) |
| Region view | In-repo WebGL2 gnomonic plot; port hatsmap renderer code where it fits | v1 regions are ≤ 1°, a tangent plane is enough | Aladin Lite v3 (GPL v3, conflicts with MIT) |
| HEALPix | WASM build of a CDS HEALPix library if Gate-0 finds a browser-ready one; else in-repo `ang2pix_nest` | Same math as HATS | Hand-rolled `query_disc` |
| Matcher | Plain JS in a Web Worker | Measure first; port to Rust/WASM only if the perf gate fails | WebGPU (v2 at earliest) |
| Cache | OPFS | Repeat regions load instantly | IndexedDB |
| Build | None. Native ES modules, import maps, GitHub Pages from `main` root | Matches other ejoliet browser tools | Vite + Actions deploy |
| Tests | `node --test` on pure modules | No browser needed for logic | Jest, Vitest |
| Sample builder | `uv run` script with inline deps (pyvo, pyarrow, astropy) | Builds tour data from the Gaia archive TAP service | Committing hand-made files |

## Repository layout

```
catdiff/
├── index.html              # app shell, import map
├── src/
│   ├── app.js              # wiring, run lifecycle
│   ├── state.js            # URL permalink encode/decode
│   ├── catalogs.js         # presets + column maps (gaia_dr2, gaia_dr3, gaia_dr4 stub)
│   ├── hats.js             # properties, partition_info, region -> partitions
│   ├── healpix.js          # ang2pix_nest + region coverage
│   ├── db.js               # DuckDB-Wasm init, fetch, events table
│   ├── epoch.js            # proper-motion propagation (pure)
│   ├── match.js            # pairs + union-find (pure)
│   ├── classify.js         # component -> event (pure)
│   ├── match.worker.js     # thin worker wrapper over match.js + classify.js
│   ├── manifest.js         # run manifest build + hash
│   ├── export.js           # Parquet, CSV, manifest bundle
│   ├── oracle.js           # agreement vs published neighbourhood table
│   └── view/
│       ├── plot.js         # WebGL2 gnomonic plot, event colors
│       ├── card.js         # lineage card
│       ├── sql.js          # SQL console + saved queries
│       └── tour.js         # guided tour
├── samples/                # tour fields: A, B, oracle Parquet + manifest.json
├── scripts/make_samples.py # builds samples/ from Gaia TAP (Emmanuel runs it)
├── tests/                  # node --test, fixtures as small JSON
├── AGENTS.md
├── README.md
└── LICENSE
```

## Prerequisites

| Requirement | Version | Used for |
|---|---|---|
| Modern browser | Chrome/Edge/Firefox/Safari current | WebGL2, OPFS, module workers |
| Node.js | 20+ | `node --test` only. No npm install. |
| uv | latest | `scripts/make_samples.py` |
| Python | 3.11+ (managed by uv) | Sample builder |

No secrets. No credentials. All data sources are public.

## Quick start

```bash
git clone https://github.com/ejoliet/catdiff && cd catdiff
uv run scripts/make_samples.py          # builds samples/ from the Gaia archive
node --test tests/                      # logic tests
python3 -m http.server 8000             # then open http://localhost:8000
```

Deploy: repo Settings → Pages → Deploy from branch `main`, folder `/`.

## Configuration reference

All configuration lives in the URL so every view is a shareable permalink. `state.js` owns this contract.

| Param | Type | Default | Required | Meaning |
|---|---|---|---|---|
| `a`, `b` | string | none | yes | Catalog preset id (`gaia_dr2`) or HATS root URL |
| `ra`, `dec` | float, deg | none | yes | Region center (ICRS) |
| `r` | float, deg | 0.25 | no | Cone radius. Hard cap 1.0. |
| `mr` | float, arcsec | 1.0 | no | Wide match radius |
| `rt` | float, arcsec | 0.2 | no | Tight radius, `r_tight` |
| `dm` | float, mag | off | no | Max magnitude difference for a candidate pair |
| `omin` | float, arcsec | Gate-0 | no | `offset_min_arcsec` |
| `ons` | float | 5 | no | `offset_nsigma` |
| `pmcap` | float, mas/yr | 10000 | no | `pm_cap` for the fetch margin |
| `sel` | int64 | none | no | Selected component id (opens its lineage card) |
| `q` | string, base64url | none | no | SQL in the console |
| `tour` | string | none | no | Tour field id; loads bundled samples |

Local files cannot go in a URL. A permalink for a run that used local files carries the run manifest hash and shows a "load the same files" prompt.

## Interface contract

### Event table (shared with the future TUI)

This schema is the contract between this web app and the planned Textual TUI (full-sky, lsdb). Change it only with a version bump in `run.schema_version`.

| Column | Type | Note |
|---|---|---|
| `event` | string enum | See event classes |
| `component_id` | int64 | Stable within one run |
| `ids_a`, `ids_b` | list<int64> | Source IDs |
| `n_a`, `n_b` | int32 | |
| `ra`, `dec` | double, deg | Component centroid at common epoch |
| `sep_arcsec` | double | 1:1 only, else null |
| `dmag` | double | 1:1 only, else null. Band from the column map. |
| `healpix_29` | int64 | Nested, order 29, from centroid |
| `flags` | list<string> | `no_pm`, `no_errors`, `pm_over_cap`, `tight_pass` |
| `run` | struct | `schema_version`, `a`, `b`, `region`, params, `epoch_a`, `epoch_b`, `tool_version`, `manifest_sha256` |

### Column map

Each catalog maps logical names to physical columns. Required: `id`, `ra`, `dec`. Optional: `ref_epoch`, `pmra`, `pmdec`, `ra_error`, `dec_error`, `mag`, plus quality columns shown on the lineage card (Gaia: `ruwe`, `duplicated_source`, `ipd_frac_multi_peak`, `astrometric_excess_noise`). Missing optional columns degrade features; they never fail the run.

`pmra` is treated as μα* (includes cos δ), as in Gaia. A column-map field `pmra_includes_cosdec` (default true) covers catalogs that differ.

### Pure function signatures

```js
// epoch.js
propagate({ra, dec, parallax, pmra, pmdec, radial_velocity}, fromEpoch, toEpoch) -> {ra, dec, moved: boolean}
// ESA 1997 Sec 1.5.5; null parallax/radial_velocity treated as 0; null pm -> moved=false
// match.js
candidatePairs(A, B, {radiusArcsec, dmagMax}) -> Int32Array pairs  // [iA0, iB0, iA1, iB1, ...]
mutualNearest(A, B, pairs, rTightArcsec) -> Int32Array pairs       // tight pass, 1:1 only
components(nA, nB, pairs, excludedA, excludedB) -> {compOfA, compOfB, count}
// classify.js
classify(component, {offsetMinArcsec, offsetNsigma}) -> {event, sep_arcsec, dmag, flags}
```

## Features

### v1 (in scope)

1. **Diff a region.** Two inputs, one cone, the full pipeline above.
2. **Local files.** Drag and drop two Parquet files. Makes catdiff work for any catalog pair, including unpublished ones.
3. **Permalinks.** Every view (inputs, region, params, selected component, SQL) is a URL. Paste it in Slack or a paper's data note.
4. **Guided tour.** Three bundled fields load instantly with no network: M67 (open cluster), one dense Galactic plane field, one sparse high-latitude field. Each step explains one event type with a real example.
5. **Lineage card.** Click a component: both releases side by side (positions, proper-motion vectors, magnitudes, quality columns), the separation, and a plain-language reason for the classification ("1 DR2 source → 2 DR3 sources 0.6″ apart; high DR2 astrometric excess noise suggests an unresolved pair"). DR2 RUWE is not in `gaiadr2.gaia_source` as far as known; the DR2 preset uses `astrometric_excess_noise` unless Gate-0 confirms a joinable RUWE table.
6. **Summary bar.** Counts per event type, event fraction versus magnitude, and the `no_pm` count (so `appeared`/`vanished` from unpropagated sources is visible).
7. **SQL console.** DuckDB over the `events` table plus the fetched `a` and `b` tables. Ships with saved example queries.
8. **Oracle panel.** For Gaia DR2→DR3, the pair set at 2″ is compared with `dr2_neighbourhood` (precision and recall of links), plus a list of disagreements. This validates pair-finding only. Classification has no published oracle; v1 shows two proxy checks instead: the fraction of `split` events whose B sources have `duplicated_source` or high `ipd_frac_multi_peak`, and the radial distribution of `vanished` sources around bright (G < 10) stars.
9. **Reproducible export.** Parquet and CSV of events, plus `manifest.json` (inputs, params, epochs, tool commit, SHA-256). The manifest alone can re-run the diff.
10. **`catdiff-fetch` CLI.** `uvx --from git+https://github.com/ejoliet/catdiff catdiff-fetch --tap <url> --table <t> --cone RA DEC R --out a.parquet` pulls any cone from any TAP service into catdiff's Parquet layout (with `_healpix_29`). This is the DR4 release-day path and replaces the ad-hoc sample script; `make_samples.py` becomes a thin wrapper around it.

### v1.1 (after v1 ships)

- **Science recipes.** One-click saved queries: newly resolved pairs (splits whose A source had high astrometric excess noise or RUWE), likely spurious A sources (vanished near bright stars), high proper-motion movers, parallax sign flips.
- **Change-rate map.** HEALPix map of event rates over a larger area; shows crowding and scan-pattern imprints.
- **"Open in Python".** Copy an lsdb snippet that reproduces the current diff, bridging to the TUI and notebooks.
- **Report a finding.** Prefilled GitHub issue link with permalink and manifest. Zero-backend crowdsourcing.
- **Name resolver.** "Go to M67" via CDS Sesame, if its CORS allows it (Gate-0 item 6).

### Later

SAMP (Simple Application Messaging Protocol) to TOPCAT/Aladin Desktop, iframe embed mode for papers and blogs, DR4 release-day gallery, Roman releases, VOTable export, the Textual TUI for full-sky runs.

### Scientific caveats the UI must show

- **Magnitudes are not directly comparable across releases.** Gaia passbands changed between releases, so `dmag` mixes calibration with real variability. Label it "release-to-release Δmag", never "variability".
- **Sources without proper motion are not propagated.** For two-parameter solutions the separation includes real motion over Δt. Show `no_pm` prominently; a `moved` or `vanished` call on such a source is weaker evidence.
- **The oracle used 2″ and no ranking.** Gaia's `dr2_neighbourhood` lists all pairs within 2″. Score the oracle panel at 2″ (or filter the oracle to the active radius), never by comparing a 1″ run to the 2″ table.
- **Match radius drives everything.** Show the active radius on every summary, plot, and export.

## Scientific references (what `epoch.js` and `match.js` implement)

| Role | Reference | Use |
|---|---|---|
| Propagation algorithm | ESA 1997, *The Hipparcos and Tycho Catalogues*, Vol. 1, Sec. 1.5.5 | Rigorous position propagation. Implemented in vector form, so no pole singularity. |
| Cross-match procedure to mirror | Gaia DR3 documentation, Chapter 16 "Cross-match with Gaia DR2" (16.2 generation, 16.4.1 ADQL) | Direction (newer → older epoch), 2″ radius, no ranking |
| Oracle table | `gaiadr3.dr2_neighbourhood` (DR3 documentation 20.4.1) | Scoring |
| Golden test values | Gaia archive ADQL `EPOCH_PROP_POS(ra, dec, parallax, pmra, pmdec, radial_velocity, ref_epoch, target_epoch)` | `make_samples.py` saves its output for ~50 sources (incl. high proper motion and |dec| > 85°) to `tests/fixtures/epoch_prop.json`. `epoch.js` must agree within 0.1 mas. |

## Error handling

| Error | Cause | Behavior |
|---|---|---|
| `CorsError` | Source lacks CORS | Stop. Suggest local-file mode. |
| `RangeUnsupportedError` | Server ignores Range | Stop for files > 50 MB; else fetch whole file. |
| `SchemaMismatchError` | Column map points at a missing required column | Stop. Open column-map editor with the offending field. |
| `RegionTooLargeError` | `r > 1.0` or estimated rows > `max_rows` (default 300k per side) | Stop before fetching. Suggest a smaller cone. |
| `PartitionFetchError` | Network failure | Retry 3 times, exponential backoff (0.5, 1, 2 s), then stop and name the partition. |
| `WorkerCrashError` | Out-of-memory in worker | Stop. Suggest smaller region. |

Never show partial events as if complete. A failed run shows no event table.

## Testing

| Suite | Command | Covers |
|---|---|---|
| Unit | `node --test tests/` | `epoch`, `match`, `classify`, `state` round-trip, `healpix` known values, `manifest` hashing |
| Fixtures | same | Hand-built cases for every event class, edge components straddling the region boundary, missing proper motion, missing errors |
| Oracle | in browser, M67 tour field | Agreement numbers shown in the oracle panel |

Browser checks (rendering, OPFS, live HATS fetch) are manual. Emmanuel runs them with the commands in Gate-0 and Next Steps.

## Non-goals (v1)

- No backend, accounts, or server-side state.
- No full-sky diffs. That is the TUI's job.
- No Iceberg in the browser.
- No regions larger than 1° radius.
- No GPL dependencies.
- No WebGPU, no multithreaded DuckDB (GitHub Pages cannot set COOP/COEP headers).

## Gate-0 (Emmanuel runs these before Phase 1)

1. **Is there a second Gaia release in HATS?**
   ```bash
   aws s3 ls --no-sign-request s3://stpubdata/gaia/
   ```
   If only DR3 exists, v1 live mode pairs DR3 with local files; the tour uses TAP-built samples.

2. **Confirm CORS and Range on the DR3 HATS bucket.**
   ```bash
   P=$(aws s3 ls --no-sign-request --recursive s3://stpubdata/gaia/gaia_dr3/public/hats/gaia/dataset/ | awk '/Npix=.*parquet/{print $4; exit}')
   curl -s -o /dev/null -D - -r 0-1023 -H "Origin: https://ejoliet.github.io" "https://stpubdata.s3.amazonaws.com/$P" | grep -Ei "^HTTP|access-control"
   ```
   Pass: `206` and `access-control-allow-origin`.

3. **DuckDB-Wasm smoke test.** Open https://shell.duckdb.org and run:
   ```sql
   SELECT count(*), min(ra), max(ra) FROM read_parquet('https://stpubdata.s3.amazonaws.com/<P from step 2>');
   ```

4. **Build the samples and the oracle.**
   ```bash
   uv run scripts/make_samples.py --field m67
   ```
   Verify the `gaiadr3.dr2_neighbourhood` column names the script uses against the Gaia archive table description before running. Also confirm whether DR2 RUWE lives in a separate table (`gaiadr2.ruwe`) and is joinable.

7. **Separation distribution.** From the M67 oracle sample, histogram `angular_distance` for pairs with `proper_motion_propagation = true`. Set `offset_min_arcsec` at roughly the 99th percentile of the core and `r_tight` where the second population begins.

5. **Choose the HEALPix library.** Check whether a CDS HEALPix WASM build is usable from a plain ES module. If not, use the in-repo fallback.

6. **Sesame CORS (v1.1 only).**
   ```bash
   curl -s -o /dev/null -D - -H "Origin: https://ejoliet.github.io" "https://cds.unistra.fr/cgi-bin/nph-sesame/-oxp/SNV?M67" | grep -i access-control
   ```

Record each result in `AGENTS.md` under Recently Burned or as a resolved Open Question.

## Open questions

| # | Question | Default if unresolved |
|---|---|---|
| 1 | Second Gaia release available as HATS with CORS? | Tour + local files only for DR2 |
| 2 | HEALPix: CDS WASM build or in-repo? | In-repo `ang2pix_nest` + dense sampling |
| 3 | Can hatsmap renderer code be reused for `plot.js`? | Write a minimal WebGL2 point renderer |
| 4 | Default `mr`, `r_tight`, `offset_min_arcsec` for DR2→DR3? | 1.0″ / 0.2″ / Gate-0 item 7 |
| 5 | Oracle target agreement for pair-finding | Measure at Gate-0, set target at 2 points below |
| 6 | Does pyiceberg support changelog views (TUI only)? | Not needed for v1 |

## Agent build instructions

Implement from this README and `AGENTS.md` only. Resolve or accept defaults for Open Questions first.

### Build order

| Phase | Deliverable | Done when |
|---|---|---|
| 0 | Scaffold, `index.html` + import map, `LICENSE`, `pyproject.toml` with `catdiff-fetch` entry point, `make_samples.py` | Page loads, empty state shows; `uv run catdiff-fetch --help` works |
| 1 | Pure modules: `epoch`, `match`, `classify`, `state`, `healpix`, `manifest` | `node --test tests/` passes all fixtures |
| 2 | `db.js` + local-file and tour mode | Tour field M67 produces an events table |
| 3 | Worker, `plot.js`, summary bar, lineage card | Emmanuel confirms visuals with the command below |
| 4 | `hats.js` live mode, OPFS cache, error classes | DR3 live cone fetch works (Emmanuel checks) |
| 5 | SQL console, oracle panel, exports, permalinks, tour steps | All acceptance criteria pass |

### Constraints

- Plain ES modules, no bundler, no npm runtime dependencies.
- Pure logic stays free of DOM and DuckDB so `node --test` covers it.
- `AIDEV-NOTE`, `AIDEV-TODO`, `AIDEV-QUESTION` anchors at non-obvious decisions.
- Do not run browser checks, push to GitHub, or deploy. Print the exact commands for Emmanuel instead.

### Acceptance criteria

- [ ] `node --test tests/` passes, every event class has a fixture
- [ ] Boundary fixture: a component straddling the region edge is counted once, by centroid
- [ ] M67 tour runs offline (DevTools offline mode) and shows all seven event types or explains absent ones
- [ ] Oracle panel shows pair-finding agreement at or above the Gate-0 target, and both proxy classification checks render
- [ ] A permalink reopened in a fresh browser reproduces the same counts
- [ ] Exported `manifest.json` re-runs to identical event counts
- [ ] 0.25° DR3 live cone in the tour's dense Galactic plane field (chosen at Gate-0 to fit under `max_rows`) completes in under 15 s on Emmanuel's laptop
- [ ] Every error class in the table is reachable by a documented manual test
- [ ] No GPL code, no secrets, no hard-coded personal URLs besides the public repo

## Next steps

1. Emmanuel runs Gate-0 items 1–5 and records results.
2. Agent builds Phase 0–1; Emmanuel runs `node --test tests/`.
3. Agent builds Phase 2–3; Emmanuel runs `python3 -m http.server 8000` and checks the M67 tour.
4. Agent builds Phase 4–5; Emmanuel enables Pages and runs the acceptance checklist.
5. Before 2 December 2026: add the `gaia_dr4` preset stub and test `catdiff-fetch` against the Gaia archive TAP so release day is a fetch plus a column-map update.

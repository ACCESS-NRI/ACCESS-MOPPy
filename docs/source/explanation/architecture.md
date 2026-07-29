# Architecture: how a CMORisation runs

ACCESS-MOPPy converts raw ACCESS model output into CMIP-compliant datasets
without depending on the CMOR C library. Instead, the whole pipeline is built
on xarray and dask: input files are opened lazily, the target variable is
selected or derived, coordinates and attributes are rewritten against a
controlled vocabulary, and the result is serialised with `netCDF4` into a
Data Reference Syntax (DRS) tree. This page explains how the pieces fit
together — the public facade, the shared `CMORiser` base class, the
realm-specific subclasses, and the batch layer that scales the whole thing
out on PBS.

## The facade: `ACCESS_ESM_CMORiser`

`ACCESS_ESM_CMORiser` (in `src/access_moppy/driver.py`) is the recommended
entry point. It does no scientific processing itself; its job is *routing*:

- **Vocabulary selection by `cmip_version`.** `"CMIP6"` uses
  `CMIP6Vocabulary`, `"CMIP6Plus"` uses `CMIP6PlusVocabulary` (or
  `CMIP6PlusMIPVocabulary` when the table uses the new MIP naming scheme,
  e.g. `APmon` rather than `Amon`), and `"CMIP7"` uses `CMIP7Vocabulary`.
  For CMIP7, a branded compound name such as
  `atmos.rsdt.tavg-u-hxy-u.mon.GLB` is first translated to its CMIP6
  equivalent (`Amon.rsdt`) so that the existing model mappings can be
  reused, while CMIP7 vocabularies drive metadata, filenames, and DRS paths.
- **Realm selection from `compound_name`.** The CMIP table in the compound
  name determines the component implementation: atmosphere/land tables
  (`Amon`, `Lmon`, `day`, `CFmon`, `fx`, …, plus MIP prefixes `AP*`, `LP*`,
  …) go to `Atmosphere_CMORiser`; ocean tables (`Omon`, `Oyr`, `Oday`,
  `Ofx`, `OP*`, `OB*`) go to `Ocean_CMORiser_OM3` for MOM6-based models
  (`ACCESS-OM3`, `ACCESS-CM3`) or `Ocean_CMORiser_OM2` for MOM5 (B-grid)
  models; sea-ice tables (`SImon`, `SIday`, `SI*`) go to `SeaIce_CMORiser`.
- **Mapping lookup.** The model mapping JSON
  (`<model_id>_mappings.json` under `src/access_moppy/mappings/`) is loaded
  for the requested variable; a `MappingNotFoundWarning` invites a
  contribution when the model or variable is not yet supported.
- **Input resolution.** Callers may pass explicit paths, an already-open
  xarray object, or just `input_folder`. In the latter case
  `file_discovery.discover_files()` locates the raw files in a payu archive
  (`output000/`, `output001/`, …) using, in order: a per-variable
  `file_pattern` override in the mapping entry, then the component-level
  `frequency_patterns` from the mapping's `model_info.file_discovery` block
  (with `{model_var}` substituted for one-file-per-variable ocean output).
  `start_year`/`end_year` filter files by the year parsed from filenames,
  with no file I/O.

The facade also fills in defaults (grid label, piControl `parent_info`) and
then delegates everything else to the component CMORiser.

## The engine: the `CMORiser` base class

`CMORiser` (in `src/access_moppy/base.py`) holds the realm-independent
machinery. Its subclasses only implement `select_and_process_variables()`
and `update_attributes()`; everything else is shared:

- **Loading** — `load_dataset()` opens inputs with `xr.open_mfdataset`
  (one dask chunk per file along time), keeping only the model variables,
  axes, and bounds the mapping requires, and optionally validating and
  resampling the temporal frequency.
- **Variable selection and units** — the mapping entry names the raw
  `model_variables` and the target CMIP units; `_check_units()` raises if
  the mapping's declared units disagree with what the CMIP table expects,
  and `_check_range()`/`_check_calendar()` guard value ranges and calendars.
- **Bounds generation** — `calculate_missing_bounds_variables()` computes
  time, latitude, and longitude bounds when the raw files do not carry them,
  and wires up each coordinate's `bounds` attribute.
- **DRS metadata and output** — the vocabulary object supplies required
  global attributes, generates the CMIP-compliant filename, and builds the
  versioned DRS path (with a `latest` symlink) when `drs_root` is set.
- **File splitting** — `write()` slices time-dependent datasets into
  calendar-aligned chunks according to `split_years` (see
  {doc}`/explanation/time_chunking_split_years`), writing each chunk with an
  optimised metadata-first NetCDF4 layout and optional compression.

## Lifecycle: `run()` → `to_dataset()` → `write()`

`ACCESS_ESM_CMORiser.run()` calls the component's `run()`, which executes a
fixed sequence:

```text
run()
 ├─ select_and_process_variables()   # realm-specific: load, derive, rename
 ├─ drop_intermediates()             # remove raw model variables
 ├─ standardize_missing_values()     # NaN → CMIP missing value (1e20)
 ├─ update_attributes()              # global + variable attrs from vocabulary
 └─ reorder()                        # canonical variable ordering
```

Because everything is lazy dask up to this point, `run()` is cheap;
`to_dataset()` exposes the live CMORised `xr.Dataset` for inspection or
in-memory use (e.g. the ESMValTool integration), and `write()` (or
`run(write_output=True)`) triggers the actual computation and serialisation.

```text
 user code
    │
    ▼
 ACCESS_ESM_CMORiser (driver.py)          ← facade
    │  compound_name → realm              cmip_version → vocabulary
    │  input_folder  → file_discovery.py
    ▼
 Atmosphere_CMORiser | Ocean_CMORiser_OM2/OM3 | SeaIce_CMORiser
    │        (all subclasses of CMORiser in base.py)
    │  mapping JSON ──► select/derive variables (derivations/)
    │  vocabulary   ──► attributes, filename, DRS path
    ▼
 write(): split_years → per-chunk NetCDF files under the DRS tree
```

The vocabulary classes in `src/access_moppy/vocabulary_processors.py` are
the single source of truth for controlled-vocabulary content: they read the
bundled CMOR tables and CV JSON files (under `src/access_moppy/vocabularies/`),
expose the axis and variable definitions the CMORisers rename against, decide
which global attributes are required, and standardise missing values.

## The batch layer

A single `ACCESS_ESM_CMORiser` instance handles one variable. Production
CMORisation of hundreds of variables is orchestrated by
`src/access_moppy/batch_cmoriser.py` (the `moppy-cmorise` CLI, described in
{doc}`/howto/batch_processing` and {doc}`/reference/configuration`):

- The login-side invocation reads a YAML config, pre-populates a **SQLite
  task database** (`cmor_tasks.db` in the output folder) via `TaskTracker`,
  and submits exactly one PBS *monitor* job — so the workflow survives the
  login shell disconnecting.
- The monitor, running on a compute node, generates and submits one PBS job
  per variable. Job scripts are rendered from **Jinja2 templates** in
  `src/access_moppy/templates/` (`cmor_job_script.j2` for the PBS wrapper,
  `cmor_python_script.j2` for the per-variable worker,
  `cmor_monitor_script.j2` for the monitor itself). Per-variable resource
  overrides come from the config's `variable_resources` section.
- Each worker records its progress in the shared SQLite database (workers
  open their own connections), the monitor reconciles jobs that die without
  reporting (e.g. OOM-killed by PBS), and an optional Streamlit dashboard
  visualises the task table live.

## Related pages

- {doc}`/tutorials/getting_started` — a first end-to-end CMORisation.
- {doc}`/reference/mapping_reference` — the mapping JSON schema the pipeline
  consumes.
- {doc}`/explanation/derived_variables` — how multi-variable derivations fit
  into `select_and_process_variables()`.
- {doc}`/explanation/time_chunking_split_years` — how `write()` splits output
  files.

# Derived variables

Many CMIP variables are not a raw ACCESS output field with a new name —
they are computed from one or more model fields, sometimes at a different
temporal resolution than the inputs (e.g. a monthly diagnostic derived from
daily extrema). This page explains *how* that computation is wired into the
CMORisation pipeline via `select_and_process_variables()`. For the JSON
schema of the `calculation` block itself, see
{doc}`/reference/mapping_reference`.

## Three calculation types

Every mapping entry's `calculation.type` selects one of three code paths in
`select_and_process_variables()` (implemented per realm, e.g.
`src/access_moppy/atmosphere.py`):

**`direct`** — the model variable is simply renamed to the CMOR name; no
computation happens. This is the common case (e.g. `tas` from `fld_s03i236`).

**`formula`** — the CMOR variable is computed from one or more model
variables using a small recursive expression language, evaluated by
`evaluate_expression()` (`src/access_moppy/derivations/__init__.py`).
Expressions are nested dictionaries such as:

```json
{
  "type": "formula",
  "operation": "optical_depth",
  "operands": [
    ["fld_s02i284", "fld_s02i285", "fld_s02i286",
     "fld_s02i287", "fld_s02i288", "fld_s02i289"],
    3
  ]
}
```

`evaluate_expression()` recursively resolves `operation`/`args`/`kwargs`
(plus `literal` and `optional` leaf nodes) against a context of the
mapping's `model_variables`, then dispatches to the named function in the
`custom_functions` registry — either a small inline lambda (`add`,
`kelvin_to_celsius`, …) or a realm-specific derivation imported from the
`calc_*` modules below. Expressions can nest arbitrarily, so compound
derivations (e.g. combine two model variables, then convert units) are
expressed as one tree rather than chained mapping entries.

**`internal`** — the CMOR variable is generated without any input file at
all, by calling a registered function directly with the `kwargs` from the
mapping entry (e.g. `calculate_areacella`, which derives cell-area from grid
geometry rather than from model output).

## Where derivation functions live

Actual numerical work is grouped by realm under `src/access_moppy/derivations/`,
and every public function is re-exported through `derivations/__init__.py`
and registered in `custom_functions` so mapping JSON can reference it by
name:

- `calc_atmos.py` — level-to-height interpolation, cell-area calculation.
- `calc_aerosol.py` — aerosol optical depth.
- `calc_land.py` — soil/carbon/nitrogen pool aggregation, tile-fraction
  extraction and weighting, land-cover reclassification.
- `calc_ocean.py` — heat/mass transport, overturning streamfunction,
  cell-area, global averages.
- `calc_seaice.py` — sea-ice extent/area/volume per hemisphere.
- `calc_utils.py` — shared helpers (adding/dropping axes, monthly
  min/max resampling) used across realms.

## Handling a change in temporal resolution

Some formulas change the time axis itself — for example a monthly mean
computed from daily inputs. After evaluating the expression,
`select_and_process_variables()` compares the result's `time` size and
coordinate values against the loaded dataset's. If either has changed, the
dataset is rebuilt around the formula's result: time-independent variables
and coordinates are carried over from the original dataset, the original
time attributes (units, calendar) are restored so filename generation still
works, and missing time/space bounds are recalculated for the new axis. If
the time axis is unchanged, the result is assigned directly into the
existing dataset — no rebuild needed.

In both calculation types that consume model variables, the raw input
variables are dropped after the CMOR variable has been produced
(`drop_intermediates()` in the `run()` lifecycle — see
{doc}`/explanation/architecture`), and any `units` attribute left over from
the calculation is cleared so that `update_attributes()` can apply the
CMOR table's authoritative units.

## Adding a new derivation

The reference page's
[Adding a New Derived Variable](/reference/mapping_reference) checklist
covers the mapping-JSON side. On the code side: implement the function in
the appropriate `calc_*.py` module, export it from
`derivations/__init__.py`, and add it to the `custom_functions` dictionary
under the name the mapping's `operation`/`function` field will reference.

## Related pages

- {doc}`/reference/mapping_reference` — the full `calculation` JSON schema,
  worked examples for each type, and the function catalogue by realm.
- {doc}`/explanation/architecture` — how `select_and_process_variables()`
  fits into the overall `run()` lifecycle.
- {doc}`/explanation/coordinates_and_grids` — derivations that reconstruct
  coordinate/grid metadata rather than data values.

## Sea-ice transport through straits

`siareaacrossline` and `simassacrossline` use the four APP4 straits on the
ACCESS-ESM1 300 × 360 grid: Fram Strait, Canadian Archipelago, Barents
Opening, and Bering Strait. Their output dimensions are `(time, line)`;
`line` contains the strait names. Grid coordinates are checked against the
bundled ancillary at every strait point before the fixed indices are used.
An unsupported or shifted grid raises an error.

The reconstruction sums `quantity × native velocity × edge length` along
the prescribed segments. The original CICE U-cell `hue/hun` lengths are
bundled in metres. T-cell `HTE/HTN`, cell areas, and geographic east/north
velocities are not interchangeable inputs. Signs follow the signed native
x/y component sums in APP4.

| Model | Area inputs | Ice mass inputs |
|---|---|---|
| ACCESS-ESM1-6 | `aice`, `siu`, `siv` | `simass`, `siu`, `siv` |
| ACCESS-ESM1-5 | `aice`, `uvel`, `vvel` | `hi × 900 kg m⁻³`, `uvel`, `vvel` |

All four native coordinate fields (`TLAT`, `TLON`, `ULAT`, `ULON`) must
also be present. Concentration is fractional; velocities are in m/s.
ESM1-5 `hi` is **grid-cell mean** ice thickness, so no additional
concentration factor is applied. ESM1-6 `simass` is already kg/m² of grid
cell; its density conversion is already included by the model.

For ESM1-6 CICE5, `siu/siv` are ice-area-weighted temporal means. Multiplying
by mean `aice` recovers the averaged area-velocity product, subject to the
model's ice-presence masking. Legacy ESM1-5 `uvel/vvel` have their original
averaging convention, retained for APP4 compatibility.

**Mass transport is an APP4-compatible reconstruction.** Multiplying
time-mean ice mass by reported velocity approximates the time-mean mass
flux. The output variable's `comment` records this method and that snow is
excluded. CICE5 `sidmasstranx/y` include snow and use a different spatial
stencil, T-cell face lengths, and timestep accumulation. They cannot
directly provide ice-only `simassacrossline`, and the reconstruction is
not an exact reproduction of that native combined diagnostic.

Masked contributions are skipped, but a wholly missing strait remains
missing. Zero ice with a masked velocity contributes zero. Time bounds
are preserved, horizontal grid coordinates and cell measures are removed,
and `line` is written as a character label with `standard_name="region"`.

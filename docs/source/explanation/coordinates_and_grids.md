# Coordinates and grids

ACCESS model output does not arrive on the simple, self-describing grids that CMIP
archives expect. The UM atmosphere uses a terrain-following "hybrid height" vertical
coordinate whose defining coefficients are not stored correctly in all model
generations, and the MOM ocean lives on a curvilinear tripolar grid whose geometry
is defined in a separate *supergrid* file rather than in the output itself. This
page explains what these coordinate systems are, why ACCESS-MOPPy has to
reconstruct or repair them during CMORisation, and where the relevant code lives.

## The UM hybrid-height vertical coordinate

The UM (Unified Model) atmosphere defines the height of model level *k* above the
geoid as a blend of a fixed height and a terrain-following term:

```
z(k) = a(k) + b(k) * z_surface
```

Here `a(k)` is a pure height in metres and `b(k)` is a dimensionless
orography-following factor that is 1 at the surface and decays to 0 where levels
become purely height-based. Both coefficients derive from the raw eta (η) levels
stored in the UM *vertlevs* namelist. The correct formulae (UM7 `setcona.F90`,
`height_gen_smooth` option; eq. 4.1 of Davies et al. 2005,
[doi:10.1256/qj.04.101](https://doi.org/10.1256/qj.04.101)) are:

```
a(k) = η(k) × z_top_of_model
b(k) = (1 − η(k) / η_etadot)²
```

where `η_etadot` is the η value of the first "constant-r" rho level — the level at
which the atmosphere decouples from orography (`b = 0` at and above it). The
quadratic term is the crucial detail: **b is not equal to η**.

### The historical bug in ACCESS-ESM1.5 and ACCESS-CM2

The CMIP6 output of ACCESS-ESM1.5 and ACCESS-CM2 stored the raw η values directly
as the `sigma_theta` (and `sigma_rho`) coordinate variables, omitting the quadratic
transformation. Any analysis relying on the orography-following part of the hybrid
height formula therefore reconstructs subtly wrong level heights over topography.
ACCESS-ESM1.6 — the first model officially supported by ACCESS-MOPPy — writes the
correctly transformed b values, so no correction is needed for ESM1.6 data. The
history of this problem is tracked in
[ACCESS-MOPPy issue #164](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/164).

### Recomputing the coefficients: `moppy-calc-ab-coeffts`

For users who compare against or re-process archived ESM1.5/CM2 data, ACCESS-MOPPy
ships a legacy utility
(`src/access_moppy/legacy_utilities/calc_hybrid_height_coeffs.py`, based on
[Martin Dix's script](https://gist.github.com/MartinDix/14d6ab8fa6997c18f5bf5456d22756d5))
that recomputes the correct a/b coefficients for both theta (full) and rho (half)
levels from a vertlevs namelist:

```
moppy-calc-ab-coeffts /path/to/vertlevs_file
```

It requires `f90nml` to parse the Fortran namelist, available via the
`access_moppy[atmos-tools]` extra. Typical vertlevs locations on Gadi are:

- ESM1.5 / ESM1.6:
  `/g/data/vk83/configurations/inputs/access-esm1p5/share/atmosphere/grids/resolution_independent/2020.05.19/vertlevs_G3`
- CM2 / CM2.1: `~access/umdir/vn10.6/ctldata/vert/vertlevs_L85_50t_35s_85km`

### Hybrid-height normalisation during CMORisation

Even with correct coefficients, hybrid-height metadata needs repair before it is
CF- and WCRP-compliant. `Atmosphere_CMORiser`
(`src/access_moppy/atmosphere.py`) handles three quirks:

- **Bounds ordering** (`_normalize_hybrid_bounds`): some UM files encode `b_bnds`
  as descending `[upper, lower]` pairs, whereas the WCRP VAR012 check expects each
  pair to bracket `b` with ordered bounds; the pairs are sorted ascending.
- **Spurious time dimensions** (`remove_spurious_time_dimensions`): when combining
  multi-file datasets, xarray conservatively broadcasts time-invariant coordinate
  and bounds variables (e.g. `lat_bnds`) along `time`. ACCESS grids are static for
  a whole run, so these dimensions are safely stripped back to a single slice.
- **`formula_terms` retargeting** (`_retarget_renamed_references`):
  `Dataset.rename` relabels variables but not the attribute *strings* that
  reference them. After renaming model-native names to CMOR names (e.g.
  `sigma_theta → b`, `surface_altitude → orog`, `theta_level_height → lev`), the
  `coordinates` and `formula_terms` attributes are rewritten token by token so the
  hybrid-height formula resolves instead of pointing at names that no longer exist.

## Ocean curvilinear grids and the MOM supergrid

### Why the grid is curvilinear

ACCESS-OM ocean configurations use MOM's tripolar grid: to avoid a coordinate
singularity in the open Arctic Ocean, the grid's northern poles are displaced onto
land. As a result latitude and longitude are two-dimensional functions of the
logical grid indices, and CMIP output must carry 2-D auxiliary coordinates
(`latitude(j, i)`, `longitude(j, i)`) alongside plain integer index dimensions
`i` and `j`.

### Reconstructing geometry from the supergrid

Raw MOM output contains cell-centre coordinates but not the cell corners CMIP
requires. The `Supergrid` class (`src/access_moppy/ocean_supergrid.py`) loads the
MOM *supergrid* — a grid at twice the model resolution whose nodes contain every
cell centre, edge midpoint, and corner. The file is selected by the model's CMIP
`nominal_resolution` (`100 km → mom1deg.nc`, `25 km → mom025deg.nc`,
`10 km → mom01deg.nc`), read from the shared Gadi data area when available and
downloaded otherwise. From it, `extract_grid` derives centres and the four cell
corners for T, U, V, and C cells (following the C-grid conventions in
[Adcroft's notes](https://gist.github.com/adcroft/c1e207024fe1189b43dddc5f1fe7dd6c)),
extending the arrays across the tripolar fold and the periodic east–west boundary
so corner cells are well defined. The result is returned as `latitude`,
`longitude`, `vertices_latitude` and `vertices_longitude` arrays with a
4-element `vertices` dimension — the CMIP representation of curvilinear cell
bounds.

### Grid staggering and model generations

`Ocean_CMORiser` (`src/access_moppy/ocean.py`) has two concrete subclasses because
the two ocean models stagger variables differently:

- `Ocean_CMORiser_OM2` — MOM5 (ACCESS-OM2, ESM1.5/1.6, CM2) uses an Arakawa
  **B-grid**; the T/U/V/C cell type is inferred from which native coordinates are
  present (`xt_ocean`/`yt_ocean` vs `xu_ocean`/`yu_ocean`).
- `Ocean_CMORiser_OM3` — MOM6 (ACCESS-OM3) uses an Arakawa **C-grid** (with
  symmetric memory mode assumed); cell type is inferred from `xh`/`yh` vs
  `xq`/`yq`.

In both cases the native dimensions are renamed to the CMIP `i`/`j`/`lev` names,
the appropriate supergrid cells are attached as coordinates and bounds, and the
data variable's `coordinates` attribute is re-pointed at `latitude longitude`
(the model's native `geolon_t geolat_t` references would otherwise dangle in the
output).

### Choosing the grid for `areacella`

The ACCESS atmosphere writes fields on four horizontal points of the same
resolution: the theta (mass) points `lat`/`lon`, and three staggered points
`lat`/`lon_u`, `lat_v`/`lon`, `lat_v`/`lon_u`. CMIP7 registers a separate grid
label for each (`g115`, `g109`, `g108`, `g110` for ACCESS-ESM1.6), and twelve
ESM1.6 variables sit on a staggered point rather than on theta — `ta`, `ua`,
`va`, `zg`, `hus`, `hur`, `wap` and `zg500` on `lat_v`/`lon_u`, `uas` and `tauu`
on `lat`/`lon_u`, `vas` and `tauv` on `lat_v`/`lon`.

`areacella` is computed from the grid rather than read from the model, so which
of the four points it describes is a choice the caller makes. A cell measure is
only usable by the fields written on the same point: attaching the 145-row theta
measure to the 144-row `ta` is a shape mismatch, and downstream tools reject it.

Name the field the measure has to match:

```python
ACCESS_ESM_CMORiser(
    compound_name="atmos.areacella.ti-u-hxy-u.fx.glb",
    match_variable="ta",        # same cells as ta -> 144 rows, label g110
    ...,
)
```

`match_variable` resolves the grid label as well, so it does not have to be
supplied separately. An explicit `grid_label` works too and is read back through
the model's own `cmip7_grid_labels`, so `grid_label="g110"` selects the same
point; the two are rejected if they contradict each other. With neither, the
theta points are used, which is what every release before this one produced.

This is a CMIP7 feature: it is the grid label that tells the four points apart,
and only CMIP7 registers one per point. CMIP6 publishes them all under `gn`, so
one dataset can hold only one `areacella`, and the CMIP6 table entry asks for
"areas that apply to surface vertical fluxes of energy" — the mass points.
CMIP6 output therefore keeps the theta grid unchanged, and passing
`match_variable` with `cmip_version="CMIP6"` is rejected rather than ignored.

### Implications for `grid_label`

Because no regridding is performed, ocean (and atmosphere) output is published on
the model's native grid, which corresponds to `grid_label = "gn"` — the default
applied by the CMORiser driver for CMIP6/CMIP6Plus (CMIP7 defaults to `g999`
pending final CV values). A curvilinear `gn` dataset tells downstream users that
cell geometry lives in the 2-D auxiliary coordinates and vertices, not in the
`i`/`j` index dimensions.

## Related pages

- {doc}`/tutorials/getting_started` — first CMORisation run
- {doc}`/reference/mapping_reference` — how model variables map to CMOR variables
- {doc}`/development/compliance_testing` — the WCRP/CF checks these fixes satisfy
- {doc}`/howto/troubleshooting` — symptoms of coordinate and grid problems

# Issue #772: calculation check with Gadi data

Checked on 2 November 2026 (Australia/Brisbane). The initial analysis below
has now been followed by a production implementation and written-output
tests, described in [Implementation and validation](#implementation-and-validation).
Gadi archive files were only read; none were changed.

**The proposed area calculation reproduces APP4 on real ESM1-6 data. The
analogous mass calculation reproduces APP4 arithmetic but is not equivalent
to CICE5's native mass diagnostic.** These are separate conclusions.

## Data and scope

- ACCESS-ESM1-6 historical, r1i1p1f1, January–December 1850, grid 300 × 360.
- Published model archive: `/g/data/jq44/access-nri/access-esm1p6/global/historical/r1i1p1f1/2026.06.12/output000/ice/`.
- Corresponding raw output: `/g/data/p73/archive/CMIP7/ACCESS-ESM1-6/production/ensemble-concentrations-historical/historical-historical-1.1-r1i1p1f1-01409bc3/historical-historical-1.1-r1i1p1f1-01409bc3/output000/ice/`.
- Monthly fields: `aice`, `siu`, `siv`, `simass`, `sisnmass`, `sivol`,
  `sidmasstranx`, `sidmasstrany`. All eight fields have identical decoded
  values and missing masks in jq44 and p73 across the full sampled year.
- Daily p73 fields for January and September, restricted to the Arctic
  window covering the four straits, were used for the averaging check.
- APP4 grid: `/g/data/p66/CMIP6/APP_ancils/cice_grid_20101208.nc`.
- Both input manifests identify the model grid as
  `/g/data/vk83/configurations/inputs/access-esm1p6/modern/share/ice/grids/global.1deg/2025.07.29/grid.nc`.
- The archived executable manifest identifies CICE5 release `2026.01.000`.

Exact paths, file sizes/timestamps, grid hashes, executable identification,
source references, and extraction-sample hash are recorded in
[provenance.json](provenance.json). The 21.2 MiB extracted NPZ is retained
locally at `/tmp/issue772_gadi_sample.npz`; it is not committed or permanently
bundled with this report.

The initial calculation check covers one year and one member. Its reference
is APP4's original calculation functions run against these ESM1-6 inputs,
not a published APP4 CMIP6 dataset. The implementation validation below adds
one ESM1-5 month and complete NetCDF output tests; other grids remain unsupported.

## Area calculation: passes APP4 comparison

The tested calculation is

```text
Tx = aice × siu × (hue / 100)
Ty = aice × siv × (hun / 100)
```

Here `aice` is a fraction, velocities are m/s, and the original `hue/hun`
are in cm. Summing the APP4 index segments gives m²/s. The original APP4
functions were extracted unchanged from `app_functions.py`; their `cdms2`
grid-file access was replaced with the actual sampled grid arrays.

The xarray prototype and APP4 give **exactly identical results for all
48 monthly strait values** (maximum absolute difference: 0 m²/s). APP4
uses zero-based inclusive segment endpoints; the port preserves those
endpoints and the native component signs.

Day-weighted annual mean area transport:

| Strait | m²/s |
|---|---:|
| Fram Strait | -36,853.49 |
| Canadian Archipelago | -591.99 |
| Barents Opening | -18.77 |
| Bering Strait | +4,282.77 |

Signs follow APP4's signed component sums, rather than a newly imposed
common import/export convention. Monthly values are in
[monthly_transports.csv](monthly_transports.csv).

## Grid and spatial alignment

The model input grid's `htn/hte` are bit-for-bit equal to APP4's `htn/hte`.
The U coordinates differ by less than 3.8e-8 radians between ancillary
files. APP4 coordinates converted to degrees agree with the history-file
coordinates to within 1.74e-5 degrees over the full grid. This supports
using the original ancillary metrics for this specific model grid.

The current model grid lacks `hun/hue`; they are available in the APP4
ancillary. `HTE/HTN` should not be substituted for them: these describe
different cell edges. Every selected APP4 index is within the domain and
over a wet T cell. The segments contain 18, 9, 21, and 2 points respectively.
[strait_cells.csv](strait_cells.csv) records every index, component,
latitude/longitude, mask value, and edge length.

![APP4 indices on the actual model grid](strait_grid_locations.png)

A four-neighbor T-to-U concentration average was also evaluated strictly as
a sensitivity experiment, with land treated as zero. It changes the annual
Fram area flux from -36,853.49 to -37,349.76 m²/s and the small Barents flux
from -18.77 to -41.48 m²/s. It is not an established correction. Adding this
interpolation alone would change APP4's method and break the matching-index
weighting relationship described below.

## Temporal averaging: the original warning needs qualification

For this CICE5 release, `siu/siv` are **ice-area-weighted** velocities.
The model accumulates `aice*uvel` and `aice*vvel`, then divides by accumulated
`aice`. Thus, away from the ice-presence threshold and masking,

```text
mean(aice) × reported_siu = mean(aice × instantaneous_uvel)
```

This is why a generic objection about multiplying two ordinary monthly
means does not apply directly to this area reconstruction. See the
[release's velocity accumulation](https://github.com/ACCESS-NRI/cice5/blob/cf5df9d4d26265dc5c79e558e5a67834b51fd38d/source/ice_history.F90#L2280-L2302)
and [normalization](https://github.com/ACCESS-NRI/cice5/blob/cf5df9d4d26265dc5c79e558e5a67834b51fd38d/source/ice_history.F90#L2925-L2982).

January and September daily reconstructions, averaged using the actual time
bounds, agree with monthly-input reconstruction to within **0.0018442%**
for nonzero strait values. The largest absolute difference is 0.18457 m²/s
(Fram, September); the January Fram difference is 0.015625 m²/s. Zero cases
are explicitly retained without dividing by zero. The remaining differences
can include finite precision and the different daily/monthly ice-presence
masks; this test does not isolate their individual contributions.

[daily_vs_monthly.csv](daily_vs_monthly.csv) gives all eight comparisons.
Daily reconstruction is a consistency check, not an independent measurement
of true strait transport.

![Area transport comparison](area_transport_comparison.png)

## Mass calculation: APP4 compatibility is not native-diagnostic equivalence

Using `simass` directly as kg/m² in `simass × velocity × edge_length`
matches APP4's ice-mass arithmetic to float32 precision: maximum difference
4 kg/s, or 4.82e-8 of the largest reference magnitude. For this comparison
only, APP4's hard-coded density was canceled by passing `simass/900` to its
`icelineTransports` function. This adapter is not a physical density choice.

The actual sampled `simass/sivol` ratio is **917 kg/m³** within float32
rounding. A production implementation should use `simass` directly; it
should not copy APP4's 900 kg/m³ assumption or multiply `simass` by `aice`
again. `sisnmass` is already kg/m² too.

The native `sidmasstranx/y` metadata explicitly includes both ice and snow.
Summing those diagnostics over the same index segments gives the following
comparison with the reconstructed ice-plus-snow total. This compares two
different discretizations; the percentage is a discrepancy, not an estimate
of the reconstruction's error against physical truth.

| Strait | APP4-style ice + snow, kg/s | Native combined, kg/s | Difference / absolute native |
|---|---:|---:|---:|
| Fram Strait | -46,988,708 | -52,667,744 | +10.78% |
| Canadian Archipelago | -756,953 | -580,327 | -30.44% |
| Barents Opening | -28,736 | -58,196 | +50.62% |
| Bering Strait | +5,932,603 | +5,390,987 | +10.05% |

These are day-weighted annual signed means. Cancellation and small net flux
can magnify relative percentages, particularly at Barents.

The [CICE5 release's mass diagnostic](https://github.com/ACCESS-NRI/cice5/blob/cf5df9d4d26265dc5c79e558e5a67834b51fd38d/source/ice_history.F90#L2318-L2346)
uses T-cell face lengths and neighboring values at each model timestep:

```text
Mx = 0.25 × HTE × (mass[i,j] + mass[i+1,j])
                  × (u[i,j-1] + u[i,j])
My = 0.25 × HTN × (mass[i,j] + mass[i,j+1])
                  × (v[i-1,j] + v[i,j])
```

Here `mass` includes ice and snow; ice-presence gating also applies. This
differs from APP4's indexed product with `hue/hun`. Furthermore, reported
velocities are weighted by ice area, not by ice mass, so the area-flux
averaging identity does not supply an equivalent mass-flux identity.
The native combined diagnostic cannot directly supply ice-only transport.

## Consequences for the proposed fix

1. Proceed with the APP4-compatible **area** implementation for this grid:
   `aice`, `siu`, `siv`, verified `hue/hun`, the original segments, and output
   `(time, line)` with named straits. Document the weighted-velocity semantics.
2. Keep grid identification and ancillary provenance explicit. Do not
   silently substitute T-cell metrics or introduce concentration
   interpolation into the compatibility calculation.
3. Treat **mass** as a separate methodological decision. An APP4-compatible
   approximation using `simass` is reproducible, but this check does not
   justify treating it as equivalent to the native face-flux diagnostic.
   Exact separate ice/snow native transports would require suitable
   separately accumulated diagnostics or sufficient timestep information.
4. Follow with a full integration test for `line` metadata and strings.
   This is now implemented and tested as described below.

## Implementation and validation

Both `siareaacrossline` and `simassacrossline` are implemented and registered
as `calc_line_transport`. ESM1-6 uses `aice` or `simass` with `siu/siv`;
ESM1-5 uses `aice` or `hi × 900 kg m⁻³` with `uvel/vvel`, preserving APP4's
legacy density convention. There is no second concentration or density
factor for ESM1-6 `simass`. The mass output explicitly records that it is
an ice-only reconstruction that approximates the time-mean mass flux.

The 50 required ancillary points are bundled with source provenance.
The calculation verifies the full grid shape and all selected T/U
coordinates before applying fixed indices. The sea-ice pipeline now handles
the generated `(time, line)` variable, preserves time bounds, writes the
four character strait labels, and removes gridded coordinates/cell measures.

Validation completed:

- **457 tests passed** on the PR branch based on `main` at `24d12577`,
  including 22 new numerical and pipeline cases (451 passed before rebasing).
  These cover both mappings, CMIP6/CMIP7 writes, file and xarray inputs,
  eager/lazy calculation, units, grid rejection, time alignment, flow signs,
  missing values, and ice-density handling. Ruff and `git diff --check` pass.
- The actual file-loading/writing path was run on the retained ESM1-6
  1850 sample and ESM1-5 December 1860 from
  `/g/data/p73/archive/CMIP6/ACCESS-ESM1-5/HI-05/history/ice/iceh.1860-12.nc`.
  Reconstructed input NetCDFs preserve the sampled values and relevant
  source metadata. These are local validation outputs; the illustrative
  ESM1-5 parent metadata is not a provenance approval for publication.
- All six output files agree with the original APP4 calculation to
  float32 rounding. Maximum absolute differences are shown below. The
  largest difference divided by the largest reference magnitude is
  **1.14 × 10⁻⁷ or less**. The vectorised production summation need not be
  bit-for-bit identical to the earlier prototype's accumulation order.
- Both ESM1-5 CMIP6 outputs pass **PrePARE: 2 files scanned, 0 errors**
  against the bundled `CMIP6_SImon.json` table.
- Both ESM1-6 CMIP7 outputs pass `cmip7repack` and range checks and have
  **no high-priority failures** in `wcrp_cmip7:1.0`.

| Sample | Formats checked | Area maximum difference (m²/s) | Mass maximum difference (kg/s) |
|---|---|---:|---:|
| ESM1-6, 12 months | CMIP6 and CMIP7 | 0.0078125 | 8 |
| ESM1-5, 1 month | CMIP6 | 0.00390625 | 4 |

These are not unconditional compliance passes. The WCRP checker reports a
medium-priority missing `cell_measures` attribute for reduced line variables,
although the CMOR table specifies no cell measure. The CF checker raises an
exception in `check_geographic_region` while interpreting the array of four
character labels (`expected str instance, numpy.ndarray found`), so that
check is incomplete. It also flags the CMOR `line(line, string_length)` naming
and, for CMIP7, using its CF-1.11 suite on a CF-1.12 file. The unit and real-data
tests independently verify the four canonical strait labels and their encoding.
The CMIP6 WCRP registry additionally reports table/registry disagreements;
ESM1-6 is also absent from its CMIP6 model CV. Checker failures and exit
codes are retained in [implementation_validation.json](implementation_validation.json).
Do not interpret these results as scientific approval of the mass approximation
or as a clean CF checker run.

The reproduction script is [verify_implementation.py](verify_implementation.py).
After extracting the modern sample and downloading APP4 as below, extract
the legacy sample and run:

```bash
ssh -x -F ~/.ssh/config -o BatchMode=yes -o ClearAllForwardings=yes gadi_rb5533 \
  'module use /opt/Modules/modulefiles; module load conda/analysis3-26.10; python -' \
  < reports/issue_772_transport_check/extract_esm15.py \
  > /tmp/issue772_esm15_sample.npz

PATH="$PWD/.pixi/envs/dev/bin:$PATH" .pixi/envs/dev/bin/python \
  reports/issue_772_transport_check/verify_implementation.py

PATH="$PWD/.pixi/envs/dev/bin:$PATH" .pixi/envs/dev/bin/python -m pytest \
  tests/unit/test_seaice_line_transport.py tests/unit/test_sea_ice.py \
  tests/unit/test_derivations_calc_seaice.py tests/unit/test_derivations.py \
  tests/unit/test_driver.py tests/unit/test_utilities_mappings.py \
  tests/unit/test_mapping_resolution.py -q --disable-warnings

UDUNITS2_XML_PATH="$PWD/.pixi/envs/dev/share/udunits/udunits2.xml" \
  .pixi/envs/dev/bin/PrePARE \
  --table-path src/access_moppy/vocabularies/cmip6_cmor_tables/Tables/CMIP6_SImon.json \
  --max-processes 1 --hide-progress --no-text-color \
  reports/issue_772_transport_check/outputs/CMIP6/CMIP/CSIRO/ACCESS-ESM1-5/historical/r1i1p1f1/SImon/*/gn/v*/*.nc
```

## Reproduction

The extraction reads model data and grids and writes only a compressed
archive to stdout. It does not submit a job or write anything on Gadi.
The module-path addition below fixes the missing `singularity` module in
this account's noninteractive SSH environment without changing its files.

```bash
ssh -x -F ~/.ssh/config -o BatchMode=yes -o ClearAllForwardings=yes gadi_rb5533 \
  'module use /opt/Modules/modulefiles; module load conda/analysis3-26.10; OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 python -' \
  < reports/issue_772_transport_check/extract_gadi.py \
  > /tmp/issue772_gadi_sample.npz

curl -fsSL https://raw.githubusercontent.com/ACCESS-Community-Hub/APP4/master/subroutines/app_functions.py \
  -o /tmp/issue772-app_functions.py

MPLCONFIGDIR=/tmp/issue772-mpl .pixi/envs/dev/bin/python \
  reports/issue_772_transport_check/check_transport.py \
  /tmp/issue772_gadi_sample.npz /tmp/issue772-app_functions.py
```

Check the reference-source hash in `provenance.json` when reproducing: the
download URL uses APP4's master branch. Numerical output is saved in
[results.json](results.json), with the complete monthly and daily tables
linked above.

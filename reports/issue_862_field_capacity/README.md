# ACCESS-ESM1.6 soil field capacity

The previous `mrsofc` resource contains the depth of water at the wilting
point, labelled with mass-per-area units. Its 10,865 land values exactly equal
4.6 m times STASH `m01s00i040` in the archived production restart. Every
published file in the 138-file November 2026 inventory agrees with that
resource after conversion to the file's stored dtype. [Issue #862](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/862) · [Original resource PR](https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/246)

The UM7 `2026.02.000` source passes the STASH 41 critical-point volumetric
fraction to CABLE's field-capacity parameter, `soil%sfc`, for soil-bearing
tiles. The same fraction applies to each of the six soil layers. Multiplying
their total thickness, 4.6 m, by the fraction gives the equivalent water depth.
Multiplying that depth by CABLE's water density, 1000 kg m⁻³, gives kg m⁻².
[UM7 soil initialisation](https://github.com/ACCESS-NRI/UM7/blob/a1fd578b49602919903a9f68ab06d71e68524369/umbase_hg3/src/atmosphere/land_surface/cable/cbl_um_init_soil.F90#L112-L174) · [UM7 layer thicknesses](https://github.com/ACCESS-NRI/UM7/blob/a1fd578b49602919903a9f68ab06d71e68524369/umbase_hg3/src/include/constant/soil_thick.h#L1-L7) · [CABLE water density](https://github.com/CABLE-LSM/CABLE/blob/15f639dd33dfb15819304332d72c2b405b51b85e/src/params/cable_phys_constants_mod.F90#L39-L44)

The 3,434 land cells with zero STASH 41 coincide with the fully glaciated
cells. Keep these soil-capacity zeros and the 16,975 ocean missing values.
On lake tiles, CABLE applies `soil%sfc` to the underlying soil and tracks
lake-water input separately. The resource represents the soil parameter on
those tiles and contains no lake-water reservoir. This follows the CMIP7
land-area definition. [UM7 lake treatment](https://github.com/ACCESS-NRI/UM7/blob/a1fd578b49602919903a9f68ab06d71e68524369/umbase_hg3/src/atmosphere/land_surface/cable/cbl_um_update_soilsnow.F90#L24-L36) · [CMIP7 variable definition](https://cmip-data-request.github.io/cmip7-dreq-webview/v1.2.2.5/u/bab1c08c-e5dd-11e5-8482-ac72891c3257.html)

The corrected resource ranges from 0 to 1766.021525742412 kg m⁻². The
ACCESS-ESM1.6 mapping loads its own resource so the existing ACCESS-ESM1.5
mapping can continue to resolve its original file.

## Regenerate on Gadi

Run from the repository root. The existing resource supplies the grid and
bounds. The replacement values come from the archived restart. The script
requires Iris, netCDF4 and NumPy and writes to a new file.

```bash
module use /opt/Modules/modulefiles /apps/Modules/modulefiles
module load conda/analysis3-26.11
restart=/g/data/p73/archive/CMIP7/ACCESS-ESM1-6/production/Ndep2-PI-CNP-concentrations/restart000/atmosphere/restart_dump.astart
python reports/issue_862_field_capacity/generate_resource.py "$restart" \
    src/access_moppy/resources/fx.mrsofc_ACCESS-ESM.nc \
    /path/to/new/fx.mrsofc_ACCESS-ESM1-6.nc
```

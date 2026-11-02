# ACCESS-ESM1 strait grid

`ACCESS-ESM1_transport_lines.json` contains the 50 grid points needed for
APP4's Fram Strait, Canadian Archipelago, Barents Opening, and Bering Strait
diagnostics. Indices are zero based on the ACCESS-ESM1 300 × 360 CICE grid.
Positive contributions follow the native x/y velocity components.

The edge lengths and T/U coordinates were extracted from
`/g/data/p66/CMIP6/APP_ancils/cice_grid_20101208.nc`. Its SHA-256 is stored
in the JSON. `hue` (x transport) and `hun` (y transport) were converted
from centimetres to metres; latitudes/longitudes from radians to degrees.
Only the points used by the straits are bundled, not the full grid.

The lines follow `icearealineTransports`, `icelineTransports`, and
`transAcrossLine` in [APP4](https://github.com/ACCESS-Community-Hub/APP4/blob/master/subroutines/app_functions.py),
originally developed by Peter Uhe and Chloe Mackallah. These definitions
are shared by the ACCESS-ESM1-5 and ACCESS-ESM1-6 mappings.

The ESM1-6 grid recorded in the sampled run's input manifest,
`/g/data/vk83/configurations/inputs/access-esm1p6/modern/share/ice/grids/global.1deg/2025.07.29/grid.nc`,
has identical `htn/hte` and matching coordinates. Its T-cell edge lengths
must not be substituted for the U-cell `hue/hun` stored here. The derivation
checks the input T/U coordinates at every selected point, with tolerance
0.0001 degree to allow float32 history coordinates, and requires the full
300 × 360 grid shape. It also accepts longitude wrapping and radian units.

To regenerate the numbers, read each selected `(j, i)` from the original
ancillary, use `hue[j,i]/100` for an x point or `hun[j,i]/100` for a y point,
and convert `tlat/tlon/ulat/ulon[j,i]` with `numpy.rad2deg`. Keep inclusive
APP4 segment endpoints and their order. The source checksum and reference
coordinates must be updated together if a new ancillary is introduced.

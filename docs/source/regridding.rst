Optional variable-aware regridding
==================================

ACCESS-MOPPy writes native-grid CMIP-style output by default.  Optional
regridding is intended for evaluation and analysis workflows such as ILAMB,
ESMValTool and REF, where a common comparison grid is useful.  It should not be
applied blindly to all variables for publication outputs.

Design
------

The regridding hook runs after the CMOR variable has been derived on the native
grid and after metadata, units and time handling have been applied.  It then:

* selects a method per variable;
* reuses a cached ESMF/xESMF weight file when present;
* otherwise generates weights with xESMF when the optional dependency is
  installed;
* updates ``grid_label``, regular ``lat``/``lon`` coordinates and bounds; and
* removes stale native-grid ``cell_measures`` metadata.

Cached weights are reusable across variables, years, experiments and batch jobs
when the source grid, target grid, method and mask policy are identical.  Weight
files are normally too large and grid-version-specific to ship inside the Python
package, so the recommended location is a shared data area.

Configuration
-------------

Example batch configuration::

   regrid:
     enabled: true
     target_grid: cmip7-1x1
     grid_label: gr
     method: auto
     weights:
       mode: reuse_or_create
       cache_dir: /g/data/xp65/public/apps/moppy/regrid_weights
       mask_policy: nomask
     variable_methods:
       pr: conservative
       tos: bilinear
       sftlf: nearest_s2d
     variable_classes:
       uo: vector

``method: auto`` uses a deliberately conservative first-pass policy:

* flux-like and extensive quantities use ``conservative``;
* smooth scalar state variables use ``bilinear``;
* masks and categorical fields use ``nearest_s2d``; and
* vector/staggered-grid fields are refused unless explicitly handled by a
  vector-aware workflow.

Weights command
---------------

Weights can also be generated explicitly before CMORisation::

   moppy-regrid-weights create \
     --source-grid ACCESS-ESM1-6-ocean-native-grid.nc \
     --target-grid cmip7-1x1 \
     --method conservative \
     --output ACCESS-ESM1-6_gn_to_cmip7-1x1_conservative_nomask_a82f13.nc

Conservative regridding requires valid source cell bounds/corners, either
``lat_bnds``/``lon_bnds`` on rectilinear grids or
``vertices_latitude``/``vertices_longitude`` on curvilinear grids.  Missing
bounds produce a clear error rather than silently generating unsafe weights.

Limitations
-----------

This is a first pass.  Applying existing sparse weight files is intentionally
lightweight and testable without ESMF, but generating weights still requires the
optional ``xesmf``/ESMF stack.  Vector rotation, staggered-grid location-aware
interpolation, bounded fraction methods and richer target-grid registries remain
future work.

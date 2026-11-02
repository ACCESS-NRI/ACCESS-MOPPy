# CABLE provenance for issue 817 / PR 819

The CABLE release cited by PR819 is correct for the retained ACCESS-ESM1.6 experiment: **CABLE 2025.11.000**, commit `15f639dd33dfb15819304332d72c2b405b51b85e`, built with `library=access-esm1.6`. The soil geometry is also confirmed in the coupled model path. This follow-up strengthens the original offline-source citation; it changes no numerical code, constants, output metadata or scientific results.

## Trace from the retained inputs

The native soil inputs identify experiment UUID `e523e199-80f6-4ca6-b84a-e513a16f2029`, base configuration `archive-piControl-2026.04.07`, and run revision `68129eb890d27f2b66b14e01e7e466c2c29f3277` in `ACCESS-NRI/access-esm1.6-experiments`.

1. That exact revision's [config.yaml](https://github.com/ACCESS-NRI/access-esm1.6-experiments/blob/68129eb890d27f2b66b14e01e7e466c2c29f3277/config.yaml#L33) loads `access-esm1p6/2026.02.001`. Its [executable manifest](https://github.com/ACCESS-NRI/access-esm1.6-experiments/blob/68129eb890d27f2b66b14e01e7e466c2c29f3277/manifests/exe.yaml#L4) identifies UM7 `2026.02.000` and records the executable checksum. The retained inputs have six numbered soil layers.
2. ACCESS-ESM1.6 release `2026.02.001`, commit `0057628f5d07cd1804568abe0030e3cc6fa322a4`, [pins UM7 2026.02.000 and CABLE 2025.11.000 with the ACCESS-ESM1.6 library target](https://github.com/ACCESS-NRI/ACCESS-ESM1.6/blob/0057628f5d07cd1804568abe0030e3cc6fa322a4/spack.yaml#L28).
3. [CABLE's matching build definition](https://github.com/CABLE-LSM/CABLE/blob/15f639dd33dfb15819304332d72c2b405b51b85e/CMakeLists.txt#L124) selects its coupled ESM interface. Its [shared soil type](https://github.com/CABLE-LSM/CABLE/blob/15f639dd33dfb15819304332d72c2b405b51b85e/src/coupled/shared/cable_soil_type_mod.F90#L38) defines `zse` as layer thickness in metres.
4. In the pinned UM7 `2026.02.000` source, commit `a1fd578b49602919903a9f68ab06d71e68524369`, the coupled initializer takes these thicknesses from UM7's `DZSOIL`. Its CABLE six-layer definition matches the values below. Authorized UM7 readers can inspect [soil_thick.h](https://github.com/ACCESS-NRI/UM7/blob/a1fd578b49602919903a9f68ab06d71e68524369/umbase_hg3/src/include/constant/soil_thick.h#L3) and [cbl_um_init_soil.F90](https://github.com/ACCESS-NRI/UM7/blob/a1fd578b49602919903a9f68ab06d71e68524369/umbase_hg3/src/atmosphere/land_surface/cable/cbl_um_init_soil.F90#L70). These restricted source files have not been copied into this public report.

The current ACCESS-ESM1.6 main branch also pins CABLE 2025.11.000, but the archived experiment's release above is the relevant provenance. A current build pin alone would not establish what produced these historical inputs. The NetCDF files do not contain a dedicated CABLE-version attribute; this conclusion follows their recorded run revision through the configuration and release manifest. It is not a claim to have inspected the original executable binary on NCI.

## Verified geometry and implications

| Layer | Thickness (m) | Bounds (m) | Centre (m) |
| --- | --- | --- | --- |
| 1 | 0.022 | 0–0.022 | 0.011 |
| 2 | 0.058 | 0.022–0.080 | 0.051 |
| 3 | 0.154 | 0.080–0.234 | 0.157 |
| 4 | 0.409 | 0.234–0.643 | 0.4385 |
| 5 | 1.085 | 0.643–1.728 | 1.1855 |
| 6 | 2.872 | 1.728–4.600 | 3.164 |

These are exactly the interfaces used by PR819. The [original public offline CABLE citation](https://github.com/CABLE-LSM/CABLE/blob/15f639dd33dfb15819304332d72c2b405b51b85e/src/offline/cable_parameters.F90#L1240) independently agrees, but the coupled initialization is the stronger basis for ACCESS-ESM1.6. The lower boundary of layer 2 is 0.080 m, so the top-10 cm calculation remains `layer1 + layer2 + (0.020 / 0.154) * layer3`, assuming uniform content within the fractional layer. The final layer centre remains 3.164 m.

`verification.json` records the native-input identifiers, release commits, source hashes and equality checks. The executable Python syntax tree is unchanged by the source-comment correction, so the previously regenerated four full-year products and numerical validation remain applicable. No new output regeneration is claimed by this provenance check. This establishes the retained experiment's configuration; it does not establish geometry for every other CABLE configuration or an independently modified future ESM1.6 build.

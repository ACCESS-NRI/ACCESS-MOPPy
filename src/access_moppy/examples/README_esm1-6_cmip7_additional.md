# ACCESS-ESM1.6 CMIP7 release batches

The `batch_config_esm1-6_cmip7_<configuration>_additional.yml` files provide candidates **beyond** the [generic ACCESS-ESM1.6 baseline example](batch_config_esm1-6_cmip7_baseline.yml). There is one file for every ACCESS-NRI [released configuration](https://github.com/ACCESS-NRI/access-esm1.6-configs) available on 24 October 2026. Each file header records the exact release tag and selected atmosphere, ocean, and sea-ice diagnostic profiles.

| Configuration | Release tag | Additional candidates |
| --- | --- | ---: |
| `piControl` | `release-piControl-2.1` | 115 |
| `esm-piControl` | `release-esm-piControl-2.1` | 115 |
| `historical` | `release-historical-1.3` | 126 |
| `esm-historical` | `release-esm-historical-1.3` | 127 |
| `1pctCO2` | `release-1pctCO2-1.0` | 107 |
| `1pctCO2-bgc` | `release-1pctCO2-bgc-1.0` | 132 |
| `1pctCO2-rad` | `release-1pctCO2-rad-1.0` | 132 |
| `abrupt-4xCO2` | `release-abrupt-4xCO2-1.0` | 107 |
| `abrupt-2xCO2` | `release-abrupt-2xCO2-2.0` | 118 |
| `abrupt-0p5xCO2` | `release-abrupt-0p5xCO2-2.0` | 118 |
| `scen7-h` | `release-scen7-h-1.0` | 124 |
| `scen7-vl` | `release-scen7-vl-1.0` | 124 |
| `esm-scen7-h` | `release-esm-scen7-h-1.0` | 125 |
| `esm-scen7-vl` | `release-esm-scen7-vl-1.0` | 125 |
| `esm-flat10` | `release-esm-flat10-1.0` | 126 |
| `esm-flat10-cdr` | `release-esm-flat10-cdr-1.0` | 126 |
| `esm-flat10-zec` | `release-esm-flat10-zec-1.0` | 126 |
| `amip` | `release-amip-1.0` | 43 |

Selection used CMIP7 Data Request `v1.2.2.5` (all priorities and opportunities) and the ACCESS-ESM1.6 mappings in this repository. Each candidate is requested for its experiment, is outside the generic baseline example, has every mapped native input in the selected STASH, MOM5, or CICE profile at the requested cadence, and has a built-in MOPPy file search. Known pressure/model-level, climatology, depth-selection, and regional-subset mapping mismatches were excluded where relevant. Native fields requiring aggregation, missing outputs, and tables without MOPPy discovery rules were also excluded.

These are **pilot templates**. A released configuration's output definition does not establish which tag an archived ensemble member used, or prove that the files exist and CMORise successfully. Confirm the run's tag, set `input_folder`, `output_folder`, PBS project, member label, and parent lineage as applicable, then run a one-year pilot and QC before scheduling the whole archive. MOPPy selects `activity_id` from the CMIP7 experiment vocabulary. The commented `start_year` and `end_year` entries can bound that pilot to an available model year. `esm-historical` uses CMIP7 experiment ID `esm-hist` in its YAML.

The 1pctCO2 and abrupt-4xCO2 releases select subdaily atmosphere, WombatLite ocean, and CMIP high ice profiles; piControl and historical select standard profiles. The selected profiles, rather than just the experiment name, determine which output can be CMORised from an archive.

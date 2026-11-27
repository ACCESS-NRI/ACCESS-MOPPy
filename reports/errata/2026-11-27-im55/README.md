# ACCESS-ESM1-6 im55 errata drafts — 27 November 2026

These drafts accompany an audit of the current local CMIP7 publication tree.
They are not registered ESGF errata and do not authorize or perform withdrawal.
The source inventory covers 19,653 latest dataset versions and 509,612 file
references; targeted numerical screening covers 287 variable products.

Each directory contains a plain-language explanation, formal JSON draft,
dataset/version list and the complete immutable file paths for that scope.
Files belonging to an affected release are listed even when only representative
files were numerically tested. H05 lists only datasets with observed negative
precipitation; broader incidence requires a complete-record scan. P01–P03 are
unconfirmed investigations and their candidate lists are not withdrawal lists.

Code repair and publication repair are separate. Existing fixes for topsoil,
snow, tsl, mrso and prc/prsn must not be mistaken for regenerated publications.
No public data, ESGF records or model source inputs were changed.

| ID | Recommended severity | Dataset versions | File references | Draft |
|---|---|---:|---:|---|
| C01 | Critical | 6 | 301 | [Explanation](C01/README.md) |
| C02 | Critical | 58 | 1,141 | [Explanation](C02/README.md) |
| C03 | Critical | 48 | 1,081 | [Explanation](C03/README.md) |
| H01 | High | 422 | 6,991 | [Explanation](H01/README.md) |
| H02 | High | 138 | 2,261 | [Explanation](H02/README.md) |
| H03 | High | 138 | 138 | [Explanation](H03/README.md) |
| H04 | High | 312 | 5,735 | [Explanation](H04/README.md) |
| M01 | Medium | 16 | 1,171 | [Explanation](M01/README.md) |
| H05 | High | 62 | 3,123 | [Explanation](H05/README.md) |
| P01 | Potentially Critical | 138 | 138 | [Explanation](P01/README.md) |
| P02 | Unclassified pending precision analysis | 14 | 643 | [Explanation](P02/README.md) |
| P03 | Potentially High for geometry; Medium for grid metadata | 28 | 994 | [Explanation](P03/README.md) |

The SHA-256 hashes in `index.json` cover each exact newline-delimited file list.

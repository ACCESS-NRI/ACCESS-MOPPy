# ACCESS-ESM1-6 im55 errata drafts — 27 November 2026

These are preparation documents, not registered ESGF errata. No public files or ESGF records were changed. The initial audit inventoried 19,653 current dataset versions and 509,612 file references, with targeted numerical screening of 287 products. This revision incorporates native-data investigations, fresh Gadi writer checks and independent reviews.

Four Critical families (C01–C03 and P01) cover 250 dataset versions / 2,661 files. The four primary High families H01–H04 cover 1,010 versions / 15,125 files. H05 adds 62 versions / 3,123 files with observed tiny negative precipitation; its formal value-change severity is High but scientific impact is low. These nine confirmed scopes total 1,322 versions / 20,909 file references. Representative numerical checks establish most release scopes; they are not full-value scans of every listed file.

Follow-up changes to the initial classification:

- **P01 is confirmed Critical:** the resource uses wilting point rather than field capacity and omits water density. Every one of its 138 fixed files was compared over the complete grid. A uniform ×1,000 correction is insufficient.
- **M01 is withdrawn:** current CMIP7 guidance permits the verified parent-grid labels. Its 1,171-file inventory is retained as `reviewed-files.txt`, not an erratum list.
- **P02 is a two-file precision caution:** the negative derived variance is already compatible with rounding in archived moments. The 643-file candidate list remains unverified, and broader severity is unassigned pending a model replay.
- **P03 remains a scientific investigation:** native rows and Arctic face geometry are verified in six comparison files. An approved latitude-following reference and matching transport inputs are needed before assigning a replacement or claiming archive-wide impact. Parent g116 alone is not an erratum.

Each directory contains a casual explanation and example, development/publication actions, a machine-readable draft, dataset/version inventory and complete immutable paths. `index.json` and the drafts record SHA-256 digests. Inventory counts for unresolved or withdrawn items describe reviewed scope, not affected releases. Extra P02/P03 lists identify the exact checked files.

| ID | Recommended seriousness | Inventory versions | File references | Details |
|---|---|---:|---:|---|
| C01 | Critical | 6 | 301 | [Explanation](C01/README.md) |
| C02 | Critical | 58 | 1,141 | [Explanation](C02/README.md) |
| C03 | Critical | 48 | 1,081 | [Explanation](C03/README.md) |
| H01 | High | 422 | 6,991 | [Explanation](H01/README.md) |
| H02 | High | 138 | 2,261 | [Explanation](H02/README.md) |
| H03 | High | 138 | 138 | [Explanation](H03/README.md) |
| H04 | High | 312 | 5,735 | [Explanation](H04/README.md) |
| M01 | None — accepted transitional convention | 16 | 1,171 | [Explanation](M01/README.md) |
| H05 | High | 62 | 3,123 | [Explanation](H05/README.md) |
| P01 | Critical | 138 | 138 | [Explanation](P01/README.md) |
| P02 | Unassigned pending model replay and broader incidence | 14 | 643 | [Explanation](P02/README.md) |
| P03 | Potentially High for transport geometry; no parent-grid-label erratum | 28 | 994 | [Explanation](P03/README.md) |

Code fixes do not replace old publications. Topsoil, snow and `mrso` corrections already shipped; `tsl` is fixed on main but postdates the checked 1.9.1 release. New source/resource changes and validation-only reports are linked per finding. Published replacements, live ESGF identifier reconciliation and erratum submission remain publication-team work.

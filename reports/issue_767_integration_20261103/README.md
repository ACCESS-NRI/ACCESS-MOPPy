# Issue 767: eight-group integration validation

All 117 piControl year-0101 products were regenerated from the combined changes. All pass processing, structural and range checks. The unchanged beta checker still reports four high-priority text-coordinate failures. The proposed upstream checker resolves the naming prerequisite and leaves one high-priority vocabulary-label failure. No PR was merged.

## Baseline and reproducibility

All eight isolated branches start from clean main `99e671d978ae38a102ddbcce36cacf45c4102c41`. A new full baseline run was generated at that commit: 113 outputs and four nitrogen processing failures. The earlier dirty checkout at `d2c2d88e9da1b2a97c24d8039cce408d693c8773` was preserved and was not used as branch content.

The tested combined source is `c4250da3f79281f2be43d452441db4753fafd60e`. Later commit `b15c5375` adds only a missing newline to a stored XML report; subsequent integration-report commits do not change production code. The 681 source-file fingerprints in `provenance.json` were verified after validation. `assignments.json` records the eight PR branches and their baseline. `preservation.json` verifies that the original dirty diff/status, seven retained reports, all 113 original outputs, and both pinned vocabulary databases remain unchanged.

Validation used Python 3.13.15, cc-plugin-wcrp 2.4.0b2, compliance-checker 6.1.0, ESGVOC 7.2.0, Universe 3.4.3 and CMIP7 2.5.4. Production uses its existing ESGVOC 6.1.0 dependency with the retained current databases in offline mode. The proposed-checker pass uses ESGF/cc-plugin-wcrp PR97 commit `7f392edb25979b761aafbf687906c334feccce48`, loaded separately without changing the stock checker environment or vocabulary.

Fresh outputs and complete raw reports remain under `/home/romain/Documents/ChatGPT/Expand variable MOPPY/issue767-work/validation/`, in `baseline`, `integration`, and `integration-proposed-checker`. None of the old baseline checker reports was reused as a new result. This directory retains aggregate results, findings, hashes, numerical audits and CI evidence; large NetCDF files and full raw checker reports remain local.

## Measured outcomes

Counts below are outputs with at least one finding at each priority; categories overlap.

| Run | Outputs / attempted | Processing failures | High-priority failures | Medium findings | Optional findings |
| --- | --- | --- | --- | --- | --- |
| Fresh clean-main baseline, stock checker | 113 / 117 | 4 | 9 | 64 | 113 |
| Combined source, stock checker | 117 / 117 | 0 | 4 | 48 | 32 |
| Same new files, proposed checker | 117 / 117 | 0 | 1 | 48 | 32 |

- ✅ PASS: all 117 structural and range checks; zero checker execution errors. Stock checker: 113 mandatory passes, 39 strict passes. Proposed checker: 116 mandatory passes, 41 strict passes.
- ✅ PASS: combined smoke/unit suite has 3,255 passed and 22 skipped. The initial sandbox run had one local-socket permission failure; that single Dask test passed with local sockets enabled. Both run outcomes are recorded in `test-results.json`. Ruff 0.8.4 check and format pass on all 19 changed Python files. All eight MOPPy PR heads have successful pre-commit, Test, codecov/patch and codecov/project checks (`pr-ci.json`).
- ✅ PASS: 112 of 113 paired main-variable arrays preserve values and masks exactly. The comparison covers 690,378,956 common valid values. All 793 compared auxiliary arrays are unchanged; all 14 text-coordinate label sets are preserved and correctly associated. Five corrected grid labels and the explicitly unresolved ocean label agree with their filenames, directories and regions.
- ✅ PASS: `mrsos` independently matches the native first two CABLE layers plus `0.020 / 0.154` of the third layer, with zero numerical error. This intentional scientific correction changes 3,965,725 valid values (maximum 24.5193765962 kg m-2) and restores masks on 6,195,875 formerly zero-valued missing cells. The three layered soil products preserve data and masks exactly.
- ✅ PASS: all four nitrogen outputs have 12 months, shape `(12, 145, 192)`, exact calendar-month bounds and exact independently calculated masks. Maximum relative error against independent float64 NumPy calculations is 2.29355e-7, within the derived float32 rounding bound. No prior nitrogen output exists for a baseline comparison.

## Group results and unresolved work

| Group | Issue | PR | Verified behavior | Remaining blocker |
| --- | --- | --- | --- | --- |
| 1: small metadata omissions | [815](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/815) | [818](https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/818) | ✅ PASS: phalf formula and half-level coefficient metadata; rsntds area measure and external reference | ⚠️ Findings: valid phalf units/standard-name metadata remain even where the vocabulary does not prescribe them |
| 2: soil depths | [817](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/817) | [819](https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/819) | ✅ PASS: four products, authoritative depth bounds, scalar 5 cm mrsos coordinate with 0–10 cm bounds and independent numerical checks | None for this scope |
| 3: coordinate descriptions | [822](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/822) | [826, draft](https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/826) | ✅ PASS: latitude/longitude descriptions on 46 outputs; 92 messages removed | ⚠️ Findings: 46 outputs retain truthful i/j descriptions; [CMIP7-CVs915](https://github.com/WCRP-CMIP/CMIP7-CVs/issues/915) addresses misleading unstructured-grid wording |
| 4: string-length dimensions | [828](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/828) | [829](https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/829) | ✅ PASS: all ten original products; shared strlen, unchanged decoded labels and data; integration checks all 14 label coordinates | None for this scope |
| 5: text auxiliary convention | [821](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/821), [checker96](https://github.com/ESGF/cc-plugin-wcrp/issues/96) | [824](https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/824), [checker97, draft](https://github.com/ESGF/cc-plugin-wcrp/pull/97) | ✅ PASS: producer associations and dependent checks now exercised with proposed checker; three of four products clear high checks | ❌ FAIL: stock checker rejects all four names; proposed checker exposes `canadian_archipelego` registry typo. [Universe440](https://github.com/WCRP-CMIP/WCRP-universe/issues/440); convention approval remains upstream |
| 6: reduced grids | [827](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/827) | [830, draft](https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/830) | ✅ PASS: four hemispheric products use g011/g012 and strait output uses g014; scientific regions and arrays preserved | ⚠️ Findings: msftmrho native tripolar rows are not constant-latitude bands. Current g116 count/topology remains unresolved; [checker98](https://github.com/ESGF/cc-plugin-wcrp/issues/98). Rejected g262 candidate was not committed |
| 7: optional comments | [823](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/823) | [825, draft](https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/825) | ✅ PASS: exact branded/compound record supplies 85 applicable descriptions, preserving producer text | ⚠️ Findings: 32 comments withheld for ambiguity, scientific conflict or missing exact record; phalf computed_standard_name remains valid but unverifiable by this vocabulary |
| 8: nitrogen time axes | [816](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/816) | [820](https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/820) | ✅ PASS: four full-year outputs, guarded documented CASA convention, packed/split regression coverage, independent values/masks/bounds checks | Applies only to the established source convention; arbitrary historical offsets are rejected |

The 48 remaining medium-finding outputs comprise 46 with i/j wording recommendations, phalf with valid extra attributes, and msftmrho with unresolved geometry/cell count. There are 49 medium check records because msftmrho has two. The 32 optional-comment outputs include phalf, which also has an additional computed-standard-name recommendation. These are not reported as scientific defects merely because the vocabulary cannot verify them.

## Interactions and run records

The combined source applies groups 1, 2, 8, 3, 5, 7, 4 and 6, including test-only follow-ups. Cherry-picks were conflict-free. Shared writer ownership is split between string encoding (group4), auxiliary associations (group5), comment preservation (group7) and input-time preprocessing (group8). Groups1 and7 change distinct vocabulary-handling paths. Groups6 and7 retain both reduced-grid selection and useful strait reconstruction comments. The integrated coordinate audit specifically checks these interactions.

`baseline-results.json`, `results.json` and `proposed-checker-results.json` are fresh run records. `comparison.json`, `soil-numerical.json`, `nitrogen-native.json` and `coordinate-interactions.json` provide independent data and coordinate evidence. `summary.json` lists every remaining finding. Reproduction harnesses are stored under `harness/`; they refer to the retained native-input paths and isolated checker environment recorded in `provenance.json`. The proposed checker reports contain before/after hashes proving that checking did not edit output files.

The published harness copies have repository formatting applied. Original executed harness hashes remain in `provenance.json`. To reuse the scripts, preserve their recorded workspace layout: the batch/checker/comparison/audit scripts live at the shared work root; `check_soil_numerical.py` lives in the integration run directory. The nitrogen validator accepts explicit native-input, output and report paths.

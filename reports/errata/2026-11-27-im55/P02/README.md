# SST variance is limited by precision in the archived moments

Mean SST and mean squared SST are rounded separately in the native MOM archive. Subtracting two nearly equal numbers to calculate variance can expose that lost precision. The same small negative variance survives CMORisation.

For example: January 0101 piControl gives a minimum published variance of −0.0119001 °C². Taking its square root for SST standard deviation gives an invalid result. That does not establish a large error in the SST mean or second moment themselves.

**Recommended seriousness:** Unassigned pending model replay and broader incidence. **Status:** DRAFT CAUTION — limitation confirmed in two files; release-wide erratum not established.

## Evidence and scope

Four matched records from monthly piControl and daily 1pctCO2-bgc reproduce the published tos/tossq exactly through the fresh Gadi writer. Negative variance is already present in the unpacked float32 Kelvin moments. All 67,113 negative native cell values fit cell-specific final-rounding intervals that include zero, conditional on round-to-nearest storage. This permits a precision explanation; it does not recover true variance or prove that no earlier accumulation error exists.

The candidate list covers 14 versions and 643 files. Only four records in two tossq files have the native/fresh/published precision check; the full candidate list is not a withdrawal list.

Inventory scope: **14 dataset versions / 643 file references**. [Exact list](candidate-files.txt) · [Dataset/version inventory](datasets.json) · [Machine-readable draft](erratum-draft.json). List SHA-256: `38e5ee7640b19e7ea340ced6fdc8d779abf82907adbbc43260bbc0142681d8a2`.

[Two verified tossq files](verified-files.txt); all other listed files remain candidate scope.

## Who needs to act?

MOM/model developers: inspect evaluation, accumulation and output precision, and supply a replay/reference central moment. MOPPy developers: retain the reproducible precision diagnostic. Publication team: assess broader incidence before deciding a formal release-wide erratum; the two verified tossq files are listed separately.

## Correction and acceptance

Run a short model replay with consistent float64 moment evaluation, time accumulation and archive precision, and compare against directly accumulated central variance. Preserve the reported moments; do not force tossq to tos² to hide the residual.

## Gadi validation

Four records each contain 69,782 wet and 38,218 masked cells. Fresh and published moments match bit-for-bit with identical grids/time support/masks. Native minima range from −0.011921 to −0.012686 °C²; published minima from −0.011900 to −0.013027 °C². All 19 focused tests passed.

[Implementation or validation PR](https://github.com/ACCESS-NRI/ACCESS-MOPPy/pull/874) at `cbcb1b993a22d8514f2b87df97fcf0c380d2f869`.

Tracking: [863](https://github.com/ACCESS-NRI/ACCESS-MOPPy/issues/863) · Audit ID **P02**.

No published data or ESGF records were changed. Replacement versions and ESGF erratum identifiers remain unset.

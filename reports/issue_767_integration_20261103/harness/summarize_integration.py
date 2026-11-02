import collections
import json
from pathlib import Path

ROOT = Path(__file__).parent
RUN = ROOT / "validation/integration"
rows = json.loads((RUN / "results.json").read_text())


def counts(items):
    total = len(items)

    def high(checks):
        return any(c["weight"] == 3 for c in checks)

    def medium(checks):
        return any(c["weight"] == 2 for c in checks)

    def optional(checks):
        return any(c["weight"] == 1 for c in checks)

    return dict(
        outputs=total,
        high_failure_outputs=sum(high(x) for x in items),
        medium_outputs=sum(medium(x) for x in items),
        optional_outputs=sum(optional(x) for x in items),
        mandatory_pass=sum(not high(x) for x in items),
        normal_pass=sum(not high(x) and not medium(x) for x in items),
        strict_pass=sum(not x for x in items),
        findings_without_high=sum(not high(x) and bool(x) for x in items),
    )


stock = [f["wcrp"]["failed_checks"] for r in rows for f in r.get("files", [])]
summary = {
    "attempted": len(rows),
    "processing": dict(collections.Counter(r["processing"] for r in rows)),
    "stock_checker": counts(stock),
    "structural_failures": [
        r["legacy"]
        for r in rows
        for f in r.get("files", [])
        if f["structural"] != "passed"
    ],
    "range_failures": [
        r["legacy"]
        for r in rows
        for f in r.get("files", [])
        if not f["range_qc"]["passed"]
    ],
    "stock_checker_errors": [
        r["legacy"]
        for r in rows
        for f in r.get("files", [])
        if f["wcrp"].get("errors") or f["wcrp"]["returncode"] not in (0, 1)
    ],
    "stock_high_failures": [
        {
            "legacy": r["legacy"],
            "checks": [
                c
                for f in r.get("files", [])
                for c in f["wcrp"]["failed_checks"]
                if c["weight"] == 3
            ],
        }
        for r in rows
        if any(
            c["weight"] == 3
            for f in r.get("files", [])
            for c in f["wcrp"]["failed_checks"]
        )
    ],
}
proposed = ROOT / "validation/integration-proposed-checker/results.json"
if proposed.exists():
    p = json.loads(proposed.read_text())
    summary["proposed_checker"] = counts([r["failed_checks"] for r in p])
    summary["proposed_high_failures"] = [
        {
            "legacy": r["legacy"],
            "checks": [c for c in r["failed_checks"] if c["weight"] == 3],
        }
        for r in p
        if r["counts"]["3"]
    ]
    summary["proposed_checker_errors"] = [
        r["legacy"] for r in p if r["status"] == "error"
    ]
    for weight, word in [(2, "medium"), (1, "optional")]:
        groups = collections.defaultdict(list)
        for r in p:
            for c in r["failed_checks"]:
                if c["weight"] == weight:
                    groups[c["name"]].append(r["legacy"])
        summary[f"remaining_{word}_groups"] = dict(groups)
comparison = RUN / "comparison.json"
if comparison.exists():
    c = json.loads(comparison.read_text())
    paired = [
        (r["legacy"], f["numerical_comparison"])
        for r in c
        for f in r["files"]
        if "numerical_comparison" in f
    ]
    summary["data_comparison"] = {
        "paired_products": len(paired),
        "identical_values_and_masks": sum(
            v["values_identical"] and v["masks_identical"] for _, v in paired
        ),
        "changed": [
            dict(legacy=n, **v)
            for n, v in paired
            if not v["values_identical"] or not v["masks_identical"]
        ],
    }
(RUN / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(
    json.dumps(
        {k: v for k, v in summary.items() if not k.startswith("remaining_")}, indent=2
    )
)

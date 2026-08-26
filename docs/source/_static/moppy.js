// Filter box for the physical-range reference table.
//
// The table itself is generated from
// access_moppy/resources/qc/cmip7_ranges.yml at build time by
// docs/qc_ranges.py; this only hides rows that do not match what is typed.
(function () {
    "use strict";

    function attach(container) {
        var input = container.querySelector(".moppy-ranges-filter input");
        var count = container.querySelector(".moppy-ranges-count");
        var rows = Array.prototype.slice.call(
            container.querySelectorAll("table.moppy-ranges tbody tr")
        );
        if (!input || !rows.length) {
            return;
        }

        function apply() {
            var needle = input.value.trim().toLowerCase();
            var shown = 0;
            rows.forEach(function (row) {
                var match = !needle || row.textContent.toLowerCase().indexOf(needle) !== -1;
                row.style.display = match ? "" : "none";
                if (match) {
                    shown += 1;
                }
            });
            if (count) {
                count.textContent = shown + " of " + rows.length + " variables";
            }
        }

        input.addEventListener("input", apply);
        apply();
    }

    document.addEventListener("DOMContentLoaded", function () {
        Array.prototype.forEach.call(
            document.querySelectorAll(".moppy-ranges-container"),
            attach
        );
    });
})();

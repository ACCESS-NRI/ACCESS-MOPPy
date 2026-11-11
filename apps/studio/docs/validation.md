# Initial pilot validation

The initial pilot was validated on 11 November 2026 with MOPPy 1.8.5 in
`conda/analysis3-26.10`. It used native input, fresh scratch output and no
publication-copy step. The source production configuration was unchanged.

- A real password login and Jupyter-to-Studio proxy test passed locally, on a
  Gadi login node and in a PBS compute job.
- A monthly `tas` pilot produced 12 time steps for 2000 and passed native range
  and repacking checks.
- The requested daily `tas`, `pr` and `tos` pilot for 1850 completed through the
  Studio interface: 3/3 variables, zero failures, monitor and workers exit 0.
- Each daily file contained 365 steps, expected experiment/model/variant metadata,
  appropriate units and finite unmasked first/last-field samples. All three
  passed the configured CF 1.11 / WCRP CMIP7 1.0 checks (CV 1.2.16; minimum weight 3)
  and native range/repacking checks. Output totalled approximately 118 MiB.
- Browser verification covered light/dark and narrow/desktop layouts, config
  loading, builder/YAML round trips, review/submission, progress and worker logs.

These results describe the initial scientific pilot, before repository packaging.
The PR's CI and local checks validate the packaged beta and its authentication
workflow. New scientific runtime versions and interface changes require their
own bounded pilot; these checks do not certify all variables or experiments.

For the packaged `0.1.0b1` beta, 29 tests passed against the installed wheel from
outside the checkout, including the real authenticated proxy and scientific
subprocess isolation. The wheel's native `inspect` probe also passed on Gadi with
MOPPy 1.8.5, without submitting another job. Browser checks confirmed that the
beta notice remains visible in desktop/narrow and light/dark layouts. Both new
guides rendered in a focused Sphinx build; their document targets were checked
against the repository. The full repository documentation build was not run locally.

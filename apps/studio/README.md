# MOPPy Studio for ARE — Beta

A beta application for loading or building MOPPy batch configurations,
reviewing and submitting native PBS monitor jobs, and following per-variable
CMORisation progress. It uses the MOPPy logo and the ACCESS blue/cyan palette
shared with ILAMB Studio, with navigation inspired by the Intake catalog docs.

**In development:** the interface and supported workflow may change. Studio is
versioned independently (`0.1.0b1`) and displays a persistent beta label.

See the [user guide](../../docs/source/howto/studio.rst) for a complete walkthrough
and [development guide](../../docs/source/development/studio.rst) for packaging,
architecture, testing and deployment.

## Launch on ARE

Installed at `/g/data/xp65/public/apps/moppy-studio`, maintained as `xp65_ci`.
Open **JupyterLab** on [ARE](https://are.nci.org.au) with:

| Setting | Value |
|---|---|
| Compute project | Your allocated project; `iq82` for the pilot |
| CPUs / memory | 1 CPU / 4 GB for the web session |
| Module directories | `/g/data/xp65/public/modules` |
| Modules | `python3/3.11.7 moppy-studio` |
| Storage | `gdata/xp65+gdata/p73+scratch/iq82`, adjusted for your input/output projects |

Select **Open JupyterLab** once the session starts. The module's `jupyter`
wrapper opens Studio using ARE's normal password login. This is a custom
JupyterLab launch, not a new tile in the NCI dashboard. Run the ARE session and
scientific jobs under your own account, with your project allocation and data
access. Loading multiple Studio modules in the same session is not supported.

## Workflow

1. **Create a batch:** load a Gadi YAML path, upload a local YAML file, or use the
   builder. Specify the model, experiment, lineage, variables, input and new
   output directory. The YAML editor retains advanced MOPPy options.
2. **Check & review:** preflight checks configuration structure, path access and
   MOPPy interface compatibility. Review the saved YAML and native monitor PBS
   script, including executable `worker_init`. Studio-generated environment
   setup is pinned to the resolved dated analysis3 module. Imported setup is
   retained, with a notice explaining the runtime distinction.
3. **Submit:** one monitor job is submitted as the current user. Native MOPPy
   manages variable jobs, tracking, recovery and output generation. Work
   continues when the ARE session ends.
4. **Runs & progress:** select a saved batch for per-variable states, PBS monitor
   status, task metadata and log tails. The selected batch refreshes every
   15 seconds. Progress measures variables completed, not elapsed work.
5. **Attach existing batch:** give an output directory or `cmor_tasks.db` path
   to follow a run that was started outside Studio. Attachment is read-only.

Saved plans and run records live in `~/.local/state/moppy-studio-are` on ARE.
Configurations, monitor scripts, SQLite tracking, logs and scientific outputs
live in the selected output directory. Scratch is not archival storage.

## Runtime and current scope

The web environment is separate from analysis3. This adapter supports the native
monitor interface in MOPPy 1.8.x and 1.9.x; the pilot was exercised with 1.8.5 in
`conda/analysis3-26.10`. It uses MOPPy's own monitor template and task tracker to
initialise a fresh batch, then submits that script. It does not start the
standalone Streamlit listener. Once submitted, Studio only reads the tracking
database; it does not migrate or change a running batch's database.

This first version covers paths and configs; Intake catalog discovery is not
included. Preflight does not verify complete temporal coverage, all variable
mappings, experiment vocabulary or scientific correctness. Those checks remain
with MOPPy and the user's QC workflow. Start with a bounded scientific check.

The pilot requires a **new output folder** for each submitted plan. Custom
`script_dir` must equal the output directory; an optional `drs_root` must be
inside it. `scheduler_options` is limited to the compute project directive;
per-variable overrides support queue, CPUs, memory, jobfs and walltime. Native
shell templates require these paths to avoid spaces and shell metacharacters.
Advanced scientific fields are retained; YAML comments/formatting are normalised.

Input/output mount suggestions include resolved top-level paths, but not every
symlink or path embedded in arbitrary advanced settings. Include all required
mounts explicitly in both ARE and PBS storage settings.

## Recovery

- **Submission uncertain / submitting:** use **Find accepted job in PBS**. The
  lookup accepts exactly one job owned by you with the recorded unique name and
  output path. It never repeats `qsub`. No match is not proof of rejection;
  inspect PBS history before starting equivalent work.
- **Unknown:** scheduler history is unavailable; this is not success.
- **Incomplete:** the monitor ended with required tasks unfinished, even if its
  PBS exit code was zero. Inspect monitor and worker logs.
- **Preparation failed:** no monitor was submitted. Inspect the recorded error
  and create a new reviewed plan with a fresh output folder.

Retrying, appending work and cancellation are deliberately left to MOPPy/PBS
tools in this pilot. Studio does not launch duplicate monitors or overwrite
existing scientific output as a retry strategy.

## Develop and deploy

```bash
cd apps/studio
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[dev]'
.venv/bin/python -m pytest -q tests
.venv/bin/moppy-studio
```

Open `http://127.0.0.1:8767` and enter the printed session token. Local editing
works without MOPPy; native preflight requires Gadi. All API routes require a
session token. ARE injects that token through an authenticated Unix socket proxy
and also requires session XSRF protection for mutations. The launcher refuses
to run without ARE's hashed password and base URL.

The standalone package includes its web assets. `pip install ./apps/studio`
installs the local app; `pip install './apps/studio[are]'` adds the ARE launcher.
Neither installation installs the scientific MOPPy environment. The repository's
root package and release process remain separate.

To deploy, copy the contents of `apps/studio/` to the shared app location and run
`bash deploy/install.sh` as its owner. The installer builds and installs the
package with its ARE extra into `.are-venv`. Preserve that environment on source
updates, rerun the installer, and restart ARE sessions to load the new package.
The installer does not change analysis3 or other Studio installations.

See [validation](docs/validation.md), [asset attribution](NOTICE),
[MOPPy batch documentation](https://access-moppy.readthedocs.io/en/latest/howto/batch_processing.html)
and [the visual reference](https://access-nri-intake-catalog.readthedocs.io/en/latest/index.html).

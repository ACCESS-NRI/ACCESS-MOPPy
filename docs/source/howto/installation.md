# Installation

ACCESS-MOPPy requires **Python >= 3.11**.

## On NCI Gadi (recommended for ACCESS users)

The `conda/analysis3` environment maintained by ACCESS-NRI already includes
`access_moppy` and its dependencies:

```bash
module use /g/data/xp65/public/modules
module load conda/analysis3
```

All command-line tools (`moppy-cmorise`, `moppy-tui`, `moppy-qc`, …) are
available immediately after loading the module. You will need membership of
the relevant NCI projects to read model archives (e.g. `p73`) and the
`xp65` project for the module itself.

## From PyPI

```bash
pip install access_moppy
```

## From conda

```bash
conda install -c accessnri access-moppy
```

## From source

```bash
git clone https://github.com/ACCESS-NRI/ACCESS-MOPPy.git
cd ACCESS-MOPPy
pip install .
```

For development, the repository uses [pixi](https://pixi.sh) to manage
environments:

```bash
pixi install          # create the default environment
pixi run pytest       # run the test suite
pixi run docs-build   # build this documentation
```

## Optional extras

| Extra | Installs | Needed for |
|---|---|---|
| `access_moppy[tui]` | `rich` | `moppy-tui` terminal dashboard |
| `access_moppy[atmos-tools]` | `f90nml` | `moppy-calc-ab-coeffts` legacy utility |
| `access_moppy[esmval]` | ESMValCore integration deps | `moppy-esmval-prepare` / `moppy-esmval-run` |
| `access_moppy[docs]` | Sphinx toolchain | building the documentation |
| `access_moppy[test]` | pytest and friends | running the test suite |

The Streamlit web dashboard (`moppy-dashboard`) additionally requires
`streamlit`:

```bash
pip install streamlit
```

## First-run setup

The first time you import `access_moppy`, it prompts for your name, email,
organisation, and ORCID, and stores them in `~/.moppy/user.yml`. These are
written into every CMORised file as provenance metadata. See
{doc}`/reference/configuration`.

## Verifying the installation

```bash
moppy-example-config        # prints the bundled example batch config
python -c "import access_moppy; print(access_moppy.__version__)"
```

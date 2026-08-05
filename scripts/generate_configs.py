#!/usr/bin/env python3
"""Reusable batch CMORisation config generator for ACCESS-ESM1-6 ensembles.

Scans an archive directory for ensemble-member sub-folders, generates a
batch_config.yml per experiment, and writes a submit_all.sh launcher.

Usage examples
--------------
Auto-discover all members in an archive directory:

    python scripts/generate_esm_historical_configs.py \\
        --archive-dir /g/data/p73/archive/CMIP7/ACCESS-ESM1-6/production/ensemble-historical \\
        --output-dir  /scratch/p73/rb5533/CMIP7_CMORisation/ensemble-historical

Explicit list (names or a file with one name per line):

    python scripts/generate_esm_historical_configs.py \\
        --archive-dir /g/data/.../ensemble-historical \\
        --experiments historical-...-r1i1p1f1-abc123 historical-...-r2i1p1f1-def456 \\
        --output-dir  /scratch/.../ensemble-historical

    python scripts/generate_esm_historical_configs.py \\
        --archive-dir /g/data/.../ensemble-historical \\
        --experiments-file members.txt \\
        --output-dir  /scratch/.../ensemble-historical

Generate configs AND immediately submit all jobs:

    python scripts/generate_esm_historical_configs.py \\
        --archive-dir ... --output-dir ... --submit

All per-variable resource overrides and the full variable list can be edited
directly in the CONFIG_TEMPLATE string below.  Only the fields marked with
{placeholders} change between experiments.
"""

import argparse
import csv
import os
import re
import stat
import subprocess
import sys

import cftime

# ---------------------------------------------------------------------------
# Regex that matches a CMIP-style variant label anywhere in a string.
# ---------------------------------------------------------------------------
_VARIANT_RE = re.compile(r"(r\d+i\d+p\d+f\d+)")

# ---------------------------------------------------------------------------
# Parent/branch metadata, sourced from scripts/cmip7_fastrack_parents.csv.
# See resolve_parent_info() / resolve_parent_variant_labels() below.
# ---------------------------------------------------------------------------
_DEFAULT_PARENT_CSV = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "cmip7_fastrack_parents.csv"
)
_DEFAULT_PARENT_EXPERIMENT_ID = "picontrol"
_DEFAULT_PARENT_VARIANT_LABEL = "r1i1p1f1"
_DEFAULT_BRANCH_TIME = 0.0
# branch_time_in_parent/child in the CSV are calendar dates. ACCESS-ESM
# picontrol/spinup runs use a 365-day calendar, so branch days are computed
# assuming "noleap" throughout.
_PARENT_TIME_UNITS = "days since 0001-01-01 00:00:00"
_PARENT_CALENDAR = "noleap"

# Archive directory names and cmip7_fastrack_parents.csv both carry a legacy
# 'esm-' prefix and inconsistent piControl casing left over from the ensemble
# naming scheme — neither is a real CMIP experiment_id. Collapse known
# aliases to their canonical id here so every consumer agrees on one spelling.
_EXPERIMENT_ID_ALIASES = {
    "esm-historical": "historical",
    "esm-picontrol": "picontrol",
    "picontrol": "picontrol",
}


def normalize_experiment_id(raw: str) -> str:
    """Map known archive/CSV naming aliases to their canonical experiment_id."""
    raw = (raw or "").strip()
    return _EXPERIMENT_ID_ALIASES.get(raw.lower(), raw)


# ---------------------------------------------------------------------------
# Config template.
# Placeholders filled per experiment:
#   {experiment}      — experiment directory name (used in comments)
#   {config_path}     — absolute path to the generated config file
#   {experiment_id}   — CMIP experiment_id  (e.g. "historical")
#   {variant_label}   — CMIP variant label  (e.g. "r1i1p1f1")
#   {source_id}       — CMIP source_id      (e.g. "ACCESS-ESM1-6")
#   {activity_id}     — CMIP activity_id    (e.g. "CMIP")
#   {cmip_version}    — CMIP era            (e.g. "CMIP7")
#   {input_folder}    — full path to this member's archive directory
#   {output_folder}   — full path to this member's scratch output directory
#   {script_dir}      — script/scratch base directory
#   {publication_lock_dir} — shared transfer-slot directory for all experiments
# ---------------------------------------------------------------------------
CONFIG_TEMPLATE = """\
# Batch CMORisation configuration — {source_id}, {cmip_version} Baseline
# Experiment: {experiment}
# Run with:
#   python -m access_moppy.batch_cmoriser {config_path}

# All CMIP7 Baseline variables using CMIP7 compound names.
# Variables with no mapping in ACCESS-ESM1-6 are commented out and marked NOT MAPPED.
# This config uses the built-in file finder: input_folder points at the archive
# root and the ACCESS-ESM1.6 mapping rules locate the native files automatically.

variables:
  # --- Fixed fields ---
  - atmos.areacella.ti-u-hxy-u.fx.glb        # fx.areacella
  - land.mrsofc.ti-u-hxy-lnd.fx.glb          # fx.mrsofc
  - land.orog.ti-u-hxy-u.fx.glb              # fx.orog
  - land.rootd.ti-u-hxy-lnd.fx.glb           # fx.rootd
  - land.sftgif.ti-u-hxy-u.fx.glb            # fx.sftgif
  - atmos.sftlf.ti-u-hxy-u.fx.glb            # fx.sftlf
  - land.slthick.ti-sl-hxy-lnd.fx.glb        # Efx.slthick
  - ocean.areacello.ti-u-hxy-u.fx.glb        # Ofx.areacello
  # - ocean.basin.ti-u-hxy-u.fx.glb          # Ofx.basin — NOT MAPPED
  - ocean.deptho.ti-u-hxy-sea.fx.glb         # Ofx.deptho
  - ocean.hfgeou.ti-u-hxy-sea.fx.glb         # Ofx.hfgeou
  - ocean.sftof.ti-u-hxy-u.fx.glb            # Ofx.sftof

  # --- Monthly atmosphere ---
  - atmos.cl.tavg-al-hxy-u.mon.glb           # Amon.cl
  - atmos.cli.tavg-al-hxy-u.mon.glb          # Amon.cli
  - atmos.clivi.tavg-u-hxy-u.mon.glb         # Amon.clivi
  - atmos.clt.tavg-u-hxy-u.mon.glb           # Amon.clt
  - atmos.clw.tavg-al-hxy-u.mon.glb          # Amon.clw
  - atmos.clwvi.tavg-u-hxy-u.mon.glb         # Amon.clwvi
  - atmos.evspsbl.tavg-u-hxy-u.mon.glb       # Amon.evspsbl
  - atmos.hfls.tavg-u-hxy-u.mon.glb          # Amon.hfls
  - atmos.hfss.tavg-u-hxy-u.mon.glb          # Amon.hfss
  - atmos.hur.tavg-p19-hxy-air.mon.glb       # Amon.hur
  - atmos.hurs.tavg-h2m-hxy-u.mon.glb        # Amon.hurs
  - atmos.hus.tavg-p19-hxy-u.mon.glb         # Amon.hus
  - atmos.huss.tavg-h2m-hxy-u.mon.glb        # Amon.huss
  - atmos.pr.tavg-u-hxy-u.mon.glb            # Amon.pr
  - atmos.prc.tavg-u-hxy-u.mon.glb           # Amon.prc
  - atmos.prsn.tavg-u-hxy-u.mon.glb          # Amon.prsn
  - atmos.prw.tavg-u-hxy-u.mon.glb           # Amon.prw
  - atmos.ps.tavg-u-hxy-u.mon.glb            # Amon.ps
  - atmos.psl.tavg-u-hxy-u.mon.glb           # Amon.psl
  - atmos.rlds.tavg-u-hxy-u.mon.glb          # Amon.rlds
  - atmos.rldscs.tavg-u-hxy-u.mon.glb        # Amon.rldscs
  - atmos.rlus.tavg-u-hxy-u.mon.glb          # Amon.rlus
  - atmos.rluscs.tavg-u-hxy-u.mon.glb        # Amon.rluscs
  - atmos.rlut.tavg-u-hxy-u.mon.glb          # Amon.rlut
  - atmos.rlutcs.tavg-u-hxy-u.mon.glb        # Amon.rlutcs
  - atmos.rsds.tavg-u-hxy-u.mon.glb          # Amon.rsds
  - atmos.rsdscs.tavg-u-hxy-u.mon.glb        # Amon.rsdscs
  - atmos.rsdt.tavg-u-hxy-u.mon.glb          # Amon.rsdt
  - atmos.rsus.tavg-u-hxy-u.mon.glb          # Amon.rsus
  - atmos.rsuscs.tavg-u-hxy-u.mon.glb        # Amon.rsuscs
  - atmos.rsut.tavg-u-hxy-u.mon.glb          # Amon.rsut
  - atmos.rsutcs.tavg-u-hxy-u.mon.glb        # Amon.rsutcs
  - atmos.sfcWind.tavg-h10m-hxy-u.mon.glb    # Amon.sfcWind
  - atmos.ta.tavg-p19-hxy-air.mon.glb        # Amon.ta
  - atmos.tas.tavg-h2m-hxy-u.mon.glb         # Amon.tas
  - atmos.tas.tmaxavg-h2m-hxy-u.mon.glb      # Amon.tasmax
  - atmos.tas.tminavg-h2m-hxy-u.mon.glb      # Amon.tasmin
  - atmos.tauu.tavg-u-hxy-u.mon.glb          # Amon.tauu
  - atmos.tauv.tavg-u-hxy-u.mon.glb          # Amon.tauv
  - atmos.ts.tavg-u-hxy-u.mon.glb            # Amon.ts
  - atmos.ua.tavg-p19-hxy-air.mon.glb        # Amon.ua
  - atmos.uas.tavg-h10m-hxy-u.mon.glb        # Amon.uas
  - atmos.va.tavg-p19-hxy-air.mon.glb        # Amon.va
  - atmos.vas.tavg-h10m-hxy-u.mon.glb        # Amon.vas
  - atmos.wap.tavg-p19-hxy-air.mon.glb       # Amon.wap
  - atmos.zg.tavg-p19-hxy-air.mon.glb        # Amon.zg

  # --- Monthly land ---
  - land.evspsblsoi.tavg-u-hxy-lnd.mon.glb   # Lmon.evspsblsoi
  - land.evspsblveg.tavg-u-hxy-lnd.mon.glb   # Lmon.evspsblveg
  - land.lai.tavg-u-hxy-lnd.mon.glb          # Lmon.lai
  - landIce.mrfso.tavg-u-hxy-lnd.mon.glb     # Lmon.mrfso
  - land.mrro.tavg-u-hxy-lnd.mon.glb         # Lmon.mrro
  - land.mrros.tavg-u-hxy-lnd.mon.glb        # Lmon.mrros
  - land.mrso.tavg-u-hxy-lnd.mon.glb         # Lmon.mrso
  - land.mrsol.tavg-d10cm-hxy-lnd.mon.glb    # Lmon.mrsos
  - land.cSoil.tavg-u-hxy-lnd.mon.glb        # Emon.cSoil
  - land.cVeg.tavg-u-hxy-lnd.mon.glb         # Lmon.cVeg
  - land.fBNF.tavg-u-hxy-lnd.mon.glb         # Emon.fBNF
  - land.gpp.tavg-u-hxy-lnd.mon.glb          # Lmon.gpp
  - land.nbp.tavg-u-hxy-lnd.mon.glb          # Lmon.nbp
  - land.ra.tavg-u-hxy-lnd.mon.glb           # Lmon.ra
  - land.rh.tavg-u-hxy-lnd.mon.glb           # Lmon.rh
  - land.tsl.tavg-sl-hxy-lnd.mon.glb         # Lmon.tsl
  - landIce.snc.tavg-u-hxy-lnd.mon.glb       # LImon.snc
  - landIce.snw.tavg-u-hxy-lnd.mon.glb       # LImon.snw

  # --- Monthly ocean ---
  - ocean.bigthetao.tavg-ol-hxy-sea.mon.glb  # Omon.bigthetao
  - ocean.hfds.tavg-u-hxy-sea.mon.glb        # Omon.hfds
  - ocean.masscello.tavg-ol-hxy-sea.mon.glb  # Omon.masscello
  - ocean.mlotst.tavg-u-hxy-sea.mon.glb      # Omon.mlotst
  - ocean.so.tavg-ol-hxy-sea.mon.glb         # Omon.so
  - ocean.sos.tavg-u-hxy-sea.mon.glb         # Omon.sos
  - ocean.tauuo.tavg-u-hxy-sea.mon.glb       # Omon.tauuo
  - ocean.tauvo.tavg-u-hxy-sea.mon.glb       # Omon.tauvo
  - ocean.thetao.tavg-ol-hxy-sea.mon.glb     # Omon.thetao
  - ocean.thkcello.tavg-ol-hxy-sea.mon.glb   # Omon.thkcello
  - ocean.tos.tavg-u-hxy-sea.mon.glb         # Omon.tos
  - ocean.umo.tavg-ol-hxy-sea.mon.glb        # Omon.umo
  - ocean.uo.tavg-ol-hxy-sea.mon.glb         # Omon.uo
  - ocean.vmo.tavg-ol-hxy-sea.mon.glb        # Omon.vmo
  - ocean.vo.tavg-ol-hxy-sea.mon.glb         # Omon.vo
  - ocean.wmo.tavg-ol-hxy-sea.mon.glb        # Omon.wmo
  - ocean.wo.tavg-ol-hxy-sea.mon.glb         # Omon.wo
  - ocean.zos.tavg-u-hxy-sea.mon.glb         # Omon.zos
  - ocean.zostoga.tavg-u-hm-sea.mon.glb      # Omon.zostoga

  # --- Monthly sea ice ---
  - seaIce.siconc.tavg-u-hxy-u.mon.glb       # SImon.siconc
  - seaIce.simass.tavg-u-hxy-si.mon.glb      # SImon.simass
  # - seaIce.snd.tavg-u-hxy-sn.mon.glb       # SImon.sisnthick — NOT MAPPED
  # - seaIce.ts.tavg-u-hxy-si.mon.glb          # SImon.sitemptop
  - seaIce.sithick.tavg-u-hxy-si.mon.glb     # SImon.sithick
  - seaIce.sitimefrac.tavg-u-hxy-sea.mon.glb # SImon.sitimefrac
  - seaIce.siu.tavg-u-hxy-si.mon.glb         # SImon.siu
  - seaIce.siv.tavg-u-hxy-si.mon.glb         # SImon.siv

    #  # --- Daily atmosphere ---
    #  - atmos.ps.tavg-u-hxy-u.day.glb            # CFday.ps
    #  - atmos.clt.tavg-u-hxy-u.day.glb           # day.clt
    #  - atmos.hur.tavg-p19-hxy-u.day.glb         # day.hur
    #  - atmos.hurs.tavg-h2m-hxy-u.day.glb        # day.hurs
    #  - atmos.hus.tavg-p19-hxy-u.day.glb         # day.hus
    #  - atmos.huss.tavg-h2m-hxy-u.day.glb        # day.huss
    #  - atmos.pr.tavg-u-hxy-u.day.glb            # day.pr
    #  - atmos.psl.tavg-u-hxy-u.day.glb           # day.psl
    #  - atmos.rsds.tavg-u-hxy-u.day.glb          # day.rsds
    #  - atmos.sfcWind.tavg-h10m-hxy-u.day.glb    # day.sfcWind
    #  - atmos.ta.tavg-p19-hxy-air.day.glb        # day.ta
    #  - atmos.tas.tavg-h2m-hxy-u.day.glb         # day.tas
    #  - atmos.tas.tmax-h2m-hxy-u.day.glb         # day.tasmax
    #  - atmos.tas.tmin-h2m-hxy-u.day.glb         # day.tasmin
    #  - atmos.ua.tavg-p19-hxy-air.day.glb        # day.ua
    #  - atmos.uas.tavg-h10m-hxy-u.day.glb        # day.uas
    #  - atmos.va.tavg-p19-hxy-air.day.glb        # day.va
    #  - atmos.vas.tavg-h10m-hxy-u.day.glb        # day.vas
    #  - atmos.wap.tavg-p19-hxy-u.day.glb         # day.wap
    #  - atmos.zg.tavg-p19-hxy-air.day.glb        # day.zg
    #
    #  # --- Daily ocean ---
    #  - ocean.sos.tavg-u-hxy-sea.day.glb         # Oday.sos
    #  - ocean.tos.tavg-u-hxy-sea.day.glb         # Oday.tos
    #  - ocean.zos.tavg-u-hxy-sea.day.glb         # Oday.zos
    #
    #  # --- Daily sea ice ---
    #  - seaIce.siconc.tavg-u-hxy-u.day.glb       # SIday.siconc
    #
    #  # --- Sub-daily atmosphere ---
    #  - atmos.pr.tavg-u-hxy-u.1hr.glb            # E1hr.pr
    #  - atmos.huss.tpt-h2m-hxy-u.3hr.glb         # 3hr.huss
    #  - atmos.pr.tavg-u-hxy-u.3hr.glb            # 3hr.pr
    #  - atmos.tas.tpt-h2m-hxy-u.3hr.glb          # 3hr.tas
    #  - atmos.uas.tpt-h10m-hxy-u.3hr.glb         # 3hrPt.uas
    #  - atmos.vas.tpt-h10m-hxy-u.3hr.glb         # 3hrPt.vas
    #  - atmos.hurs.tavg-h2m-hxy-u.6hr.glb        # 6hrPlev.hurs
    #  - atmos.ta.tpt-p3-hxy-air.6hr.glb          # 6hrPlevPt.ta
    #  - atmos.ua.tpt-p3-hxy-air.6hr.glb          # 6hrPlevPt.ua
    #  - atmos.va.tpt-p3-hxy-air.6hr.glb          # 6hrPlevPt.va


# Required: CMIP metadata
cmip_version: {cmip_version}
experiment_id: {experiment_id}
source_id: {source_id}
variant_label: {variant_label}
activity_id: {activity_id}


# Optional: Parent experiment information.
# Without this block the package CMIP7 default is used (ACCESS-ESM1-6 / CMIP7
# picontrol). Override it here when your run used different parent settings or
# to set the correct branch_time_in_parent for your specific run.
parent_info:
  parent_experiment_id: {parent_experiment_id}
  parent_activity_id: CMIP
  parent_source_id: {source_id}
  parent_variant_label: {parent_variant_label}
  parent_time_units: "days since 0001-01-01 00:00:00"
  parent_mip_era: {cmip_version}
  branch_time_in_child: {branch_time_in_child}
  branch_time_in_parent: {branch_time_in_parent}    # replace with the actual branch day
  branch_method: standard


script_dir: "{script_dir}"
# Required: Input and output paths
input_folder: "{input_folder}"
output_folder: "{output_folder}"

# Optional: model_id selects which mapping file drives auto file-discovery.
# Defaults to "ACCESS-ESM1.6" when omitted.
# model_id: ACCESS-ESM1.6

# Optional: per-variable file-pattern overrides.
# Leave this block commented out unless the run layout differs from the
# expected ACCESS-ESM1-6 archive structure or you are debugging discovery.
#
# file_patterns:
#   atmos.tas.tavg-h2m-hxy-u.mon.glb: "/output*/atmosphere/netCDF/*mon.nc"
#   ocean.tos.tavg-u-hxy-sea.mon.glb: "/output*/ocean/ocean-2d-surface_temp-1monthly-mean*.nc"


# PBS job configuration (defaults for all variables)
queue: "normalbw" # Use Broadwell nodes, much cheaper.
cpus_per_node: 12
mem: "64GB"
jobfs: 100GB
use_jobfs_staging: true  # Stage NetCDF writes and per-file QC reads/plots on $PBS_JOBFS before publishing. Off by default.
walltime: "06:00:00"
scheduler_options: "#PBS -P iq82"
storage: "gdata/p73+gdata/tm70+scratch/tm70+gdata/xp65+scratch/p73"

# Keep all variable jobs independent while preventing simultaneous JobFS-to-
# Lustre copies from overwhelming /scratch. The lock directory is shared by
# every generated experiment, so this is a workflow-wide rather than per-run
# limit. Completed transfers release their slot immediately.
publication_lock_dir: "{publication_lock_dir}"
max_concurrent_publications: 12
publication_jitter_seconds: 120
publication_stale_seconds: 86400

# One aggregate qstat request is made for all workers at this interval. Waiting
# for completion makes submit_all.sh run one ~100-variable experiment at a time,
# retaining variable-level computational parallelism without exposing 3000 jobs
# and 30 monitor jobs to PBS at once.
monitor_poll_interval: 300
wait_for_completion: true

# Compute up to this many bounded Dask slices ahead of the serial NetCDF
# writer. Increase only when workers are underused and memory headroom remains.
write_prefetch: 4

# Variable-specific resource overrides
variable_resources:

  # Large 3D ocean variables
  # Memory-intensive, but only modestly parallel.
  ocean.thetao.tavg-ol-hxy-sea.mon.glb: &large_ocean
    queue: "normalbw"
    cpus_per_node: 7
    mem: "256GB"
    walltime: "05:00:00"

  ocean.bigthetao.tavg-ol-hxy-sea.mon.glb:
    <<: *large_ocean

  ocean.so.tavg-ol-hxy-sea.mon.glb:
    <<: *large_ocean

  ocean.uo.tavg-ol-hxy-sea.mon.glb:
    <<: *large_ocean

  ocean.vo.tavg-ol-hxy-sea.mon.glb:
    <<: *large_ocean

  ocean.wo.tavg-ol-hxy-sea.mon.glb:
    <<: *large_ocean

  ocean.umo.tavg-ol-hxy-sea.mon.glb:
    <<: *large_ocean

  ocean.vmo.tavg-ol-hxy-sea.mon.glb:
    <<: *large_ocean

  ocean.wmo.tavg-ol-hxy-sea.mon.glb:
    <<: *large_ocean

  ocean.masscello.tavg-ol-hxy-sea.mon.glb:
    <<: *large_ocean

  ocean.thkcello.tavg-ol-hxy-sea.mon.glb:
    <<: *large_ocean

  # Fixed ocean fields: same memory profile, shorter runtime
  ocean.masscello.ti-ol-hxy-sea.fx.glb: &large_ocean_fx
    queue: "normalbw"
    cpus_per_node: 7
    mem: "256GB"
    walltime: "02:00:00"

  ocean.thkcello.ti-ol-hxy-sea.fx.glb:
    <<: *large_ocean_fx

  # Daily 3D pressure-level atmosphere
  atmos.ta.tavg-p19-hxy-air.day.glb: &daily_3d_atmos
    queue: "normalbw"
    cpus_per_node: 7
    mem: "128GB"
    walltime: "12:00:00"

  atmos.ua.tavg-p19-hxy-air.day.glb:
    <<: *daily_3d_atmos

  atmos.va.tavg-p19-hxy-air.day.glb:
    <<: *daily_3d_atmos

  atmos.hus.tavg-p19-hxy-u.day.glb:
    <<: *daily_3d_atmos

  atmos.hur.tavg-p19-hxy-u.day.glb:
    <<: *daily_3d_atmos

  atmos.wap.tavg-p19-hxy-u.day.glb:
    <<: *daily_3d_atmos

  atmos.zg.tavg-p19-hxy-air.day.glb:
    <<: *daily_3d_atmos

  # Three-hourly surface atmosphere
  atmos.pr.tavg-u-hxy-u.3hr.glb: &subdaily_surface
    queue: "normalbw"
    cpus_per_node: 4
    mem: "64GB"
    walltime: "12:00:00"

  atmos.tas.tpt-h2m-hxy-u.3hr.glb:
    <<: *subdaily_surface

  atmos.huss.tpt-h2m-hxy-u.3hr.glb:
    <<: *subdaily_surface

  atmos.uas.tpt-h10m-hxy-u.3hr.glb:
    <<: *subdaily_surface

  atmos.vas.tpt-h10m-hxy-u.3hr.glb:
    <<: *subdaily_surface

  # Six-hourly pressure-level atmosphere
  atmos.ta.tpt-p3-hxy-air.6hr.glb: &subdaily_3d_atmos
    queue: "normalbw"
    cpus_per_node: 7
    mem: "128GB"
    walltime: "06:00:00"

  atmos.ua.tpt-p3-hxy-air.6hr.glb:
    <<: *subdaily_3d_atmos

  atmos.va.tpt-p3-hxy-air.6hr.glb:
    <<: *subdaily_3d_atmos

  atmos.hurs.tavg-h2m-hxy-u.6hr.glb:
    queue: "normalbw"
    cpus_per_node: 4
    mem: "64GB"
    walltime: "04:00:00"

  atmos.pr.tavg-u-hxy-u.1hr.glb:
    queue: "normalbw"
    cpus_per_node: 4
    mem: "64GB"
    walltime: "06:00:00"


# Environment setup for each job
worker_init: |
  module use /g/data/xp65/public/modules
  module load conda/analysis3-latest

# Optional: Generate QC diagnostic plots after each variable is CMORised.
# When enabled, two PNG files are written per output file into
# <output_folder>/qc_plots/:
#   - <stem>_snapshot.png  : spatial map of the first timestep
#   - <stem>_timeseries.png: per-timestep mean/min/max and std dev (time-varying only)
# Requires: pip install "access_moppy[qc-plots]"  (installs matplotlib)
qc_plots: true
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def extract_variant_label(experiment: str) -> str:
    """Return the CMIP variant label embedded in *experiment* (e.g. 'r1i1p1f1')."""
    m = _VARIANT_RE.search(experiment)
    if not m:
        raise ValueError(
            f"Cannot find a CMIP variant label in experiment name: {experiment!r}\n"
            "Expected something like 'r1i1p1f1' somewhere in the directory name."
        )
    return m.group(1)


def discover_experiments(archive_dir: str) -> list[str]:
    """Return sorted list of sub-directory names under *archive_dir* that
    contain a recognisable CMIP variant label."""
    try:
        entries = sorted(os.listdir(archive_dir))
    except FileNotFoundError:
        sys.exit(f"ERROR: archive directory not found: {archive_dir}")
    experiments = [
        e
        for e in entries
        if os.path.isdir(os.path.join(archive_dir, e)) and _VARIANT_RE.search(e)
    ]
    if not experiments:
        sys.exit(
            f"ERROR: no sub-directories with a variant label found in {archive_dir}"
        )
    return experiments


def load_parent_rows(path: str) -> dict[str, dict[str, str]]:
    """Load cmip7_fastrack_parents.csv, keyed by its ``experiment_name`` column.

    Returns an empty dict (with a warning) if *path* doesn't exist, so callers
    can fall back to the package default parent settings.
    """
    if not os.path.isfile(path):
        print(
            f"  [parent-info] parents CSV not found at {path} — using default picontrol parent for all experiments."
        )
        return {}
    rows: dict[str, dict[str, str]] = {}
    with open(path, newline="") as fh:
        for row in csv.DictReader(fh):
            name = (row.get("experiment_name") or "").strip()
            if name:
                rows[name] = row
    return rows


def date_to_branch_days(date_str: str) -> float | None:
    """Convert a CSV 'YYYY-MM-DD' date to days since _PARENT_TIME_UNITS,
    assuming _PARENT_CALENDAR. Returns None if *date_str* is blank or
    unparseable."""
    date_str = (date_str or "").strip()
    if not date_str:
        return None
    try:
        year, month, day = (int(part) for part in date_str.split("-"))
        date = cftime.datetime(year, month, day, calendar=_PARENT_CALENDAR)
    except ValueError:
        return None
    return float(
        cftime.date2num(date, units=_PARENT_TIME_UNITS, calendar=_PARENT_CALENDAR)
    )


def resolve_parent_variant_labels(
    experiments: list[str],
    parent_rows: dict[str, dict[str, str]],
    override: str | None,
) -> dict[str, str]:
    """Determine parent_variant_label for every distinct parent_experiment_id
    referenced by *experiments*.

    cmip7_fastrack_parents.csv has no variant-label column for parents, so
    this either applies *override* (from --parent-variant-label) to all of
    them, or prompts interactively once per unique parent_experiment_id
    (default r1i1p1f1 on empty input).
    """
    parent_ids = {
        normalize_experiment_id(parent_rows[exp].get("parent_experiment_id"))
        or _DEFAULT_PARENT_EXPERIMENT_ID
        for exp in experiments
        if exp in parent_rows
    }

    cache: dict[str, str] = {}
    for pid in sorted(parent_ids):
        if override:
            cache[pid] = override
            continue
        try:
            resp = input(
                f"Parent variant_label for parent_experiment_id='{pid}' "
                f"[{_DEFAULT_PARENT_VARIANT_LABEL}]: "
            ).strip()
        except EOFError:
            resp = ""
        cache[pid] = resp or _DEFAULT_PARENT_VARIANT_LABEL
    return cache


def resolve_parent_info(
    experiment: str,
    parent_rows: dict[str, dict[str, str]],
    variant_cache: dict[str, str],
) -> dict[str, object]:
    """Return the parent_info template fields for *experiment*, sourced from
    cmip7_fastrack_parents.csv. Falls back to the package CMIP7 picontrol
    default when the experiment has no CSV row or a field is blank."""
    row = parent_rows.get(experiment)
    if row is None:
        print(
            f"  [parent-info] '{experiment}' not found in parents CSV — using default picontrol parent."
        )
        return {
            "parent_experiment_id": _DEFAULT_PARENT_EXPERIMENT_ID,
            "parent_variant_label": _DEFAULT_PARENT_VARIANT_LABEL,
            "branch_time_in_child": _DEFAULT_BRANCH_TIME,
            "branch_time_in_parent": _DEFAULT_BRANCH_TIME,
        }

    parent_experiment_id = (
        normalize_experiment_id(row.get("parent_experiment_id"))
        or _DEFAULT_PARENT_EXPERIMENT_ID
    )

    branch_child = date_to_branch_days(row.get("branch_time_in_child", ""))
    if branch_child is None:
        print(
            f"  [parent-info] '{experiment}': no branch_time_in_child in CSV — defaulting to 0.0."
        )
        branch_child = _DEFAULT_BRANCH_TIME

    branch_parent = date_to_branch_days(row.get("branch_time_in_parent", ""))
    if branch_parent is None:
        print(
            f"  [parent-info] '{experiment}': no branch_time_in_parent in CSV — defaulting to 0.0."
        )
        branch_parent = _DEFAULT_BRANCH_TIME

    return {
        "parent_experiment_id": parent_experiment_id,
        "parent_variant_label": variant_cache.get(
            parent_experiment_id, _DEFAULT_PARENT_VARIANT_LABEL
        ),
        "branch_time_in_child": branch_child,
        "branch_time_in_parent": branch_parent,
    }


def generate_config(
    experiment: str,
    archive_dir: str,
    output_dir: str,
    *,
    experiment_id: str,
    source_id: str,
    activity_id: str,
    cmip_version: str,
    parent_experiment_id: str,
    parent_variant_label: str,
    branch_time_in_child: float,
    branch_time_in_parent: float,
) -> str:
    """Write batch_config.yml for *experiment* into *output_dir*/<experiment>/
    and return the absolute path to the written file."""
    variant_label = extract_variant_label(experiment)
    exp_dir = os.path.join(output_dir, experiment)
    os.makedirs(exp_dir, exist_ok=True)

    config_path = os.path.abspath(os.path.join(exp_dir, "batch_config.yml"))

    # Some ensembles have a duplicated sub-directory, e.g.
    #   <archive>/<member>/<member>/output001/...
    # Detect this and point input_folder at the inner copy.
    candidate_input = os.path.join(archive_dir, experiment)
    nested = os.path.join(candidate_input, experiment)
    input_folder = nested if os.path.isdir(nested) else candidate_input

    content = CONFIG_TEMPLATE.format(
        experiment=experiment,
        config_path=config_path,
        experiment_id=experiment_id,
        variant_label=variant_label,
        source_id=source_id,
        activity_id=activity_id,
        cmip_version=cmip_version,
        script_dir=os.path.join(output_dir, experiment),
        input_folder=input_folder,
        output_folder=os.path.join(output_dir, experiment),
        publication_lock_dir=os.path.join(output_dir, ".moppy_publication_slots"),
        parent_experiment_id=parent_experiment_id,
        parent_variant_label=parent_variant_label,
        branch_time_in_child=branch_time_in_child,
        branch_time_in_parent=branch_time_in_parent,
    )
    with open(config_path, "w") as fh:
        fh.write(content)
    return config_path


def write_submit_script(config_paths: list[str], output_dir: str) -> str:
    """Write submit_all.sh next to the per-experiment directories and return
    its path.  For each experiment it cd-s into the experiment directory,
    runs ``moppy-cmorise batch_config.yml``, then returns to the original
    directory before moving to the next."""
    script_path = os.path.join(output_dir, "submit_all.sh")
    abs_output = os.path.abspath(output_dir)
    lines = [
        "#!/usr/bin/env bash",
        "# Auto-generated by generate_esm_historical_configs.py",
        "# Submits all experiments to PBS via moppy-cmorise.",
        "# Run from any directory:",
        "#   bash " + os.path.abspath(script_path),
        "",
        "set -euo pipefail",
        "",
        f'BASE="{abs_output}"',
        "",
    ]
    for cfg in config_paths:
        exp_dir = os.path.dirname(cfg)
        exp_name = os.path.basename(exp_dir)
        lines.append(f'echo "=== Submitting: {exp_name} ==="')
        lines.append(f'cd "{exp_dir}"')
        lines.append("moppy-cmorise batch_config.yml")
        lines.append('cd "$BASE"')
        lines.append("")
    lines.append('echo "All experiments submitted."')
    lines.append("")

    with open(script_path, "w") as fh:
        fh.write("\n".join(lines))

    # Make it executable
    current = stat.S_IMODE(os.stat(script_path).st_mode)
    os.chmod(script_path, current | stat.S_IXUSR | stat.S_IXGRP)
    return os.path.abspath(script_path)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # ---- required paths ----
    p.add_argument(
        "--archive-dir",
        "-a",
        required=True,
        metavar="DIR",
        help="Root archive directory containing ensemble-member sub-folders.",
    )
    p.add_argument(
        "--output-dir",
        "-o",
        required=True,
        metavar="DIR",
        help="Directory under which per-experiment config sub-folders are created "
        "and where submit_all.sh is written.",
    )

    # ---- experiment selection ----
    selection = p.add_mutually_exclusive_group()
    selection.add_argument(
        "--experiments",
        "-e",
        nargs="+",
        metavar="NAME",
        help="Explicit list of experiment directory names to process. "
        "If omitted, all sub-directories of --archive-dir that contain "
        "a variant label are discovered automatically.",
    )
    selection.add_argument(
        "--experiments-file",
        "-f",
        metavar="FILE",
        help="Path to a text file with one experiment name per line "
        "(blank lines and lines starting with '#' are ignored).",
    )

    # ---- CMIP metadata ----
    p.add_argument(
        "--experiment-id",
        default=None,
        metavar="ID",
        help="CMIP experiment_id to embed in configs. "
        "Default: auto-detected from the archive directory name "
        "(first path component matching a known pattern), or 'historical'.",
    )
    p.add_argument(
        "--source-id",
        default="ACCESS-ESM1-6",
        metavar="ID",
        help="CMIP source_id (default: %(default)s).",
    )
    p.add_argument(
        "--activity-id",
        default="CMIP",
        metavar="ID",
        help="CMIP activity_id (default: %(default)s).",
    )
    p.add_argument(
        "--cmip-version",
        default="CMIP7",
        metavar="VER",
        help="CMIP era string (default: %(default)s).",
    )

    # ---- parent/branch metadata ----
    p.add_argument(
        "--parent-csv",
        default=_DEFAULT_PARENT_CSV,
        metavar="FILE",
        help="CSV mapping experiment_name to parent_experiment_id and branch "
        "dates (default: bundled scripts/cmip7_fastrack_parents.csv).",
    )
    p.add_argument(
        "--no-parent-csv",
        action="store_true",
        help="Ignore --parent-csv and use the package default picontrol "
        "parent for every experiment.",
    )
    p.add_argument(
        "--parent-variant-label",
        default=None,
        metavar="LABEL",
        help="parent_variant_label to use for every experiment, skipping the "
        "interactive prompt (the CSV has no such column).",
    )

    # ---- launch ----
    p.add_argument(
        "--submit",
        action="store_true",
        help="After generating all configs, immediately execute submit_all.sh "
        "to submit every experiment to PBS.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done without writing any files.",
    )

    return p


def infer_experiment_id(archive_dir: str) -> str:
    """Best-effort extraction of a CMIP experiment_id from the archive path.

    Looks for a component that looks like 'historical', 'piControl', etc.
    Falls back to 'historical' if nothing is recognisable.
    """
    basename = os.path.basename(os.path.normpath(archive_dir))
    # Strip leading 'ensemble-' prefix if present
    candidate = re.sub(r"^ensemble-", "", basename)
    # Strip version suffixes like -01, -1.1, etc.
    candidate = re.sub(r"[-_]\d[\d.]*$", "", candidate)
    return normalize_experiment_id(candidate) or "historical"


def main() -> None:
    args = build_parser().parse_args()

    archive_dir = os.path.abspath(args.archive_dir)
    output_dir = os.path.abspath(args.output_dir)

    # --- Resolve experiment list ---
    if args.experiments:
        experiments = args.experiments
    elif args.experiments_file:
        fpath = args.experiments_file
        if not os.path.isfile(fpath):
            sys.exit(f"ERROR: experiments file not found: {fpath}")
        with open(fpath) as fh:
            experiments = [
                line.strip() for line in fh if line.strip() and not line.startswith("#")
            ]
    else:
        print(f"Auto-discovering experiments in: {archive_dir}")
        experiments = discover_experiments(archive_dir)
        print(f"  Found {len(experiments)} member(s).")

    # --- Resolve experiment_id ---
    experiment_id = args.experiment_id or infer_experiment_id(archive_dir)

    # --- Summary ---
    print("\nConfiguration:")
    print(f"  archive_dir   : {archive_dir}")
    print(f"  output_dir    : {output_dir}")
    print(f"  experiment_id : {experiment_id}")
    print(f"  source_id     : {args.source_id}")
    print(f"  activity_id   : {args.activity_id}")
    print(f"  cmip_version  : {args.cmip_version}")
    print(f"  experiments   : {len(experiments)}")
    if args.dry_run:
        print("\n[dry-run] Would generate:")
        for exp in experiments:
            print(f"  {output_dir}/{exp}/batch_config.yml")
        print(f"  {output_dir}/submit_all.sh")
        return

    # --- Resolve parent/branch metadata ---
    parent_rows = {} if args.no_parent_csv else load_parent_rows(args.parent_csv)
    if parent_rows:
        print(
            f"  parent info   : {len(parent_rows)} row(s) loaded from {args.parent_csv}"
        )
        variant_cache = resolve_parent_variant_labels(
            experiments, parent_rows, args.parent_variant_label
        )
    else:
        variant_cache = {}

    # --- Generate configs ---
    os.makedirs(output_dir, exist_ok=True)
    config_paths = []
    print(f"\nGenerating configs in: {output_dir}")
    for exp in experiments:
        parent_fields = resolve_parent_info(exp, parent_rows, variant_cache)
        path = generate_config(
            exp,
            archive_dir,
            output_dir,
            experiment_id=experiment_id,
            source_id=args.source_id,
            activity_id=args.activity_id,
            cmip_version=args.cmip_version,
            **parent_fields,
        )
        config_paths.append(path)
        print(f"  {path}")

    # --- Write submit_all.sh ---
    submit_script = write_submit_script(config_paths, output_dir)
    print(f"\nLauncher written: {submit_script}")
    print(f"  To submit all experiments: bash {submit_script}")

    # --- Optionally submit ---
    if args.submit:
        print("\nSubmitting all experiments...")
        result = subprocess.run(["bash", submit_script], check=False)
        if result.returncode != 0:
            sys.exit(f"submit_all.sh exited with code {result.returncode}")
    else:
        print(f"\nDone — {len(experiments)} config(s) generated.")
        print(f"Review the configs, then run:\n  bash {submit_script}")


if __name__ == "__main__":
    main()

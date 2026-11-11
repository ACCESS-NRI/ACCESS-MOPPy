#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/.."
source /etc/profile.d/modules.sh
module purge
module use /opt/Modules/modulefiles /apps/Modules/modulefiles
module load python3/3.11.7
python3 -m venv .are-venv
.are-venv/bin/python -m pip install --upgrade '.[are]'
chmod 755 deploy/are/bin/jupyter
install -m 644 deploy/are/modulefile /g/data/xp65/public/modules/moppy-studio

"""Launch short probes in the user's selected scientific environment."""

import json
import os
import shlex
import subprocess
from pathlib import Path

from .configuration import module_setup

BRIDGE = Path(__file__).resolve().with_name("bridge.py")


class Runtime:
    def call(self, action, payload, module, timeout=90):
        if action not in ("inspect", "initialize"):
            raise ValueError("Unsupported runtime action")
        command = (
            module_setup(module)
            # Run the standalone bridge without adding the web environment's
            # site-packages to analysis3's import path.
            + f"exec python {shlex.quote(str(BRIDGE))} {action}"
        )
        env = {
            k: v
            for k, v in os.environ.items()
            if k not in ("PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV")
        }
        proc = subprocess.run(
            ["/bin/bash", "-c", command],
            input=json.dumps(payload),
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
            cwd=BRIDGE.parent,
        )
        lines = [
            s.partition("MOPPY_STUDIO_JSON=")[2]
            for s in proc.stdout.splitlines()
            if s.startswith("MOPPY_STUDIO_JSON=")
        ]
        if not lines:
            raise RuntimeError(
                (proc.stderr or proc.stdout or "MOPPy runtime unavailable")[-4000:]
            )
        result = json.loads(lines[-1])
        if proc.returncode or result.get("error"):
            raise ValueError(result.get("error", proc.stderr[-3000:]))
        return result

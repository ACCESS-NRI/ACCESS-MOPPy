"""Launch Studio behind ARE's password-protected Jupyter server.

Optional dependencies are installed with the package's [are] extra. This entry point
consumes ARE's generated config; it does not register a new dashboard tile.
"""

from __future__ import annotations

import argparse
import os
import re
import secrets
import sys
from pathlib import Path

from jupyter_server.auth.login import LoginFormHandler
from jupyter_server.base.handlers import JupyterHandler
from jupyter_server.serverapp import ServerApp
from tornado import web
from traitlets.config import Config
from traitlets.config.loader import PyFileConfigLoader


class ARELoginHandler(LoginFormHandler):
    def check_xsrf_cookie(self):
        # ARE posts its generated password directly from the dashboard. That
        # first login has no Jupyter XSRF cookie yet. Keep this exception local
        # to login; the password is still checked by Jupyter's identity provider.
        if self.request.method != "POST":
            super().check_xsrf_cookie()

    def _redirect_safe(self, url, default=None):
        # Studio sends this cookie back as a header on API writes.
        self.xsrf_token
        super()._redirect_safe(url, default)


class StudioRedirect(JupyterHandler):
    @web.authenticated
    def get(self):
        self.xsrf_token
        self.redirect(self.base_url + "studio/")


def _jupyter_server_extension_points():
    return [{"module": "moppy_studio.are"}]


def _load_jupyter_server_extension(serverapp):
    # Accept existing ARE links to /lab or /tree as well as the server root.
    base = re.escape(serverapp.base_url.rstrip("/"))
    serverapp.web_app.add_handlers(".*$", [(base + r"/(?:lab|tree)/?", StudioRedirect)])


class StudioServerApp(ServerApp):
    def load_config_file(self, suppress_errors=True):
        # Use only the explicit ARE config, leaving personal Jupyter settings
        # and extensions alone. Their installation is not needed for this pilot.
        pass

    def migrate_config(self):
        pass


def server_config(path: Path) -> Config:
    source = PyFileConfigLoader(path.name, path=str(path.parent)).load_config()
    options = {**source.get("NotebookApp", {}), **source.get("ServerApp", {})}
    password = source.get("PasswordIdentityProvider", {}).get(
        "hashed_password"
    ) or options.get("password")
    if not isinstance(password, str) or not password.startswith(("sha1:", "argon2:")):
        raise ValueError(
            "ARE config must supply a hashed session password; refusing an unauthenticated server"
        )
    base_url = options.get("base_url", "")
    if not isinstance(base_url, str) or not re.fullmatch(
        r"/(?:[A-Za-z0-9_.-]+/)+", base_url
    ):
        raise ValueError("ARE config must supply an absolute base_url ending in /")
    port = options.get("port")
    if not isinstance(port, int) or not 1 <= port <= 65535:
        raise ValueError("ARE config must supply a valid port")

    token = secrets.token_urlsafe(32)
    state_dir = Path(
        os.environ.get(
            "MOPPY_STUDIO_STATE_DIR", Path.home() / ".local/state/moppy-studio-are"
        )
    )
    state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    runtime_dir = path.parent / "studio-runtime"
    runtime_dir.mkdir(mode=0o700, exist_ok=True)
    config = Config()
    config.ServerApp.ip = options.get("ip", "127.0.0.1")
    config.ServerApp.port = port
    config.ServerApp.port_retries = 0
    config.ServerApp.base_url = base_url
    config.ServerApp.default_url = "/studio/"
    config.ServerApp.open_browser = False
    config.ServerApp.allow_remote_access = True
    # Permit ARE even if its proxy forwards the compute node's Host header.
    config.ServerApp.allow_origin = "https://are.nci.org.au"
    config.ServerApp.disable_check_xsrf = False
    config.ServerApp.root_dir = str(state_dir)
    config.ServerApp.terminals_enabled = False
    config.ServerApp.jpserver_extensions = {
        "jupyter_server_proxy": True,
        "moppy_studio.are": True,
    }
    os.environ["JUPYTER_RUNTIME_DIR"] = str(runtime_dir)
    config.ServerApp.cookie_secret_file = str(runtime_dir / "cookie-secret")
    config.IdentityProvider.token = ""
    config.IdentityProvider.login_handler_class = ARELoginHandler
    config.PasswordIdentityProvider.hashed_password = password
    config.PasswordIdentityProvider.password_required = True
    config.PasswordIdentityProvider.allow_password_change = False
    config.ServerProxy.servers = {
        "studio": {
            "command": [
                sys.executable,
                "-m",
                "uvicorn",
                "moppy_studio.app:app",
                "--uds",
                "{unix_socket}",
                "--root-path",
                "{base_url}studio",
                "--no-access-log",
            ],
            "unix_socket": True,
            "absolute_url": False,
            "timeout": 30,
            "environment": {
                "MOPPY_STUDIO_ACCESS_TOKEN": token,
                "MOPPY_STUDIO_REQUIRE_XSRF": "1",
                "MOPPY_STUDIO_STATE_DIR": str(state_dir),
            },
            "request_headers_override": {"X-MOPPY-Token": token},
            "launcher_entry": {"title": "MOPPy Studio"},
        }
    }
    return config


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["lab"])
    parser.add_argument("--config", default=os.environ.get("CONFIG_FILE"))
    # ARE supplies this for Jupyter's file browser; Studio uses its own state.
    parser.add_argument("--FileContentsManager.root_dir", dest="unused_root")
    args = parser.parse_args()
    if not args.config:
        parser.error("An ARE-generated --config file is required")
    os.umask(0o077)
    try:
        config = server_config(Path(args.config).expanduser().resolve())
    except (ValueError, OSError) as exc:
        parser.error(str(exc))
    server = StudioServerApp(config=config)
    server.initialize(argv=[], find_extensions=False)
    server.start()


if __name__ == "__main__":
    main()

"""Exercise an actual password login, proxy, and Studio subprocess locally."""

import http.cookiejar
import importlib.util
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin
from urllib.request import HTTPCookieProcessor, Request, build_opener

AVAILABLE = all(
    importlib.util.find_spec(name)
    for name in ("jupyter_server", "jupyter_server_proxy")
)


@unittest.skipUnless(
    AVAILABLE, "Install access-moppy-studio[are] to test the ARE launcher"
)
class ARETests(unittest.TestCase):
    def test_authenticated_direct_launch(self):
        from jupyter_server.auth.security import passwd

        from moppy_studio.are import server_config

        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                port = sock.getsockname()[1]
            base = f"http://127.0.0.1:{port}/node/test/{port}/"
            config = directory / "config.py"
            config.write_text(
                "c.NotebookApp.ip = '127.0.0.1'\n"
                f"c.NotebookApp.port = {port}\n"
                f"c.NotebookApp.base_url = '/node/test/{port}/'\n"
                f"c.NotebookApp.password = {passwd('pilot-test-password', algorithm='sha1')!r}\n"
                "c.NotebookApp.allow_origin = '*'\n"
                "c.NotebookApp.disable_check_xsrf = True\n",
                encoding="utf-8",
            )
            report = directory / "report"
            report.mkdir()
            (report / "index.html").write_text("<h1>Test report</h1>")
            env = {**os.environ, "MOPPY_STUDIO_STATE_DIR": str(directory / "state")}
            with (directory / "server.log").open("w+") as log:
                process = subprocess.Popen(
                    [
                        sys.executable,
                        "-m",
                        "moppy_studio.are",
                        "lab",
                        "--config=" + str(config),
                        "--FileContentsManager.root_dir=" + str(directory),
                    ],
                    cwd=directory,
                    env=env,
                    stdout=log,
                    stderr=log,
                )
                try:
                    jar = http.cookiejar.CookieJar()
                    browser = build_opener(HTTPCookieProcessor(jar))
                    deadline = time.monotonic() + 30
                    while time.monotonic() < deadline:
                        try:
                            with browser.open(base + "login", timeout=1) as response:
                                self.assertEqual(response.status, 200)
                            break
                        except (URLError, TimeoutError):
                            if process.poll() is not None:
                                log.seek(0)
                                self.fail(log.read())
                            time.sleep(0.1)
                    else:
                        self.fail("Pilot server did not become ready")

                    with browser.open(base + "studio/api/health") as response:
                        self.assertIn("/login?", response.url)

                    # Mimic ARE's first password POST, before any XSRF cookie.
                    jar.clear()
                    with self.assertRaises(HTTPError) as error:
                        browser.open(
                            Request(
                                base + "login",
                                data=urlencode({"password": "wrong"}).encode(),
                            )
                        )
                    self.assertEqual(error.exception.code, 401)
                    error.exception.close()
                    jar.clear()
                    login = Request(
                        base + "login?next=" + urljoin("/", f"node/test/{port}/lab"),
                        data=urlencode({"password": "pilot-test-password"}).encode(),
                        headers={"Origin": "https://are.nci.org.au"},
                    )
                    with browser.open(login) as response:
                        self.assertEqual(response.url, base + "studio/")
                        html = response.read().decode()
                        self.assertIn('href="style.css', html)
                        self.assertIn('src="app.js', html)
                    for resource in (
                        "style.css",
                        "app.js",
                        "moppy-logo.png",
                        "api/health",
                        "api/runs",
                    ):
                        with browser.open(base + "studio/" + resource) as response:
                            self.assertEqual(response.status, 200, resource)
                    payload = json.dumps({"yaml": "variables: [Amon.tas]"}).encode()
                    with self.assertRaises(HTTPError) as error:
                        browser.open(
                            Request(
                                base + "studio/api/config/parse",
                                data=payload,
                                headers={"Content-Type": "application/json"},
                            )
                        )
                    self.assertEqual(error.exception.code, 403)
                    error.exception.close()
                    xsrf = next(
                        cookie.value for cookie in jar if cookie.name == "_xsrf"
                    )
                    headers = {
                        "Content-Type": "application/json",
                        "X-XSRFToken": xsrf,
                        "X-MOPPY-Token": "untrusted-client-value",
                        "Origin": "https://are.nci.org.au",
                    }
                    with browser.open(
                        Request(
                            base + "studio/api/config/parse",
                            data=payload,
                            headers=headers,
                        )
                    ) as response:
                        self.assertEqual(response.status, 200)
                        self.assertEqual(response.headers["Cache-Control"], "no-store")
                        self.assertEqual(
                            json.load(response)["config"]["variables"], ["Amon.tas"]
                        )
                    # Unrelated Jupyter APIs still enforce XSRF protection.
                    with self.assertRaises(HTTPError) as error:
                        browser.open(
                            Request(
                                base + "api/contents/test",
                                data=b"{}",
                                method="PUT",
                                headers={"Content-Type": "application/json"},
                            )
                        )
                    self.assertEqual(error.exception.code, 403)
                    error.exception.close()
                finally:
                    process.terminate()
                    try:
                        process.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)

            config.write_text("c.NotebookApp.port = 12345\n")
            with self.assertRaisesRegex(ValueError, "hashed session password"):
                server_config(config)


if __name__ == "__main__":
    unittest.main()

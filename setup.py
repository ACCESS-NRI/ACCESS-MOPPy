from setuptools import setup

import versioneer

setup(
    name="access_mopper",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
)

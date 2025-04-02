from setuptools import setup

import versioneer

setup(
    name="your_package_name",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
)

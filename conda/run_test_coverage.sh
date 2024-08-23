#!/bin/bash
pip install coverage pytest-cov
python -m pytest --cov=mopper --cov-report xml:/tmp/artefacts/tests/pytest/coverage.xml --junit-xml /tmp/artefacts/tests/pytest/results.xml
python -m pytest --cov=mopdb --cov-report xml:/tmp/artefacts/tests/pytest/coverage.xml --junit-xml /tmp/artefacts/tests/pytest/results.xml


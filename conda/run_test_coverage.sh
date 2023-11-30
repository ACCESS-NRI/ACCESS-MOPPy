#!/bin/bash
pip install coverage pytest-cov
py.test --cov=mopper --cov-report xml:/tmp/artefacts/tests/pytest/coverage.xml --junit-xml /tmp/artefacts/tests/pytest/results.xml
py.test --cov=mopdb --cov-report xml:/tmp/artefacts/tests/pytest/coverage.xml --junit-xml /tmp/artefacts/tests/pytest/results.xml


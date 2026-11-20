"""Fixtures shared by local project CMORisation tests."""

import pytest


@pytest.fixture
def project_cv():
    return {
        "activity_id": {"ACCESS-DEV": "ACCESS model development"},
        "institution_id": {"ACCESS-NRI": "ACCESS-NRI"},
        "experiment_id": {
            "om3-real-run": {
                "experiment": "ACCESS-OM3 development run",
                "description": "Evaluation of the actual om3-real-run configuration",
                "activity_id": ["ACCESS-DEV"],
                "parent_experiment_id": ["none"],
                "required_model_components": ["OGCM"],
                "sub_experiment_id": ["none"],
            }
        },
        "source_id": {
            "ACCESS-OM3": {
                "label": "ACCESS-OM3 development configuration",
                "institution_id": ["ACCESS-NRI"],
                "model_component": {
                    "ocean": {
                        "description": "MOM6 ocean",
                        "native_nominal_resolution": "25 km",
                    },
                    "seaIce": {
                        "description": "CICE sea ice",
                        "native_nominal_resolution": "25 km",
                    },
                },
            }
        },
    }

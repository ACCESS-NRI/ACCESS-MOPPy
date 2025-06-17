import importlib.resources as resources
import json
from dataclasses import dataclass, field

import yaml

from access_mopper import _creator


@dataclass
class ACCESS_Dataset:
    # General attributes
    Conventions: str = "CF-1.7, ACDD-1.3"
    comment: str = (
        "post-processed using ACCESS-MOPPeR v2, please contact ACCESS-NRI for questions"
    )
    license: str = "https://creativecommons.org/licenses/by/4.0/"

    # General information for ACCESS models
    source_id: str = ""
    source: str = ""
    keywords: str = ""
    references: str = ""
    forcing: str = ""
    calendar: str = ""
    grid: str = ""
    grid_label: str = ""
    nominal_resolution: str = ""
    parent: bool = None
    tracking_id_prefix: str = ""


@dataclass
class ACCESS_Experiment(ACCESS_Dataset):
    title: str = ""
    exp_description: str = ""
    product_version: str = ""
    date_created: str = ""
    time_coverage_start: str = ""
    time_coverage_end: str = ""
    outpath: str = "MOPPeR_outputs"

    creator_name: str = field(default_factory=lambda: _creator.creator_name)
    creator_email: str = field(default_factory=lambda: _creator.creator_email)
    creator_url: str = field(default_factory=lambda: _creator.creator_url)
    organisation: str = field(default_factory=lambda: _creator.organisation)

    mapping_file: str = ""

    def initialise(self, access_configuration):
        with (
            resources.files("access_mopper")
            .joinpath("ACCESS_configurations.yml")
            .open() as f
        ):
            yaml_data = yaml.safe_load(f)
        attributes = yaml_data[access_configuration]

        for key, value in attributes.items():
            if hasattr(self, key):
                setattr(self, key, value)

    # Method to save the instance data to a file
    def save_to_file(self, file_path: str):
        # Convert the dataclass to a dictionary and then to a JSON string
        json_data = json.dumps(self.__dict__, indent=4)

        # Write the JSON string to a file
        with open(file_path, "w") as f:
            f.write(json_data)
        print(f"Data saved to {file_path}")

    @classmethod
    def get_mapping(cls, compound_name):
        mip_name, cmor_name = compound_name.split(".")
        filename = f"{cls.mapping_file_prefix}{mip_name}.json"
        # Use importlib.resources to access the file
        with (
            resources.files("access_mopper.mappings")
            .joinpath(filename)
            .open("r") as file
        ):
            data = json.load(file)
        return data[cmor_name]

    @classmethod
    def mapping_info(cls, compound_name):
        """
        Prints the mapping information for a given compound name in a notebook-friendly format.

        Args:
            compound_name (str): The compound name in the format "MIP_table.CMOR_variable".
        """
        from IPython.display import Markdown, display

        # Get the mapping data
        mapping = cls.get_mapping(compound_name)

        # Extract relevant information
        mip_table, cmor_name = compound_name.split(".")
        cf_name = mapping.get("CF standard name", "N/A")
        model_variables = mapping.get("model_variables", [])
        formula = mapping.get("calculation", {}).get("formula", "N/A")

        # Format the output as Markdown
        output = f"""
    ### Mapping Information for `{compound_name}`
    - **Compound Name**: `{compound_name}`
    - **CF Standard Name**: `{cf_name}`
    - **Required Variables**: `{", ".join(model_variables)}`
    - **Formula**: `{formula}`
        """

        # Display the Markdown content
        display(Markdown(output.strip()))


@dataclass
class CMIP6_Experiment(ACCESS_Experiment):
    Conventions: str = ""
    institution_id: str = ""
    source_id: str = ""
    source_type: str = "AOGCM"
    experiment_id: str = ""
    activity_id: str = "CMIP"
    realization_index: str = ""
    initialization_index: str = ""
    physics_index: str = ""
    forcing_index: str = ""
    tracking_prefix: str = "hdl:21.14100"
    parent_experiment_id: str = "none"
    parent_activity_id: str = "none"
    parent_source_id: str = "none"
    parent_variant_label: str = "none"
    sub_experiment: str = "none"
    sub_experiment_id: str = "none"
    branch_method: str = "none"
    branch_time_in_child: str = ""
    branch_time_in_parent: str = ""
    _controlled_vocabulary_file: str = "CMIP6_CV.json"
    _AXIS_ENTRY_FILE: str = "CMIP6_coordinate.json"
    _FORMULA_VAR_FILE: str = "CMIP6_formula_terms.json"
    _cmip6_option: str = "CMIP6"
    mip_era: str = "CMIP6"
    parent_mip_era: str = "CMIP6"
    parent_time_units: str = ""
    _history_template: str = "%s ;rewrote data to be consistent with <activity_id> for variable <variable_id> found in table <table_id>."
    output_path_template: str = "<mip_era><activity_id><institution_id><source_id><experiment_id><_member_id><table><variable_id><grid_label><version>"
    output_file_template: str = (
        "<variable_id><table><source_id><experiment_id><_member_id><grid_label>"
    )
    license: str = "CMIP6 model data produced by CSIRO is licensed under a Creative Commons Attribution-ShareAlike 4.0 International License (https://creativecommons.org/licenses/). Consult https://pcmdi.llnl.gov/CMIP6/TermsOfUse for terms of use governing CMIP6 output, including citation requirements and proper acknowledgment.  Further information about this data, including some limitations, can be found via the further_info_url (recorded as a global attribute in this file). The data producers and data providers make no warranty, either express or implied, including, but not limited to, warranties of merchantability and fitness for a particular purpose. All liabilities arising from the supply of the information (including any liability arising in negligence) are excluded to the fullest extent permitted by law."

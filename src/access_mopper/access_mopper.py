import yaml
from typing import Dict, Any
from dataclasses import dataclass, asdict
import json


def load_yaml(file_path: str) -> Dict[str, Any]:
    with open(file_path, "r") as file:
        return yaml.safe_load(file)


@dataclass
class Creator:
    institution: str = None
    organisation: str = None
    contact: str = None
    creator_name: str = None
    creator_email: str = None
    creator_url: str = None

    def __post__init__(self):
        try:
            with open('user_profile.yaml', 'r') as file:
                creator = yaml.safe_load(file)
        except FileNotFoundError:
            print(".user_config.yaml file not found, using default values.")

        for key, value in creator.items():
            if hasattr(self, key):
                setattr(self, key, value)            
        

@dataclass
class ACCESS_Dataset:

    # General attributes
    Conventions: str = "CF-1.7, ACDD-1.3"
    comment: str = "post-processed using ACCESS-MOPPeR v2, please contact ACCESS-NRI for questions"
    license: str = "https://creativecommons.org/licenses/by/4.0/"
    
    # General information for ACCESS models
    source_id: str = None
    source: str = None
    keywords: str = None
    references: str = None
    forcing: str = None
    calendar: str = None
    grid: str = None
    grid_label: str = None
    nominal_resolution: str = None
    parent: bool = None
    tracking_id_prefix: str = None


    def initialise(self, access_configuration):
        yaml_data = load_yaml("ACCESS_configurations.yaml")
        access_cm2_attributes = yaml_data[access_configuration]

        for key, value in access_cm2_attributes.items():
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

    def to_json(self):
        return json.dumps(asdict(self), indent=4)

@dataclass
class ACCESS_Experiment(Creator, ACCESS_Dataset):
    title: str = None
    exp_description: str = None
    product_version: str = None
    date_created: str = None
    time_coverage_start: str = None
    time_coverage_end: str = None
    outpath: str = "MOPPeR_outputs"


@dataclass
class CMIP6_Experiment(ACCESS_Experiment):
    Conventions: str = "CF-1.7 CMIP-6.2" 
    institution_id: str = None
    source_id: str = None
    source_type: str = "AOGCM"
    experiment_id: str = None
    activity_id: str = "CMIP"
    realization_index: str = None
    initialization_index: str = None
    physics_index: str = None
    forcing_index: str = None
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
    parent_time_units: str = None
    _history_template: str = "%s ;rewrote data to be consistent with <activity_id> for variable <variable_id> found in table <table_id>."
    output_path_template: str = "<mip_era><activity_id><institution_id><source_id><experiment_id><_member_id><table><variable_id><grid_label><version>"
    output_file_template: str = "<variable_id><table><source_id><experiment_id><_member_id><grid_label>"
    license: str = "CMIP6 model data produced by CSIRO is licensed under a Creative Commons Attribution-ShareAlike 4.0 International License (https://creativecommons.org/licenses/). Consult https://pcmdi.llnl.gov/CMIP6/TermsOfUse for terms of use governing CMIP6 output, including citation requirements and proper acknowledgment.  Further information about this data, including some limitations, can be found via the further_info_url (recorded as a global attribute in this file). The data producers and data providers make no warranty, either express or implied, including, but not limited to, warranties of merchantability and fitness for a particular purpose. All liabilities arising from the supply of the information (including any liability arising in negligence) are excluded to the fullest extent permitted by law."
   
@dataclass
class ACCESS_CM2(ACCESS_Experiment):

    def __post_init__(self):
        ACCESS_Dataset.initialise(self, "ACCESS-CM2")

@dataclass
class ACCESS_CM2_CMIP6(CMIP6_Experiment):
    
    def __post_init__(self):
        ACCESS_Dataset.initialise(self, "ACCESS-CM2")
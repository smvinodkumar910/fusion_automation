import os
import json

ROOT_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), ''))

print(ROOT_DIR)

with open(os.path.join(ROOT_DIR, 'config', 'fusion_to_bq_sync_config.json')) as f:
    config_file = json.load(f)

ENVIRONMENT_NAME = config_file["environment"]


def get_configs(script_file_name: str):
    COMPOSER_ENV_NAME = os.environ["COMPOSER_ENVIRONMENT"]
    pipelines = ""
    if config_file["environment"] == COMPOSER_ENV_NAME:
        pipeline_configs = config_file.get("pipeline-configurations")
        for pipeline_config in pipeline_configs:
            pipelines = pipelines + pipeline_config.get("pipeline-name") + "::"
            if pipeline_config.get("pipeline-name") == script_file_name:
                task_configs = pipeline_config["task-configurations"]
                break
        if (task_configs == None):
            raise Exception(pipelines, "No valid task configuration was found for this pipeline ")
        else:
            return task_configs
    else:
        raise Exception("No valid pipeline configuration was found for the current composer environment")


# print(config_file['helix-automation'])
helix_automation = config_file['helix-automation']

PVO_LIST_PATH = helix_automation['pvo_list_path']
BASE_PVOS_PATH = helix_automation['base_file_path']
BICC_FILE_PATH = helix_automation['bicc_file_path']
DDL_TGT_PATH = helix_automation['ddl_target_path']
STG_SCHEMA = helix_automation['STG_SCHEMA']
RAW_SCHEMA = helix_automation['RAW_SCHEMA']
DW_SCHEMA = helix_automation['DW_SCHEMA']
HIST_SCHEMA = helix_automation['HIST_SCHEMA']
BUCKET_NAME = helix_automation['bucket_name']
PROJECT_NAME = helix_automation['project_name']
TEMP_PATH = helix_automation['temp_path']
REST_API_URL = helix_automation['rest_api_url']
REST_API_USER = helix_automation['rest_api_user']
REST_API_SECRET = helix_automation['rest_api_secret']
META_DATASET_NAME = helix_automation['meta_dataset_name']
DATA_PROJECT_NAME = helix_automation['data_project_name']
FUSION_SCHEMA = helix_automation['fusion_schema']
pvo_meta_path = helix_automation['pvo_meta_path']

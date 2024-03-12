import os
import json

ROOT_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), ''))

#print(ROOT_DIR)

with open(os.path.join(ROOT_DIR, 'config', 'fusion_to_bq_sync_config.json')) as f:
    config_file = json.load(f)

ENVIRONMENT_NAME = config_file["environment"]

class DataSets:
    def __init__(self, module:str):
        if module != "common":    
            module_datasets = config_file.get('helix-automation').get('bq_datasets').get(module)
            self.STG_SCHEMA = module_datasets['STG_SCHEMA']
            self.RAW_SCHEMA = module_datasets['RAW_SCHEMA']
            self.DW_SCHEMA = module_datasets['DW_SCHEMA']
            self.HIST_SCHEMA = module_datasets['HIST_SCHEMA']
            self.VIEW_SCHEMA = module_datasets['VIEW_SCHEMA']
            self.DATA_BUCKET_NAME = module_datasets['DATA_BUCKET_NAME']
            self.PLATFORM_BUCKET_NAME = module_datasets['PLATFORM_BUCKET_NAME']
        
        self.MODULES_LIST = list(config_file.get('helix-automation').get('bq_datasets').keys())
        helix_automation = config_file.get('helix-automation')
        self.ROOT_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), ''))
        self.BUCKET_NAME = helix_automation['bucket_name']
        self.BIP_LIST_PATH = helix_automation['bip_list_path']
        self.PVO_LIST_PATH = helix_automation['pvo_list_path']
        self.BASE_PVOS_PATH = helix_automation['base_file_path']
        self.BASE_BIP_PATH = helix_automation['base_bip_file_path']
        self.BICC_FILE_PATH = helix_automation['bicc_file_path']
        self.DDL_TGT_PATH = helix_automation['ddl_target_path']
        self.PROJECT_NAME = helix_automation['project_name']
        self.TEMP_PATH = helix_automation['temp_path']
        self.REST_API_URL = helix_automation['rest_api_url']
        self.REST_API_USER = helix_automation['rest_api_user']
        self.REST_API_SECRET = helix_automation['rest_api_secret']
        self.META_DATASET_NAME = helix_automation['meta_dataset_name']
        self.DATA_PROJECT_NAME = helix_automation['data_project_name']
        self.FUSION_SCHEMA = helix_automation['fusion_schema']
        self.pvo_meta_path = helix_automation['pvo_meta_path']
        # New configs for dag config json
        self.GCS2BQ_TEMPLATE_ID = helix_automation['gcs2bq_template_id']
        self.DATA_MERGE_TEMPLATE_ID = helix_automation['data_merge_template_id']
        self.SOFTDEL_MERGE_TEMPLATE_ID = helix_automation['softdel_merge_template_id']
        self.SOURCE_OBJECTS_PATH = helix_automation['source_objects_path']
        self.SCHEMA_OBJECT_PATH = helix_automation['schema_object_path']
        self.UCM2GCS_TASK_ID = helix_automation['ucm2gcs_task_id']
        self.ARCHIVE_TASK_ID = helix_automation['archive_task_id']
        # New configs for BIP integration
        self.BIP_CF_URL = helix_automation['bip_cf_url']
        self.BIP_CF_PROPS = helix_automation['bip_cf_props']
        self.BIP_TABLE_SCH_PATH = "gs://"+self.BIP_CF_PROPS.get('bucketName')+'/'+self.BIP_CF_PROPS.get('table_schema_tgt_path')
        self.BIP_REQ_COL_TGT_PATH = "gs://"+self.BIP_CF_PROPS.get('bucketName')+'/'+self.BIP_CF_PROPS.get('req_col_tgt_path')
        #For handling specific columns
        self.SAMPLE_EXTRACT_PATH = helix_automation['sample_extract_path']
        #print(self.BIP_TABLE_SCH_PATH)
        #print(self.BIP_REQ_COL_TGT_PATH)


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
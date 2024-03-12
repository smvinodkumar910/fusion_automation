from google.cloud import bigquery
import json
from fusbq.path_config import DataSets
pc = DataSets('common')
import os
import sqlparse
import pandas as pd
from google.cloud import storage

base_excel_path = pc.BASE_PVOS_PATH

# new configs
ucm2gcs_task = pc.UCM2GCS_TASK_ID
archive_task = pc.ARCHIVE_TASK_ID
gcs_to_bq_template = pc.GCS2BQ_TEMPLATE_ID
merge_template = pc.DATA_MERGE_TEMPLATE_ID
softdel_merge_template = pc.SOFTDEL_MERGE_TEMPLATE_ID
source_objects_path = pc.SOURCE_OBJECTS_PATH
schema_object_path = pc.SCHEMA_OBJECT_PATH

# Existing config
'''
data_project_name = pc.DATA_PROJECT_NAME
gcs_bucket = pc.BUCKET_NAME
stg_schema = pc.STG_SCHEMA
dl_schema = pc.HIST_SCHEMA
raw_schema = pc.RAW_SCHEMA
'''
task_output_path = os.path.join(pc.ROOT_DIR, pc.DDL_TGT_PATH)


def create_stg_load_tasks(task_type, dicts):
    tasklist = []
    for item in dicts:
        pc = DataSets(item.get('MODULE'))
        # Derive file name format
        if item.get('SOURCE_TYPE')=='BICC':
            file_name = item.get('DATASTORE_NAME').lower()
            file_name = 'file_' + file_name.replace('.', '_') + '-batch'
            file_name = file_name + '*.csv' if task_type == 'data_tasks_json' else file_name + '*.pecsv'
        else:
            file_name = item.get('SOURCE_TABLE_NAME')+'*.csv'
            file_name = file_name if task_type == 'data_tasks_json' else 'PK_'+file_name  

        stg_table_name = item.get('STG_TABLE_NAME') if task_type == 'data_tasks_json' else item.get('STG_DEL_TABLE_NAME')

        task = dict()
        task['task_name'] = item.get('TABLE_ID') + '_STG_LOAD'
        task['template_reference'] = gcs_to_bq_template
        task['task_type'] = 'EXECUTION'

        config = dict()
        config['bucket'] = pc.DATA_BUCKET_NAME
        config['schema_object_bucket'] = pc.PLATFORM_BUCKET_NAME
        config['source_objects'] = [item.get('MODULE').lower() + source_objects_path + file_name]
        config['schema_object'] = item.get('MODULE').lower() + schema_object_path + stg_table_name + '.json'
        config['destination_project_dataset_table'] = '{0}.{1}.{2}'.format(pc.DATA_PROJECT_NAME, pc.STG_SCHEMA,
                                                                           stg_table_name)
        task['config'] = config
        task['upstream_tasks'] = ["LOAD_UCM_FILE_EXTRACT", "LOAD_UCM_JSON_DETAIL"]
        tasklist.append(task)
    return tasklist


def create_merge_tasks(task_type, dicts):
    tasklist = []
    for item in dicts:
        pc = DataSets(item.get('MODULE'))
        task = dict()
        task['task_name'] = item.get('TABLE_ID') + '_MERGE'
        config = dict()
        if task_type == 'data_tasks_json':
            task['template_reference'] = merge_template
            config['stg_table'] = '{0}.{1}'.format(pc.STG_SCHEMA, item.get('STG_TABLE_NAME'))
        elif task_type == 'del_tasks_json':
            task['template_reference'] = softdel_merge_template
            config['stg_table'] = '{0}.{1}'.format(pc.STG_SCHEMA, item.get('STG_DEL_TABLE_NAME'))

        task['task_type'] = 'MERGE'

        
        if task_type == 'data_tasks_json':
            config['dl_table'] = '{0}.{1}'.format(pc.HIST_SCHEMA, item.get('HIST_TABLE_NAME'))
        config['raw_table'] = '{0}.{1}'.format(pc.RAW_SCHEMA, item.get('RAW_TABLE_NAME'))
        config['primary_key_columns'] = item.get('PRIMARY_KEY')
        config['incremental_date_columns'] = item.get('INCREMENTAL_KEY')
        task['config'] = config
        task['upstream_tasks'] = [item.get('TABLE_ID') + '_STG_LOAD']
        task['downstream_tasks'] = [archive_task]
        tasklist.append(task)

    return tasklist


def create_dw_tasks(dicts):
    tasklist = []
    for item in dicts:
        pc = DataSets(item.get('MODULE'))

        task = dict()
        task['task_name'] = item.get('TABLE_ID') + '_DW_SIL_PROC_EXEC'
        task['template_reference'] = "SQL_DW_PROC_1"
        task['task_type'] = 'EXECUTION'

        config = dict()
        config['sql_procedure_name'] = "{}.{}.{}".format(pc.DATA_PROJECT_NAME, pc.DW_SCHEMA ,item.get('DW_SIL_PROC'))
        config['parameters'] = r" '{{ get_audit_param('dag_run_id') }}' , '{{ get_audit_param('from_delta_key') }}' , '{{ get_audit_param('to_delta_key') }}',  {{ get_audit_param('adhoc_load_flag') }} "
        task['config'] = config
        task['upstream_tasks'] = ["PRE_TRANSFORM_TASK"]
        task['downstream_tasks'] = ["POST_TRANSFORM_TASK"]
        tasklist.append(task)
    
    return tasklist




def create_task_list(tasktype: str, where_clause: str):
    client = bigquery.Client(pc.DATA_PROJECT_NAME)
    sql = 'select TABLE_ID, SOURCE_TABLE_NAME, DATASTORE_NAME,\
            STG_TABLE_NAME, \
            RAW_TABLE_NAME,\
            DW_TABLE_NAME,\
            DW_SIL_PROC,\
            RUN_DDL_FLAG,\
            MODULE,\
            HIST_TABLE_NAME,\
            PRIMARY_KEY,\
            INCREMENTAL_KEY,\
            STG_DEL_TABLE_NAME, \
            SOURCE_TYPE, \
            TRACK_NAME \
            from ' \
          'helix-data-dev.BQ_CTL_METADATA.FUSION_TO_BQ_PVO_DTL {0} ORDER BY TABLE_ID ASC'.format(
        where_clause)
    # sql = sqlparse.format(sql, reindent=True, keyword_case='upper')
    print(sqlparse.format(sql, reindent=True, keyword_case='upper'))
    df = client.query(sql).to_dataframe()
    '''
    pvodf = pd.read_csv(base_excel_path)
    dicts = pvodf.to_dict('records')
    '''
    dicts = df.to_dict(orient='records')
    client = storage.Client(pc.PROJECT_NAME)
    bucket = client.get_bucket(pc.BUCKET_NAME)
    if tasktype!='tranformation_tasks_json':
        stg_task_list = create_stg_load_tasks(tasktype, dicts)
        merge_task_list = create_merge_tasks(tasktype, dicts)
        stg_task_list.extend(merge_task_list)
    
        os.makedirs(os.path.join(task_output_path), exist_ok=True)
        with open(os.path.join(task_output_path, 'TASK_LISTS.json'), 'w') as file:
            file.write(json.dumps(stg_task_list, indent=2))

        
        blob = bucket.blob(pc.DDL_TGT_PATH +'TASK_LISTS.json')
        blob.upload_from_string(json.dumps(stg_task_list, indent=2))
    else:
        transform_task_list = create_dw_tasks(dicts)
        os.makedirs(os.path.join(task_output_path), exist_ok=True)
        with open(os.path.join(task_output_path, 'TASK_LISTS.json'), 'w') as file:
            file.write(json.dumps(transform_task_list, indent=2))
        
        blob = bucket.blob(pc.DDL_TGT_PATH +'TASK_LISTS.json')
        blob.upload_from_string(json.dumps(stg_task_list, indent=2))

    
#create_task_list('data_tasks_json','')

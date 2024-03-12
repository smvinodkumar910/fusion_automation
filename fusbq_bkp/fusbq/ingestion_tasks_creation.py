from google.cloud import bigquery
import json
import fusbq.path_config as pc
import os
import sqlparse

gcs_to_bq_template = 'FUSION_GCS2BQ'
merge_template = 'FUSION-MERGE-1'
data_project_name = 'helix-data-dev'
gcs_bucket = 'helix-finance-test'
source_objects_path = 'gl_data_test/load_gl_test/uat_first_test_bring_all_oracle/archive_gl/'
schema_object_path = 'gl_data_test/load_gl_test/fusion_schema/'
stg_schema = 'helix_stg'
dl_schema = 'helix_dl'
raw_schema = 'helix_raw'
task_output_path = os.path.join(pc.ROOT_DIR, pc.DDL_TGT_PATH)


def create_stg_load_tasks(task, dicts):
    tasklist = []
    for item in dicts:
        if task == 'data_tasks_json':
            stg_table_name = item.get('STG_TABLE_NAME')
        elif task == 'del_tasks_json':
            stg_table_name = item.get('STG_DEL_TABLE_NAME')

        task = dict()
        task['task_name'] = item.get('TABLE_ID') + '_STG_LOAD'
        task['template_reference'] = gcs_to_bq_template
        task['task_type'] = 'EXECUTION'

        config = dict()
        config['bucket'] = gcs_bucket

        # Derive file name format
        file_name = item.get('DATASTORE_NAME').lower()
        file_name = 'file_' + file_name.replace('.', '_')

        config['source_objects'] = source_objects_path + file_name + '*.csv'
        config['schema_object'] = schema_object_path + stg_table_name + '.json'
        config['destination_project_dataset_table'] = '{0}.{1}.{2}'.format(data_project_name, stg_schema,
                                                                           stg_table_name)
        task['config'] = config
        tasklist.append(task)
    return tasklist


def create_merge_tasks(dicts):
    tasklist = []
    for item in dicts:
        task = dict()
        task['task_name'] = item.get('TABLE_ID') + '_MERGE'
        task['template_reference'] = gcs_to_bq_template
        task['task_type'] = 'EXECUTION'

        config = dict()
        config['stg_table'] = '{0}.{1}'.format(stg_schema, item.get('STG_TABLE_NAME'))
        config['dl_table'] = '{0}.{1}'.format(dl_schema, item.get('HIST_TABLE_NAME'))
        config['raw_table'] = '{0}.{1}'.format(raw_schema, item.get('RAW_TABLE_NAME'))
        config['primary_key_columns'] = item.get('PRIMARY_KEY')
        config['incremental_date_columns'] = item.get('INCREMENTAL_KEY')
        task['config'] = config
        task['upstream_tasks'] = [item.get('TABLE_ID') + '_STG_LOAD']
        tasklist.append(task)

    return tasklist


def create_task_list(tasktype: str, module: str):
    client = bigquery.Client(data_project_name)
    sql = 'select TABLE_ID, SOURCE_TABLE_NAME, DATASTORE_NAME,\
            STG_TABLE_NAME, \
            RAW_TABLE_NAME,\
            DW_TABLE_NAME,\
            RAW_SIL_PROC,\
            DW_SIL_PROC,\
            RUN_DDL_FLAG,\
            MODULE,\
            HIST_TABLE_NAME,\
            PRIMARY_KEY,\
            INCREMENTAL_KEY,\
            STG_DEL_TABLE_NAME,\
            RAW_DEL_PLP_PROC from ' \
          'helix-data-dev.BQ_CTL_METADATA.FUSION_TO_BQ_PVO_DTL where MODULE = "{0}" ORDER BY TABLE_ID ASC'.format(
        module)
    # sql = sqlparse.format(sql, reindent=True, keyword_case='upper')
    df = client.query(sql).to_dataframe()
    dicts = df.to_dict(orient='records')
    stg_task_list = create_stg_load_tasks(tasktype, dicts)
    merge_task_list = create_merge_tasks(dicts)
    with open(os.path.join(task_output_path, 'gcs2bq_task_list.json'), 'w') as file:
        file.write(json.dumps(stg_task_list, indent=2))
    with open(os.path.join(task_output_path, 'merge_task_list.json'), 'w') as file:
        file.write(json.dumps(merge_task_list, indent=2))

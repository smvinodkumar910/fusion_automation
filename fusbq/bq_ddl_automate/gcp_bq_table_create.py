"""
############################################################################################################################
#EQUINIX CORPORATION - All Rights Reserved.
#---------------------------------------------------------------------------------------------------------------------------
#
#Script Name  : Fusion_PVO_to_Bq_ddl.py
#Purpose      : To create BQ DDL Scripts based on Fusion PVO metadata
#Version      : 1.0
#---------------------------------------------------------------------------------------------------------------------------
#Date            Updated By                Comments
#---------------------------------------------------------------------------------------------------------------------------
#19-Oct-2022     Vinodkumar Madhavan       Initial Creation
############################################################################################################################
"""
import sys
import traceback
from google.cloud import bigquery, storage
import json
from google.cloud.exceptions import NotFound
import pandas as pd
from fusbq.path_config import DataSets
pc = DataSets('common')

client = bigquery.Client(pc.DATA_PROJECT_NAME)


def drop_table_bq(ddl: str):
    sql = ddl.replace('project_name', pc.DATA_PROJECT_NAME)
    job = client.query(sql)  # API request.
    job.result()  # Waits for the query to finish.
    print(
        'Dropped the table "{}.{}.{}".'.format(
            job.destination.project,
            job.destination.dataset_id,
            job.destination.table_id,
        )
    )


def create_table_bq(ddl: str):
    sql = ddl.replace('project_name', pc.DATA_PROJECT_NAME)
    sql = sql.replace('OPTIONS (labels=[label_1])','')
    job = client.query(sql)  # API request.
    job.result()  # Waits for the query to finish.
    print(
        'Created new Table "{}.{}.{}".'.format(
            job.destination.project,
            job.destination.dataset_id,
            job.destination.table_id,
        )
    )


def create_procs(ddl: str, module: str):
    pc = DataSets(module)
    sql = ddl.replace('project_name', pc.DATA_PROJECT_NAME)
    sql = sql.replace('OPTIONS (labels=[label_1])','')
    job = client.query(sql)  # API request.
    job.result()  # Waits for the query to finish.


def read_data_bq(query: str):
    job = client.query(query)
    result = job.result()
    df = result.to_dataframe()
    dict_out = df.to_dict(orient='records')
    print(dict_out)


def bq_table_exists(table_id: str):
    try:
        client.get_table(table_id)  # Make an API request.
        print("Table {} already exists.".format(table_id))
        return True
    except NotFound:
        print("Table {} is not found.".format(table_id))
        return False


def create_table_api(table_name: str, table_type: str, module: str):
    pc = DataSets(module)
    if table_type == 'STG':
        table_id = "{}.{}.{}".format(pc.DATA_PROJECT_NAME, pc.STG_SCHEMA, table_name)
    elif table_type == 'RAW':
        table_id = "{}.{}.{}".format(pc.DATA_PROJECT_NAME, pc.RAW_SCHEMA, table_name)
    elif table_type == 'HIST':
        table_id = "{}.{}.{}".format(pc.DATA_PROJECT_NAME, pc.HIST_SCHEMA, table_name)

    # Get new table schema from gcs
    gcs_client = storage.Client(pc.PROJECT_NAME)
    bucket = gcs_client.get_bucket(pc.BUCKET_NAME)
    print("blob:"+pc.TEMP_PATH + 'JSON_' + table_type + '/' + table_name + '.json')
    blob = bucket.get_blob(pc.TEMP_PATH + 'JSON_' + table_type + '/' + table_name + '.json')
    schema_dtl = json.loads(blob.download_as_string())  # Schema as list of dicts

    # Create BQ Schema object
    schema = []
    for a in schema_dtl:
        schema.append(bigquery.SchemaField(a['name'], a['type'], a['mode']))

    if bq_table_exists(table_id):

        table = client.get_table(table_id)

        # Compare existing table schema vs new table schema
        if table.schema == schema:
            print('Table {} having similar schema, Hence not deployed'.format(table_id))
        else:
            print('Table {} having different schema, Hence replacing table'.format(table_id))
            replace_bq_table_schema(table_id, table.schema, schema, table_type)
    else:
        # Since table not there create the table
        table = bigquery.Table(table_id, schema=schema)

        # Applying partition if HIST table
        if table_type == 'HIST':
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="_metadata_insert_time"  # name of column to use for partitioning
            )
        elif table_type == 'RAW':
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="_metadata_update_time"  # name of column to use for partitioning
            )

        table = client.create_table(table)  # Make an API request.
        set_default_cols(table_id, table_type)
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )


def set_default_cols(table_id: str, table_type: str):
    if table_type in ('HIST', 'RAW'):
        default1 = "ALTER TABLE {0} ALTER COLUMN _metadata_insert_time SET DEFAULT CURRENT_TIMESTAMP;".format(table_id)
        job1 = client.query(default1)  # API request.
        job1.result()  # Waits for the query to finish.

        default2 = "ALTER TABLE {0} ALTER COLUMN _metadata_deleted SET DEFAULT 'N';".format(table_id)
        job2 = client.query(default2)  # API request.
        job2.result()  # Waits for the query to finish.

    elif table_type == 'STG':
        default1 = "ALTER TABLE {0} ALTER COLUMN _metadata_insert_time SET DEFAULT CURRENT_TIMESTAMP;".format(table_id)
        job1 = client.query(default1)  # API request.
        job1.result()  # Waits for the query to finish.

    print('default columns updated for the table {}'.format(table_id))
    return True


def replace_bq_table_schema(table_id: str, old_schema, new_schema, table_type):
    """
    Step 1: Create back up of the given table
    Step 2: Drop the current table
    Step 3: Create current table again with new schema
    Step 4: Import Data from backup table to new table
    Step 5: Drop the backup table
    """

    # Step 1:
    bkp_tb_sql = 'CREATE TABLE {table_id}_bkp as select * from {table_id};'.format(table_id=table_id)
    job = client.query(bkp_tb_sql)  # API request.
    job.result()
    print('Bkp Table created')

    # Step 2:
    client.delete_table(table_id)
    print('Deleted old table')

    # Step 3:
    table = bigquery.Table(table_id, schema=new_schema)
    
     # Applying partition if HIST table
    if table_type == 'HIST':
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="_metadata_insert_time"  # name of column to use for partitioning
        )
    elif table_type == 'RAW':
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="_metadata_update_time"  # name of column to use for partitioning
        )
    
    table = client.create_table(table)  # Make an API request.
    set_default_cols(table_id, table_type)
    print(
        "Created table with new Schema {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )
    

    # Step 4:
    fields = []
    types = []
    for schemaField in old_schema:
        fields.append(schemaField.name)
        types.append(schemaField.field_type)

    old_schema = pd.DataFrame(list(zip(fields, types)), columns=['old_columns_name', 'old_column_type'])

    fields.clear()
    types.clear()

    for schemaField in new_schema:
        fields.append(schemaField.name)
        types.append(schemaField.field_type)

    new_schema = pd.DataFrame(list(zip(fields, types)), columns=['new_columns_name', 'new_column_type'])

    merged_df = pd.merge(old_schema, new_schema, how='inner',
                         left_on=['old_columns_name'],
                         right_on=['new_columns_name'])
    
    merged_df['insert_cols'] = merged_df.apply(lambda
                                                   x: x.old_columns_name if x.new_column_type == x.old_column_type else 'CAST(' + str(x.old_columns_name) + ' as ' + str(x.new_column_type) + ')',
                                               axis=1)

    columns_list = ','.join(merged_df.old_columns_name.values.tolist())
    insert_list = ','.join(merged_df.insert_cols.values.tolist())
    bkp_to_main = 'INSERT INTO {main_tb} ({columns_list}) select {insert_list} ' \
                  'from {main_tb}_bkp;'.format(main_tb=table_id, columns_list=columns_list, insert_list=insert_list)
    #print('import query:'+bkp_to_main)
    try:
        job = client.query(bkp_to_main)  # API request.
        job.result()
        print('Import data from bkp table to new table')
    except Exception as e:
        traceback.print_exc()
        sys.exit(1)

    # Step 5:
    client.delete_table(table_id + '_bkp')
    print('Drop the bkp table')

if __name__ == "__main__":
    create_table_api('DL_ZX_REC_NREC_DIST_BIP','HIST','ZX')
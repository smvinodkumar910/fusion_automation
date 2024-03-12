import os
import fusbq.path_config as pc
import pandas as pd
from google.cloud import storage
pd.set_option('mode.chained_assignment', None)


def stg_integration_id_derive(x):
    if x.type == 'STRING':
        return 'COALESCE(' + x['name'] + ',\'\') '
    elif x.type == 'NUMERIC':
        return 'COALESCE(' + x['name'] + ', 0)'
    else:
        return 'COALESCE(CAST(' + x['name'] + ' as STRING),\'\') '


def raw_integration_id_derive(x):
    if x.type == 'STRING':
        return 'COALESCE(' + x['raw_column_name'] + ',\'\') '
    elif x.type == 'NUMERIC':
        return 'COALESCE(' + x['raw_column_name'] + ', 0)'
    else:
        return 'COALESCE(CAST(' + x['raw_column_name'] + ' as STRING),\'\') '


def generate_raw_del_plp(df: pd.DataFrame, in_stg_table_name, in_raw_table_name):
    df['stg_integration_id'] = df.apply(
        lambda x: stg_integration_id_derive(x),
        axis=1)

    stg_integration_id = ",'~',".join(df.stg_integration_id.values.tolist())
    stg_integration_id = 'concat(' + stg_integration_id + ') '

    #print(stg_integration_id)

    df['raw_integration_id'] = df.apply(
        lambda x: raw_integration_id_derive(x), axis=1)

    raw_integration_id = ",'~',".join(df.raw_integration_id.values.tolist())
    raw_integration_id = 'concat(' + raw_integration_id + ') '

    #print(raw_integration_id)

    project_name = pc.DATA_PROJECT_NAME
    raw_schema = pc.RAW_SCHEMA
    raw_table_name = in_raw_table_name
    proc_name = 'PRC_PLP_' + in_raw_table_name + '_DEL'
    stg_schema = pc.STG_SCHEMA
    stg_table_name = in_stg_table_name

    script_template = "CREATE OR REPLACE PROCEDURE `project_name.{raw_schema}.{proc_name}`() \n" \
                      "BEGIN \n\n" \
                      "UPDATE {raw_schema}.{raw_table_name}\n" \
                      "SET _metadata_deleted= 'N'\n" \
                      "WHERE 1=1;\n\n" \
                      "UPDATE {raw_schema}.{raw_table_name}\n" \
                      "SET _metadata_deleted='Y' \n" \
                      "WHERE {raw_integration_id} NOT IN ( \n" \
                      "SELECT DISTINCT {stg_integration_id} FROM {stg_schema}.{stg_table_name}); " \
                      "END\n"

    proc_str = script_template.format(proc_name=proc_name, raw_schema=raw_schema,
                                      raw_table_name=raw_table_name, stg_schema=stg_schema,
                                      stg_table_name=stg_table_name,
                                      stg_integration_id=stg_integration_id, raw_integration_id=raw_integration_id)

    os.makedirs(os.path.join(pc.DDL_TGT_PATH, 'RAW_DEL_PLP_PROCs'), exist_ok=True)
    with open(os.path.join(pc.DDL_TGT_PATH, 'RAW_DEL_PLP_PROCs', proc_name + '.sql'), 'w') as file:
        file.write(proc_str)

    client = storage.Client(project_name)
    bucket = client.get_bucket(pc.BUCKET_NAME)
    blob = bucket.blob(pc.TEMP_PATH + 'RAW_DEL_PLP_PROCs/' + proc_name + '.sql')
    blob.upload_from_string(proc_str)

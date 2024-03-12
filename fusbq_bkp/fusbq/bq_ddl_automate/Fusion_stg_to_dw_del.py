import pandas as pd
import fusbq.path_config as pc
import os
from google.cloud import storage


def generate_dw_del_sil(df: pd.DataFrame, in_stg_table_name, in_dw_table_name):
    df['integration_id'] = df.apply(lambda x: 'COALESCE(' + x['name'] + ',\'\') ' if x.type == 'STRING' else
    'COALESCE(' + x['name'] + ', 0)', axis=1)

    integration_id = ",'~',".join(df.integration_id.values.tolist())
    integration_id = 'concat( ' + integration_id + ') as INTEGRATION_ID '

    dw_column_names = df.raw_column_name.values.tolist()
    dw_column_names.extend(['INTEGRATION_ID', 'CREATEDATE'])
    dw_column_names = ',\n'.join(dw_column_names)

    stg_column_names = df.name.values.tolist()
    stg_column_names.extend([integration_id, 'current_timestamp()'])
    stg_column_names = ',\n'.join(stg_column_names)

    project_name = pc.DATA_PROJECT_NAME
    proc_name = 'PRC_SIL_' + in_dw_table_name + '_DEL'
    dw_schema = pc.DW_SCHEMA
    dw_table_name = in_dw_table_name + '_DEL'
    stg_schema = pc.STG_SCHEMA
    stg_table_name = in_stg_table_name

    script_template = "CREATE OR REPLACE PROCEDURE `project_name.{dw_schema}.{proc_name}`() \n" \
                      "BEGIN \n" \
                      "TRUNCATE TABLE {dw_schema}.{dw_table_name}; \n" \
                      "INSERT INTO {dw_schema}.{dw_table_name}( \n" \
                      "{dw_column_names} \n" \
                      ") \n" \
                      "select DISTINCT \n" \
                      "{stg_column_names} \n" \
                      "from {stg_schema}.{stg_table_name};\n" \
                      "END \n"

    proc_str = script_template.format(proc_name=proc_name, dw_schema=dw_schema,
                                      dw_table_name=dw_table_name, stg_schema=stg_schema, stg_table_name=stg_table_name,
                                      dw_column_names=dw_column_names, stg_column_names=stg_column_names)

    os.makedirs(os.path.join(pc.DDL_TGT_PATH, 'DW_DEL_SIL_PROCs'), exist_ok=True)
    with open(os.path.join(pc.DDL_TGT_PATH, 'DW_DEL_SIL_PROCs', proc_name + '.sql'), 'w') as file:
        file.write(proc_str)

    client = storage.Client(project_name)
    bucket = client.get_bucket(pc.BUCKET_NAME)
    blob = bucket.blob(pc.TEMP_PATH + 'DW_DEL_SIL_PROCs/' + proc_name + '.sql')
    blob.upload_from_string(proc_str)

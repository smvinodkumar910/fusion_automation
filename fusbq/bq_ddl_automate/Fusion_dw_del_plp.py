import os
from fusbq.path_config import DataSets
pc = DataSets('common')
from google.cloud import storage



def generate_dw_del_plp(in_dw_table_name: str, module):
    pc = DataSets(module)
    project_name = pc.DATA_PROJECT_NAME
    dw_schema = pc.DW_SCHEMA
    dw_table_name = in_dw_table_name
    dw_del_table_name = in_dw_table_name + '_DEL'
    proc_name = 'PRC_PLP_' + in_dw_table_name + '_DEL'

    script_template = "CREATE OR REPLACE PROCEDURE `project_name.{dw_schema}.{proc_name}`() \n" \
                      "BEGIN \n\n" \
                      "UPDATE {dw_schema}.{dw_table_name}\n" \
                      "SET DELETE_FLAG= 'N'\n" \
                      "WHERE DATA_SOURCE_NUM_ID=34;\n\n" \
                      "UPDATE {dw_schema}.{dw_table_name}\n" \
                      "SET DELETE_FLAG='Y',\n" \
                      "PLP_TGT_UPDATE_DT = CURRENT_DATETIME()\n" \
                      "WHERE INTEGRATION_ID NOT IN ( \n" \
                      "SELECT DISTINCT INTEGRATION_ID FROM {dw_schema}.{dw_del_table_name}) AND" \
                      " DATA_SOURCE_NUM_ID = 34;\n" \
                      "END\n"

    proc_str = script_template.format(dw_schema=dw_schema, dw_table_name=dw_table_name,
                                      dw_del_table_name=dw_del_table_name, proc_name=proc_name)

    os.makedirs(os.path.join(pc.DDL_TGT_PATH, 'DW_DEL_PLP_PROCs'), exist_ok=True)
    with open(os.path.join(pc.DDL_TGT_PATH, 'DW_DEL_PLP_PROCs', proc_name + '.sql'), 'w') as file:
        file.write(proc_str)

    client = storage.Client(project_name)
    bucket = client.get_bucket(pc.BUCKET_NAME)
    blob = bucket.blob(pc.TEMP_PATH + 'DW_DEL_PLP_PROCs/' + proc_name + '.sql')
    blob.upload_from_string(proc_str)
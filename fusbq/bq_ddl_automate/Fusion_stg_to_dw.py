import pandas as pd
import os
from fusbq.path_config import DataSets as pc
pc = pc('common')

from google.cloud import storage

target_ddl_file_path = os.path.join(pc.ROOT_DIR, pc.DDL_TGT_PATH)

project_name = pc.DATA_PROJECT_NAME
bucket_name = pc.BUCKET_NAME
temp_path = pc.TEMP_PATH

static_cols = {'DATA_SOURCE_NUM_ID': "34",
               'DELETE_FLAG': "'N'",
               'ETL_PROC_WID': "IN_ETL_PROC_WID",
               'TGT_CREATE_DT': "CURRENT_TIMESTAMP()",
               'TGT_UPDATE_DT': "CURRENT_TIMESTAMP()",
               'PLP_TGT_UPDATE_DT': "NULL"}


def generate_dw_sil(coldf: pd.DataFrame, module):
    pc = pc(module)
    # Process key columns
    keydf = coldf.loc[coldf['isPrimaryKey'], ['source_table_name', 'source_column_name']]
    keydf = keydf.groupby(['source_table_name'])['source_column_name'].apply(lambda x: ",'~',".join(list(x)))

    # To handle inconsistent datatype between src & target
    coldf = coldf.fillna('')
    coldf['source_column_name'] = coldf[['source_data_type', 'target_data_type', 'source_column_name']].apply(
        lambda x: 'cast(' + x['source_column_name'] + ' as ' + x['target_data_type'] + ')' if (
                x['source_data_type'] != x['target_data_type'] and x['source_column_name'] != '') else x[
            'source_column_name'],
        axis=1
    )

    # Process all columns
    # Update script
    coldf_mod = coldf.loc[:, ['source_column_name', 'target_column_name', ]]
    coldf_mod = coldf_mod.fillna('')
    coldf_dict = coldf_mod.to_dict(orient='records')
    update_script_col = []
    for a in coldf_dict:
        output = update_script_fun(a['source_column_name'], a['target_column_name'])
        update_script_col.append(output)

    coldf['update_script'] = update_script_col

    target_col_name = []
    for a in coldf_dict:
        output = target_col_name_fun(a['source_column_name'], a['target_column_name'])
        target_col_name.append(output)

    coldf['target_column_name'] = target_col_name

    source_col_name = []
    for a in coldf_dict:
        output = source_values_fun(a['source_column_name'], a['target_column_name'])
        source_col_name.append(output)

    coldf['source_column_name'] = source_col_name

    select_col_list = []
    for a in coldf_dict:
        output = select_list_fun(a['source_column_name'], a['target_column_name'])
        select_col_list.append(output)

    coldf['select_col_list'] = select_col_list

    update_script = coldf.groupby(['source_table_name'])['update_script'].apply(list)
    source_column_names = coldf.groupby(['source_table_name'])['source_column_name'].apply(list)
    target_column_names = coldf.groupby(['source_table_name'])['target_column_name'].apply(list)
    select_columns_list = coldf.groupby(['source_table_name'])['select_col_list'].apply(list)

    target_table_name = coldf.groupby(['source_table_name'])['target_table_name'].apply(set)

    # To add integration_id
    for a in select_columns_list.index:
        select_columns_list[a].pop(0)
        integ = ''.join(keydf[a])
        integ = 'concat(' + integ + ')'
        select_columns_list[a].insert(0, 'cast(' + integ + ' as string) as INTEGRATION_ID,\n')

    final_df = pd.DataFrame({'src_table_name': update_script.index, 'update_script': update_script.values,
                             'source_column_names': source_column_names.values,
                             'target_column_name': target_column_names.values,
                             'select_columns_list': select_columns_list.values,
                             'target_table_name': target_table_name.values})

    # final_df['target_table_name'] = 'DW_'+final_df['src_table_name'].str[4:]

    final_dict = final_df.to_dict(orient='records')

    for a in final_dict:
        tgt_table = ''.join(a['target_table_name'])
        project = project_name
        tgt_sch = pc.RAW_SCHEMA
        proc_name = 'PRC_SIL_' + tgt_table
        src_sch = pc.STG_SCHEMA
        src_table = a['src_table_name']
        update_statement = ''.join(a['update_script'])
        update_statement = update_statement[:-2]

        sel_columns_list = ''.join(a['select_columns_list'])
        sel_columns_list = sel_columns_list[:-2]

        src_col_list = ''.join(a['source_column_names'])
        src_col_list = src_col_list[:-2]

        tgt_col_list = ''.join(a['target_column_name'])
        tgt_col_list = tgt_col_list[:-2]
        script_template = 'CREATE OR REPLACE PROCEDURE `project_name.{tgt_sch}.{proc_name}`(IN_ETL_PROC_WID INT64) \n ' \
                          '---Created with Automation Script \n' \
                          'BEGIN MERGE {tgt_sch}.{tgt_table} TGT \n USING \n(SELECT \n {select_columns_list} \nFROM {' \
                          'src_sch}.{' \
                          'src_table} ) SRC \n' \
                          'ON (SRC.INTEGRATION_ID = TGT.INTEGRATION_ID) WHEN MATCHED THEN UPDATE SET \n' \
                          '{update_statement} \n' \
                          'WHEN NOT MATCHED THEN INSERT \n' \
                          '(\n{tgt_col_list} \n) \n' \
                          'VALUES \n (\n{src_col_list}\n);\n' \
                          'END;'
        proc_str = script_template.format(tgt_sch=tgt_sch, proc_name=proc_name, tgt_table=tgt_table,
                                          src_sch=src_sch, src_table=src_table,
                                          update_statement=update_statement, src_col_list=src_col_list,
                                          tgt_col_list=tgt_col_list,
                                          select_columns_list=sel_columns_list)
        os.makedirs(os.path.join(target_ddl_file_path, 'DW_PROCs'), exist_ok=True)

        with open(os.path.join(target_ddl_file_path, 'DW_PROCs', proc_name + '.sql'), 'w') as file:
            file.write(proc_str)
        client = storage.Client(project_name)
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(temp_path + 'DW_PROCs/' + proc_name + '.sql')
        blob.upload_from_string(proc_str)


def select_list_fun(x, y):
    # To handle default columns
    # if source_col is null, and target_col name in static_cols default values if source_col is null then comment source_col is not null below logic
    if x == '' and (y in static_cols.keys()):
        return static_cols[y] + ' as ' + y + ',\n'
    elif x == '' and (y not in static_cols.keys()):
        return '---' + y + ' ,\n'
    elif x != '':
        return x + ' as ' + y + ',\n'


def update_script_fun(x, y):
    # To handle default columns
    # if source_col is null, and target_col name in static_cols default values if source_col is null then comment source_col is not null below logic
    if y == 'PLP_TGT_UPDATE_DT':
        return '---TGT.' + y + ',\n'
    elif x == '' and (y in static_cols.keys()):
        return 'TGT.' + y + ' = SRC.' + y + ',\n'
    elif (x == '' and (y not in static_cols.keys())) or y == 'INTEGRATION_ID':
        return '---TGT.' + y + ',\n'
    elif x != '':
        return 'TGT.' + y + ' = SRC.' + y + ',\n'


def target_col_name_fun(x, y):
    # To handle default columns
    # if source_col is null, and target_col name in static_cols default values if source_col is null then comment source_col is not null below logic
    if y == 'PLP_TGT_UPDATE_DT':
        return '---TGT.' + y + ',\n'
    elif x == '' and y == 'INTEGRATION_ID':
        return y + ',\n'
    elif x == '' and (y not in static_cols.keys()):
        return '---TGT.' + y + ',\n'
    else:
        return y + ',\n'


def source_values_fun(x, y):
    # To handle default columns
    # if source_col is null, and target_col name in static_cols default values if source_col is null then comment source_col is not null below logic
    if y == 'PLP_TGT_UPDATE_DT':
        return '---' + y + ',\n'
    elif x == '' and (y in static_cols.keys() or y == 'INTEGRATION_ID'):
        return 'SRC.' + y + ',\n'
    elif x == '' and (y not in static_cols.keys()):
        return '---' + y + ',\n'
    elif x != '':
        return 'SRC.' + y + ',\n'

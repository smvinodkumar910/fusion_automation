import pandas as pd
import fusbq.path_config as pc
import os
from google.cloud import storage

target_ddl_file_path = os.path.join(pc.ROOT_DIR, pc.DDL_TGT_PATH)

project_name = pc.DATA_PROJECT_NAME
bucket_name = pc.BUCKET_NAME
temp_path = pc.TEMP_PATH

static_cols = ['DATA_SOURCE_NUM_ID',
               'DELETE_FLAG',
               'ETL_PROC_WID',
               'TGT_CREATE_DT',
               'TGT_UPDATE_DT',
               'PLP_TGT_UPDATE_DT'
               ]


def stg_integration_id_derive(x):
    if x.source_data_type == 'STRING':
        return 'COALESCE(' + x['source_column_name'] + ',\'\') '
    elif x.source_data_type == 'NUMERIC':
        return 'COALESCE(' + x['source_column_name'] + ', 0)'
    else:
        return 'COALESCE(CAST(' + x['source_column_name'] + ' as STRING),\'\') '


def join_condition_derive(x):
    if x.source_data_type == 'STRING':
        return 'ifnull(SRC.' + x.target_column_name + ',\'\') = ifnull(TGT.' + x.target_column_name + ',\'\')'
    elif x.source_data_type == 'NUMERIC':
        return 'ifnull(SRC.' + x.target_column_name + ', 0) = ifnull(TGT.' + x.target_column_name + ', 0)'
    else:
        return 'ifnull(CAST(SRC.' + x.target_column_name + ' AS STRING),\'\') = ifnull(CAST(TGT.' + x.target_column_name + ' AS STRING),\'\')'


def generate_raw_sil(coldf: pd.DataFrame):
    # to derive RNUM to avoid duplication
    primarykeydf = coldf.loc[coldf['isPrimaryKey'], :]
    primarykeydf['stg_integration_id'] = primarykeydf.apply(
        lambda x: stg_integration_id_derive(x),
        axis=1)

    stg_integration_id = ",'~',".join(primarykeydf.stg_integration_id.values.tolist())
    stg_integration_id = 'concat(' + stg_integration_id + ') '

    lastupdatedf = coldf.loc[coldf['isLastUpdateDate'], ['source_column_name']]
    incremental_col = lastupdatedf.iloc[0]['source_column_name']
    #print(incremental_col)

    RNUM_COL_STR = ',\nROW_NUMBER() OVER(PARTITION BY {STG_INTEGRATION_ID} ORDER BY CAST({INCREMENTAL_COL} ' \
                   'AS TIMESTAMP) DESC ) AS RNUM'.format(STG_INTEGRATION_ID=stg_integration_id,
                                                         INCREMENTAL_COL=incremental_col)

    # Process Key Columns
    keys = coldf.loc[coldf['isPrimaryKey'], ['source_table_name', 'target_column_name', 'source_data_type']]
    keys['join_condition'] = keys.apply(lambda x: join_condition_derive(x), axis=1)
    join_condition = keys.groupby(['source_table_name'])['join_condition'].apply(lambda x: ' and '.join(list(x)))

    # Process all columns
    coldf['select_col_name'] = coldf['source_column_name'] + ' as ' + coldf['target_column_name'] + ',\n'
    coldf['update_script'] = 'TGT.' + coldf['target_column_name'] + ' = SRC.' + coldf['target_column_name'] + ',\n'
    coldf['source_column_name'] = 'SRC.' + coldf['target_column_name'] + ',\n'
    coldf['target_column_name'] = coldf['target_column_name'] + ',\n'

    select_col_name = coldf.groupby(['source_table_name'])['select_col_name'].apply(list)
    update_script = coldf.groupby(['source_table_name'])['update_script'].apply(list)
    source_column_names = coldf.groupby(['source_table_name'])['source_column_name'].apply(list)
    target_column_names = coldf.groupby(['source_table_name'])['target_column_name'].apply(list)
    # join_condition = keydf.groupby(['table_name'])['join_condition'].apply(list)

    target_table_name = coldf.groupby(['source_table_name'])['target_table_name'].apply(set)

    final_df = pd.DataFrame({'src_table_name': update_script.index, 'update_script': update_script.values,
                             'source_column_names': source_column_names.values,
                             'target_column_name': target_column_names.values,
                             'join_condition': join_condition.values, 'select_col_name': select_col_name.values,
                             'target_table_name': target_table_name.values})

    # final_df['target_table_name'] = 'RAW_'+final_df['src_table_name'].str[4:]

    final_dict = final_df.to_dict(orient='records')

    for a in final_dict:
        tgt_table = ''.join(a['target_table_name'])
        project = project_name
        tgt_sch = pc.RAW_SCHEMA
        proc_name = 'PRC_SIL_' + tgt_table
        src_sch = pc.STG_SCHEMA
        src_table = a['src_table_name']
        join_condition = ''.join(a['join_condition'])
        update_statement = ''.join(a['update_script'])
        update_statement = update_statement[:-2]
        src_col_list = ''.join(a['source_column_names'])
        src_col_list = src_col_list[:-2]
        tgt_col_list = ''.join(a['target_column_name'])
        tgt_col_list = tgt_col_list[:-2]
        sel_col_name = ''.join(a['select_col_name'])
        sel_col_name = sel_col_name[:-2]
        sel_col_name = sel_col_name + RNUM_COL_STR

        # To add delete_flag
        update_statement = update_statement + ",\nDELETE_FLAG='N'"
        tgt_col_list = tgt_col_list + ",\nDELETE_FLAG"
        src_col_list = src_col_list + ",\n'N'"

        script_template = '---Created with Automation Script \n' \
                          'CREATE OR REPLACE PROCEDURE `{project}.{tgt_sch}.{proc_name}`(IN_ETL_PROC_WID INT64) \n ' \
                          'BEGIN MERGE {project}.{tgt_sch}.{tgt_table} TGT \n USING \n(SELECT * FROM (SELECT {select_col_name} FROM {project}.{src_sch}.{' \
                          'src_table} ) WHERE RNUM = 1 \n ) \n SRC \n' \
                          'ON ({join_condition}) WHEN MATCHED THEN UPDATE SET \n' \
                          '{update_statement} ' \
                          'WHEN NOT MATCHED THEN INSERT ' \
                          '({tgt_col_list}) \n' \
                          'VALUES \n ({src_col_list});\n' \
                          'END;'
        proc_str = script_template.format(project=project_name, tgt_sch=tgt_sch, proc_name=proc_name,
                                          tgt_table=tgt_table,
                                          src_sch=src_sch, src_table=src_table,
                                          join_condition=join_condition, update_statement=update_statement,
                                          src_col_list=src_col_list, tgt_col_list=tgt_col_list,
                                          select_col_name=sel_col_name)
        # Create required directories if not exists
        os.makedirs(os.path.join(target_ddl_file_path, 'RAW_PROCs'), exist_ok=True)
        with open(os.path.join(target_ddl_file_path, 'RAW_PROCs', proc_name + '.sql'), 'w') as file:
            file.write(proc_str)

        client = storage.Client(project_name)
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(temp_path + 'RAW_PROCs/' + proc_name + '.sql')
        blob.upload_from_string(proc_str)

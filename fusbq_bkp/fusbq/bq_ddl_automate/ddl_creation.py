import pandas as pd
import re
import os
import json
import glob
from fusbq import path_config as pc
from fusbq.bq_ddl_automate.Fusion_stg_to_raw import generate_raw_sil
from fusbq.bq_ddl_automate.Fusion_stg_to_dw import generate_dw_sil
from google.cloud import storage
from fusbq.bq_ddl_automate.Fusion_stg_to_dw_del import generate_dw_del_sil
from fusbq.bq_ddl_automate.Fusion_dw_del_plp import generate_dw_del_plp
from fusbq.bq_ddl_automate.Fusion_raw_del_plp import generate_raw_del_plp

project_name = pc.PROJECT_NAME
bucket_name = pc.BUCKET_NAME
temp_path = pc.TEMP_PATH


def derive_col_name(col_name: str):
    chrs = set()
    for match in re.finditer(r'[A-Z][a-z]', str(col_name)):
        chrs.add(match.group())
    for a in chrs:
        col_name = re.sub(a, "_" + a, col_name)
    return col_name.replace('__', '_').upper()[1:]


def create_merge_script(ddlroot: str):
    ddl_types = ['STG', 'RAW', 'DW']
    for f in ddl_types:
        filenames = glob.glob(os.path.join(ddlroot, f, '*.sql'))
        # Open files in write mode
        with open(os.path.join(ddlroot, f + '_ALL_DDLS.sql'), 'w') as outfile:
            # Iterate through list
            for names in filenames:
                # Open each file in read mode
                with open(names) as infile:
                    # read the data from file1 and
                    # file2 and write it in file3
                    outfile.write(infile.read())

                outfile.write("\n")


def create_raw_sil_df(ddldf: pd.DataFrame, stg_table_name, raw_table_name, target_path):
    ddldf['source_table_schema'] = pc.STG_SCHEMA
    ddldf['source_table_name'] = stg_table_name
    ddldf['target_schema'] = pc.RAW_SCHEMA
    ddldf['target_table_name'] = raw_table_name
    ddldf['source_data_type'] = ddldf['type']
    ddldf['target_data_type'] = ddldf['type']
    ddldf = ddldf.rename(columns={'name': 'source_column_name', 'raw_column_name': 'target_column_name'})
    ddldf = ddldf.loc[:,
            ['source_table_schema', 'source_table_name', 'source_column_name', 'source_data_type', 'target_schema',
             'target_table_name', 'target_column_name', 'target_data_type', 'isPrimaryKey', 'isLastUpdateDate']]

    '''
    if os.path.exists(os.path.join(target_path, 'RAW_SIL_COLUMNS_MATCH.csv')):
        ddldf.to_csv(os.path.join(target_path, 'RAW_SIL_COLUMNS_MATCH.csv'), mode='a', index=False, header=False)
    else:
        ddldf.to_csv(os.path.join(target_path, 'RAW_SIL_COLUMNS_MATCH.csv'), mode='a', index=False)
    client = storage.Client(project_name)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(os.path.join(temp_path, 'RAW_SIL_COLUMNS_MATCH.csv'))
    blob.upload_from_filename(os.path.join(target_path, 'RAW_SIL_COLUMNS_MATCH.csv'))
    '''
    generate_raw_sil(ddldf)


def create_dw_sil_df(ddldf: pd.DataFrame, stg_table_name, dw_table_name, target_path):
    ddldf['source_table_schema'] = pc.STG_SCHEMA
    ddldf['source_table_name'] = stg_table_name
    ddldf['target_schema'] = pc.RAW_SCHEMA
    ddldf['target_table_name'] = dw_table_name
    ddldf['source_data_type'] = ddldf['type']
    ddldf['target_data_type'] = ddldf['type']
    ddldf = ddldf.rename(columns={'name': 'source_column_name', 'raw_column_name': 'target_column_name'})
    ddldf = ddldf.loc[:,
            ['source_table_schema', 'source_table_name', 'source_column_name', 'source_data_type', 'target_schema',
             'target_table_name', 'target_column_name', 'target_data_type', 'isPrimaryKey', 'isLastUpdateDate']]

    # Create static_df
    static_cols = {
        'target_column_name': ['INTEGRATION_ID', 'DATA_SOURCE_NUM_ID', 'DELETE_FLAG', 'ETL_PROC_WID', 'TGT_CREATE_DT',
                               'TGT_UPDATE_DT']}
    static_df = pd.DataFrame(static_cols)
    static_df['source_table_schema'] = pc.STG_SCHEMA
    static_df['source_table_name'] = stg_table_name
    static_df['source_column_name'] = ''
    static_df['source_data_type'] = ''
    static_df['target_schema'] = pc.RAW_SCHEMA
    static_df['target_table_name'] = dw_table_name
    static_df['target_data_type'] = ''
    static_df['isPrimaryKey'] = False

    static_df = static_df.loc[:,
                ['source_table_schema', 'source_table_name', 'source_column_name', 'source_data_type', 'target_schema',
                 'target_table_name',
                 'target_column_name', 'target_data_type', 'isPrimaryKey']]
    ddldf = pd.concat([static_df, ddldf])
    '''
    if os.path.exists(os.path.join(target_path, 'DW_SIL_COLUMNS_MATCH.csv')):
        ddldf.to_csv(os.path.join(target_path, 'DW_SIL_COLUMNS_MATCH.csv'), mode='a', index=False, header=False)
    else:
        ddldf.to_csv(os.path.join(target_path, 'DW_SIL_COLUMNS_MATCH.csv'), mode='a', index=False)
    client = storage.Client(project_name)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(os.path.join(temp_path, 'DW_SIL_COLUMNS_MATCH.csv'))
    blob.upload_from_filename(os.path.join(target_path, 'DW_SIL_COLUMNS_MATCH.csv'))
    '''
    generate_dw_sil(ddldf)


def create_del_tables(oradf, stg_del_table_name, raw_table_name, dw_table_name, target_path):
    oradf = oradf.loc[oradf['isPrimaryKey'], ['name', 'type', 'raw_column_name']]
    oradf['ddl'] = oradf['name'] + ' ' + oradf['type'] + ',\n'
    oradf['raw_ddl'] = oradf['raw_column_name'] + ' ' + oradf['type'] + ',\n'
    collist = list(oradf['ddl'].drop_duplicates())
    raw_col_list = list(oradf['raw_ddl'].drop_duplicates())
    colstr = ''.join(collist)
    raw_col_str = ''.join(raw_col_list)

    _metadata_insert_time = "_metadata_insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n"

    static_cols = "INTEGRATION_ID STRING,\n" \
                  "CREATEDATE TIMESTAMP NOT NULL \n"

    create = 'CREATE TABLE IF NOT EXISTS `project_name.helix_stg.' + stg_del_table_name + '`\n(\n'
    colstrstg = create + colstr + _metadata_insert_time
    colstrstg = colstrstg[:-2] + '\n);'

    client = storage.Client(project_name)
    bucket = client.get_bucket(bucket_name)

    with open(os.path.join(target_path, 'STG_DEL', stg_del_table_name + '.sql'), 'w') as file:
        file.write(colstrstg)
    blob = bucket.blob(temp_path + 'STG_DEL/' + stg_del_table_name + '.sql')
    blob.upload_from_string(colstrstg)

    create = 'CREATE TABLE IF NOT EXISTS `project_name.helix_dw.' + dw_table_name + '_DEL`\n(\n'
    colstrdw = create + raw_col_str + static_cols
    colstrdw = colstrdw[:-2] + '\n);'

    with open(os.path.join(target_path, 'DW_DEL', dw_table_name + '_DEL.sql'), 'w') as file:
        file.write(colstrdw)
    blob = bucket.blob(temp_path + 'DW_DEL/' + dw_table_name + '_DEL.sql')
    blob.upload_from_string(colstrstg)

    # Steps to modify df for bq json
    oradf['mode'] = 'NULLABLE'
    bqschema = oradf.loc[:, ['name', 'type', 'mode']].to_dict('records')
    bqschema_without_meta = bqschema
    bqschema.append({
        "name": "_metadata_insert_time",
        "type": "TIMESTAMP",
        "mode": "NULLABLE",
        "default_value_expression": "CURRENT_TIMESTAMP"
    })

    # Write bq schema json file
    with open(os.path.join(target_path, 'JSON', stg_del_table_name + '.json'), 'w') as file:
        file.write(json.dumps(bqschema, indent=2))
    # Upload to fusion_schema folder
    blob = bucket.blob(pc.FUSION_SCHEMA + '/' + stg_del_table_name + '.json')
    blob.upload_from_string(json.dumps(bqschema_without_meta, indent=2))
    # Upload to temp folder
    blob = bucket.blob(temp_path + 'JSON_STG_DEL/' + stg_del_table_name + '.json')
    blob.upload_from_string(json.dumps(bqschema, indent=2))

    generate_dw_del_sil(oradf, stg_del_table_name, dw_table_name)
    generate_dw_del_plp(dw_table_name)
    generate_raw_del_plp(oradf, stg_del_table_name, raw_table_name)


def create_bq_ddl(oradf: pd.DataFrame, table_name: str, stg_table_name: str, raw_table_name: str, dw_table_name: str,
                  hist_table_name: str, stg_del_table_name:str,
                  target_path: str):
    # Create required directories if not exists
    os.makedirs(os.path.join(target_path, 'STG'), exist_ok=True)
    os.makedirs(os.path.join(target_path, 'RAW'), exist_ok=True)
    os.makedirs(os.path.join(target_path, 'DW'), exist_ok=True)
    os.makedirs(os.path.join(target_path, 'JSON'), exist_ok=True)
    os.makedirs(os.path.join(target_path, 'STG_DEL'), exist_ok=True)
    os.makedirs(os.path.join(target_path, 'DW_DEL'), exist_ok=True)
    os.makedirs(os.path.join(target_path, 'HIST'), exist_ok=True)

    datatype_map = {'BINARY': 'STRING', 'CLOB': 'STRING', 'NUMERIC': 'NUMERIC', 'VARCHAR': 'STRING', 'DATE': 'DATE',
                    'TIMESTAMP': 'TIMESTAMP', 'FLOAT': 'NUMERIC'}

    # To add insert_time in STG table
    _metadata_insert_time = "_metadata_insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n"

    # To add platform supported column in RAW tables
    raw_meta_cols = "_metadata_insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, \n" \
                    "_metadata_update_time TIMESTAMP, \n" \
                    "_metadata_deleted STRING DEFAULT 'N',\n" \
                    "_metadata_dag_run_id STRING,\n"

    hist_meta_cols = "_metadata_insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, \n" \
                     "_metadata_deleted STRING DEFAULT 'N',\n" \
                     "_metadata_dag_run_id STRING,\n"

    integration_id = "INTEGRATION_ID STRING NOT NULL,\n"
    static_cols = "DATA_SOURCE_NUM_ID FLOAT NOT NULL,\n" \
                  "DELETE_FLAG STRING NOT NULL,\n" \
                  "ETL_PROC_WID FLOAT NOT NULL,\n" \
                  "TGT_CREATE_DT TIMESTAMP NOT NULL,\n" \
                  "TGT_UPDATE_DT TIMESTAMP NOT NULL \n"
    # "PLP_TGT_UPDATE_DT TIMESTAMP' \n"

    # To handle duplicate scenarios
    # 1. for colunms like CREATION_DATE,
    # 2. If PVO column name found multiple times in BICC Sheet
    duplicate_cols = oradf.groupby(['raw_column_name'])['raw_column_name'].count()
    duplicate_orig_cols = oradf.groupby(['name'])['name'].count()
    duplicate_cols = duplicate_cols.loc[lambda x: x > 1].index
    duplicate_orig_cols = duplicate_orig_cols.loc[lambda x: x > 1].index
    raw_series = oradf.loc[:, ['raw_column_name', 'name']].to_dict('records')
    raw_new_list = []
    # print(duplicate_cols)
    # print(duplicate_orig_cols)
    for a in raw_series:
        if pd.isna(a['raw_column_name']) or a['raw_column_name'] in duplicate_cols or a['raw_column_name'] in \
                ['INTERVAL'] or a['name'] in duplicate_orig_cols:
            raw_new_list.append(derive_col_name(a['name']))
        else:
            raw_new_list.append(a['raw_column_name'])
    #oradf['raw_column_name'] = raw_new_list
    # To have Stage table also have column names as that of Source
    oradf['raw_column_name'] = oradf['name'].str.upper()
    #oradf['name'] = raw_new_list

    oradf = oradf.drop_duplicates()

    oradf['name'] = oradf['name'].str.upper()
    oradf['type'] = oradf['type'].fillna('VARCHAR')

    # oradf['type'] = oradf['type'].map(datatype_map)
    oradf['scale'] = oradf['scale'].astype('int64')
    oradf['type'] = oradf.apply(
        lambda x: 'BIGNUMERIC' if (x.scale == -127 and x.type == 'NUMERIC') else datatype_map[x.type], axis=1)
    oradf['ddl'] = oradf['name'] + ' ' + oradf['type'] + ',\n'
    oradf['raw_ddl'] = oradf['raw_column_name'] + ' ' + oradf['type'] + ',\n'

    collist = list(oradf['ddl'].drop_duplicates())
    collistraw = list(oradf['raw_ddl'].drop_duplicates())

    colstr = ''.join(collist)  # Created string of all columns with bq type for stg
    colstrraw = ''.join(collistraw)  # Created string of all columns with bq type for raw
    colstrdw = colstrraw  # Created string of all columns with bq type for dw
    colstrhist = colstrraw  # Created string of all columns with bq type of dw of history tb

    # Stg table ddl creation
    create = 'CREATE TABLE IF NOT EXISTS `project_name.helix_stg.' + stg_table_name + '`\n(\n'
    colstrstg = create + colstr + _metadata_insert_time
    colstrstg = colstrstg[:-2] + '\n);'

    # Raw table ddl creation
    createraw = 'CREATE TABLE IF NOT EXISTS `project_name.helix_raw.' + raw_table_name + '`\n(\n'
    createraw = createraw + colstrraw + raw_meta_cols
    colstrraw = createraw[:-2] + '\n);'

    # Hist table ddl creation
    createhist = 'CREATE TABLE IF NOT EXISTS `project_name.helix_stg.' + hist_table_name + '`\n(\n'
    createhist = createhist + colstrhist + hist_meta_cols
    colstrhist = createhist[:-2] + '\n)\nPARTITION BY DATE(_metadata_insert_time);'

    # DW table ddl creation
    createdw = 'CREATE TABLE IF NOT EXISTS `project_name.helix_dw.' + dw_table_name + '`\n(\n'
    colstrdw = createdw + integration_id + colstrdw + static_cols  # adding three additional column for HIST table
    colstrdw = colstrdw[:-2] + '\n);'

    # Create the tables in BQ
    # create_table_bq(colstrstg)
    # create_table_bq(colstrraw)
    # create_table_bq(colstrdw)
    client = storage.Client(project_name)
    bucket = client.get_bucket(bucket_name)

    # Write STG ddl script to file
    with open(os.path.join(target_path, 'STG', stg_table_name + '.sql'), 'w') as file:
        file.write(colstrstg)
    blob = bucket.blob(temp_path + 'STG/' + stg_table_name + '.sql')
    blob.upload_from_string(colstrstg)

    # Write ddl script for RAW Table to file
    with open(os.path.join(target_path, 'RAW', raw_table_name + '.sql'), 'w') as file:
        file.write(colstrraw)
    blob = bucket.blob(temp_path + 'RAW/' + raw_table_name + '.sql')
    blob.upload_from_string(colstrraw)

    # Write HIST ddl script to file
    with open(os.path.join(target_path, 'HIST', hist_table_name + '.sql'), 'w') as file:
        file.write(colstrhist)
    blob = bucket.blob(temp_path + 'HIST/' + hist_table_name + '.sql')
    blob.upload_from_string(colstrhist)

    # Write DW ddl script to file
    with open(os.path.join(target_path, 'DW', dw_table_name + '.sql'), 'w') as file:
        file.write(colstrdw)
    blob = bucket.blob(temp_path + 'DW/' + dw_table_name + '.sql')
    blob.upload_from_string(colstrdw)

    # Steps to modify df for bq json for stg
    oradf.drop('ddl', inplace=True, axis=1)
    oradf['mode'] = 'NULLABLE'
    bqstgschema = oradf.loc[:, ['name', 'type', 'mode']].to_dict('records')
    bqstgschema_without_meta = bqstgschema
    bqstgschema.append({
        "name": "_metadata_insert_time",
        "type": "TIMESTAMP",
        "mode": "NULLABLE",
        "default_value_expression": "CURRENT_TIMESTAMP"
    })

    # Steps to modify df for bq json for raw tables
    raworadf = oradf.loc[:, ['raw_column_name', 'type', 'mode']]
    raworadf = raworadf.rename(columns={'raw_column_name': 'name'})
    bqrawschema = raworadf.to_dict('records')
    bqrawschema.append({
        "name": "_metadata_insert_time",
        "type": "TIMESTAMP",
        "mode": "NULLABLE",
        "default_value_expression": "CURENT_TIMESTAMP"
    })
    bqrawschema.append({
        "name": "_metadata_update_time",
        "type": "TIMESTAMP",
        "mode": "NULLABLE"
    })
    bqrawschema.append({
        "name": "_metadata_deleted",
        "type": "STRING",
        "mode": "NULLABLE",
        "default_value_expression": "N"
    })
    bqrawschema.append({
        "name": "_metadata_dag_run_id",
        "type": "STRING",
        "mode": "NULLABLE"
    })

    # Steps to modify df for bq json for hist tables
    historadf = oradf.loc[:, ['raw_column_name', 'type', 'mode']]
    historadf = historadf.rename(columns={'raw_column_name': 'name'})
    bqhistschema = historadf.to_dict('records')
    bqhistschema.append({
        "name": "_metadata_insert_time",
        "type": "TIMESTAMP",
        "mode": "NULLABLE",
        "default_value_expression": "CURRENT_TIMESTAMP"
    })
    bqhistschema.append({
        "name": "_metadata_deleted",
        "type": "STRING",
        "mode": "NULLABLE",
        "default_value_expression": "N"
    })
    bqhistschema.append({
        "name": "_metadata_dag_run_id",
        "type": "STRING",
        "mode": "NULLABLE"
    })

    # Write bq schema json file
    with open(os.path.join(target_path, 'JSON', stg_table_name + '.json'), 'w') as file:
        file.write(json.dumps(bqstgschema, indent=2))
    with open(os.path.join(target_path, 'JSON', raw_table_name + '.json'), 'w') as file:
        file.write(json.dumps(bqrawschema, indent=2))
    with open(os.path.join(target_path, 'JSON', hist_table_name + '.json'), 'w') as file:
        file.write(json.dumps(bqhistschema, indent=2))

    # Upload to fusion_schema folder
    blob = bucket.blob(pc.FUSION_SCHEMA + '/' + stg_table_name + '.json')
    blob.upload_from_string(json.dumps(bqstgschema_without_meta, indent=2))

    # Upload to temp_stg folder
    blob = bucket.blob(temp_path + 'JSON_STG/' + stg_table_name + '.json')
    blob.upload_from_string(json.dumps(bqstgschema, indent=2))

    # Upload to temp_raw folder
    blob = bucket.blob(temp_path + 'JSON_RAW/' + raw_table_name + '.json')
    blob.upload_from_string(json.dumps(bqrawschema, indent=2))

    # Upload to temp_raw folder
    blob = bucket.blob(temp_path + 'JSON_HIST/' + hist_table_name + '.json')
    blob.upload_from_string(json.dumps(bqhistschema, indent=2))

    # Finally merge all ddls into single file
    create_merge_script(target_path)
    create_raw_sil_df(oradf, stg_table_name, raw_table_name, target_path)
    create_dw_sil_df(oradf, stg_table_name, dw_table_name, target_path)
    create_del_tables(oradf, stg_del_table_name, raw_table_name, dw_table_name, target_path)

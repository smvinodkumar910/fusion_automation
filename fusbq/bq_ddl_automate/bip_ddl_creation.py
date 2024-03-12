from copy import deepcopy
import pandas as pd
import os
import json
from google.cloud import storage
from fusbq.path_config import DataSets
pc = DataSets('common')
from airflow.providers.google.common.utils import id_token_credentials as id_token_credential_utils
import google.auth.transport.requests
from google.auth.transport.requests import AuthorizedSession



def bip_cf_trigger(payload) :
    url = pc.BIP_CF_URL
    request = google.auth.transport.requests.Request()
    id_token_credentials = id_token_credential_utils.get_default_id_token_credentials(url, request=request)
    print(payload)
    headers = { 'Content-Type': 'application/json'}
    resp = AuthorizedSession(id_token_credentials).post(url=url, data=payload, headers=headers, timeout=3900)

    print(resp)
    if resp.status_code != 200:
        raise Exception("Cloud function failure.")
    return True



def create_bq_ddl_bip(oradf: pd.DataFrame, table_name: str, stg_table_name: str, raw_table_name: str, dw_table_name: str,
                  hist_table_name: str, stg_del_table_name: str,
                  target_path: str, module: str):
    pc = DataSets(module)
    # Create required directories if not exists
    os.makedirs(os.path.join(target_path, 'STG'), exist_ok=True)
    os.makedirs(os.path.join(target_path, 'RAW'), exist_ok=True)
    #os.makedirs(os.path.join(target_path, 'DW'), exist_ok=True)
    os.makedirs(os.path.join(target_path, 'JSON'), exist_ok=True)
    os.makedirs(os.path.join(target_path, 'STG_DEL'), exist_ok=True)
    #os.makedirs(os.path.join(target_path, 'DW_DEL'), exist_ok=True)
    os.makedirs(os.path.join(target_path, 'HIST'), exist_ok=True)
    os.makedirs(os.path.join(target_path, 'RAW_VIEWS'), exist_ok=True)
    os.makedirs(os.path.join(target_path, pc.FUSION_SCHEMA), exist_ok=True)
    
    oradf['DATA_TYPE'] = oradf['DATA_TYPE'].fillna('VARCHAR')

    datatype_map = {'BINARY': 'STRING', 'CLOB': 'STRING', 'NUMERIC': 'NUMERIC', 'VARCHAR': 'STRING', 'DATE': 'TIMESTAMP',
                    'TIMESTAMP': 'TIMESTAMP', 'FLOAT': 'NUMERIC','NUMBER':'NUMERIC','VARCHAR2': 'STRING','TIMESTAMP(6)':'TIMESTAMP','ROWID':'STRING'}
    
    _metadata_insert_time = "_metadata_insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n"
    
    # To add platform supported column in RAW tables
    raw_meta_cols = "_metadata_insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, \n" \
                    "_metadata_update_time TIMESTAMP, \n" \
                    "_metadata_deleted STRING DEFAULT 'N',\n" \
                    "_metadata_dag_run_id STRING,\n"
    
    hist_meta_cols = "_metadata_insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, \n" \
                     "_metadata_deleted STRING DEFAULT 'N',\n" \
                     "_metadata_dag_run_id STRING,\n"

    oradf['bq_type'] = oradf['DATA_TYPE'].map(datatype_map)
    oradf.loc[oradf.COLUMN=='RUN_DATE',["bq_type"]] = 'TIMESTAMP'
    oradf.loc[oradf.COLUMN=='KEY',["bq_type"]] = 'STRING'
    #print(oradf)
    oradf['ddl'] = oradf['COLUMN'] + ' ' + oradf['bq_type'] + ',\n'
    collist = list(oradf['ddl'].drop_duplicates())
    colstr = ''.join(collist)

    # Stg table ddl creation
    create = 'CREATE TABLE IF NOT EXISTS `project_name.' +  pc.STG_SCHEMA + '.' + stg_table_name + '`\n(\n'
    colstrstg = create + colstr + _metadata_insert_time
    colstrstg = colstrstg[:-2] + '\n);'

    # Stg del table creation
    createstgdel = 'CREATE TABLE IF NOT EXISTS `project_name.' +  pc.STG_SCHEMA + '.' + stg_del_table_name + '`\n(\n'
    colstrstgdel = createstgdel + colstr + _metadata_insert_time
    colstrstgdel = colstrstgdel[:-2] + '\n);'

    
    # Raw table ddl creation
    createraw = 'CREATE TABLE IF NOT EXISTS `project_name.' + pc.RAW_SCHEMA  + '.' + raw_table_name + '`\n(\n'
    createraw = createraw + colstr + raw_meta_cols
    colstrraw = createraw[:-2] + '\n)\nPARTITION BY DATE(_metadata_update_time);'

    # Hist table ddl creation
    createhist = 'CREATE TABLE IF NOT EXISTS `project_name.' + pc.HIST_SCHEMA + '.' + hist_table_name + '`\n(\n'
    createhist = createhist + colstr + hist_meta_cols
    colstrhist = createhist[:-2] + '\n)\nPARTITION BY DATE(_metadata_insert_time);'

    client = storage.Client(pc.PROJECT_NAME)
    bucket = client.get_bucket(pc.BUCKET_NAME)
    databucket = client.get_bucket(pc.PLATFORM_BUCKET_NAME)
    

    with open(os.path.join(target_path, 'STG', stg_table_name + '.sql'), 'w') as file:
        file.write(colstrstg)
    blob = bucket.blob(pc.TEMP_PATH + 'STG/' + stg_table_name + '.sql')
    blob.upload_from_string(colstrstg)

    with open(os.path.join(target_path, 'STG', stg_del_table_name + '.sql'), 'w') as file:
        file.write(colstrstgdel)
    blob = bucket.blob(pc.TEMP_PATH + 'STG_DEL/' + stg_del_table_name + '.sql')
    blob.upload_from_string(colstrstgdel)

    with open(os.path.join(target_path, 'RAW', raw_table_name + '.sql'), 'w') as file:
        file.write(colstrraw)
    blob = bucket.blob(pc.TEMP_PATH + 'RAW/' + raw_table_name + '.sql')
    blob.upload_from_string(colstrraw)

    with open(os.path.join(target_path, 'HIST', hist_table_name + '.sql'), 'w') as file:
        file.write(colstrhist)
    blob = bucket.blob(pc.TEMP_PATH + 'HIST/' + hist_table_name + '.sql')
    blob.upload_from_string(colstrhist)
    
    oradf['mode'] = 'NULLABLE'
    #print(oradf)
    oradf['name']=oradf['COLUMN']
    oradf['type']=oradf['bq_type']
    bqstgschema = oradf.loc[:, ['name', 'type', 'mode']].to_dict('records')
    bqstgschema_without_meta = deepcopy(bqstgschema)
    bqstgschema.append({
        "name": "_metadata_insert_time",
        "type": "TIMESTAMP",
        "mode": "NULLABLE",
        "defaultValueExpression": "CURRENT_TIMESTAMP"
    })

    # Steps to modify df for bq json for raw tables
    raworadf = oradf.loc[:, ['name', 'type', 'mode']]
    raworadf = raworadf.rename(columns={'raw_column_name': 'name'})
    bqrawschema = raworadf.to_dict('records')
    bqrawschema.append({
        "name": "_metadata_insert_time",
        "type": "TIMESTAMP",
        "mode": "NULLABLE",
        "defaultValueExpression": "CURENT_TIMESTAMP"
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
        "defaultValueExpression": "N"
    })
    bqrawschema.append({
        "name": "_metadata_dag_run_id",
        "type": "STRING",
        "mode": "NULLABLE"
    })


    # Steps to modify df for bq json for raw tables
    historadf = oradf.loc[:, ['name', 'type', 'mode']]
    historadf = historadf.rename(columns={'raw_column_name': 'name'})
    bqhistschema = historadf.to_dict('records')
    bqhistschema.append({
        "name": "_metadata_insert_time",
        "type": "TIMESTAMP",
        "mode": "NULLABLE",
        "defaultValueExpression": "CURENT_TIMESTAMP"
    })
    bqhistschema.append({
        "name": "_metadata_deleted",
        "type": "STRING",
        "mode": "NULLABLE",
        "defaultValueExpression": "N"
    })
    bqhistschema.append({
        "name": "_metadata_dag_run_id",
        "type": "STRING",
        "mode": "NULLABLE"
    })
    


    #To store json in local
    with open(os.path.join(target_path, pc.FUSION_SCHEMA, stg_table_name + '.json'), 'w') as file:
        file.write(json.dumps(bqstgschema_without_meta, indent=2))
    with open(os.path.join(target_path, pc.FUSION_SCHEMA, stg_del_table_name + '.json'), 'w') as file:
        file.write(json.dumps(bqstgschema_without_meta, indent=2))
    
    with open(os.path.join(target_path, 'JSON', stg_table_name + '.json'), 'w') as file:
        file.write(json.dumps(bqstgschema, indent=2))
    with open(os.path.join(target_path, 'JSON', stg_del_table_name + '.json'), 'w') as file:
        file.write(json.dumps(bqstgschema, indent=2))
    with open(os.path.join(target_path, 'JSON', raw_table_name + '.json'), 'w') as file:
        file.write(json.dumps(bqrawschema, indent=2))
    with open(os.path.join(target_path, 'JSON', hist_table_name + '.json'), 'w') as file:
        file.write(json.dumps(bqhistschema, indent=2))
    
    # Upload to fusion_schema folder
    blob = bucket.blob(pc.FUSION_SCHEMA + '/' + stg_table_name + '.json')
    blob.upload_from_string(json.dumps(bqstgschema_without_meta, indent=2))

    blob = bucket.blob(pc.FUSION_SCHEMA + '/' + stg_del_table_name + '.json')
    blob.upload_from_string(json.dumps(bqstgschema_without_meta, indent=2))

    # Upload to fusion_schema folder in that modules's respective bucket
    blob = databucket.blob( module.lower() + pc.SCHEMA_OBJECT_PATH + stg_table_name + '.json')
    blob.upload_from_string(json.dumps(bqstgschema_without_meta, indent=2))

    blob = databucket.blob( module.lower() + pc.SCHEMA_OBJECT_PATH + stg_del_table_name + '.json')
    blob.upload_from_string(json.dumps(bqstgschema_without_meta, indent=2))

    # Upload to temp_stg folder
    blob = bucket.blob(pc.TEMP_PATH + 'JSON_STG/' + stg_table_name + '.json')
    blob.upload_from_string(json.dumps(bqstgschema, indent=2))

    # Upload to temp_stg folder
    blob = bucket.blob(pc.TEMP_PATH + 'JSON_STG_DEL/' + stg_del_table_name + '.json')
    blob.upload_from_string(json.dumps(bqstgschema, indent=2))

    # Upload to temp_raw folder
    blob = bucket.blob(pc.TEMP_PATH + 'JSON_RAW/' + raw_table_name + '.json')
    blob.upload_from_string(json.dumps(bqrawschema, indent=2))

    # Upload to temp_hist folder
    blob = bucket.blob(pc.TEMP_PATH + 'JSON_HIST/' + hist_table_name + '.json')
    blob.upload_from_string(json.dumps(bqhistschema, indent=2))
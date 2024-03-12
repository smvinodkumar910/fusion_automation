#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from fusbq.path_config import DataSets
pc = DataSets('common')
from google.cloud import bigquery
from google.cloud import secretmanager
import argparse
import re
import fusbq.ingestion_tasks_creation as itc

pd.set_option('mode.chained_assignment', None)


def derive_where_clause(task_type,module,tables) -> str:
    if task_type in ['data_tasks_json', 'del_tasks_json']:
        #module = args.module
        if module is None:
            #tables = args.tables
            tables = '","'.join(tables.split(','))
            tables = '"' + tables + '"'
            where_clause = ' where TABLE_ID in ({})'.format(tables)
        else:
            where_clause = ' where MODULE = "{}" '.format(module)

    if task_type in ['tranformation_tasks_json']:
        #module = args.module
        if module is None:
            #tables = args.tables
            tables = '","'.join(tables.split(','))
            tables = '"' + tables + '"'
            where_clause = ' where TABLE_ID in ({}) and DW_TABLE_NAME is not null'.format(tables)
        else:
            where_clause = ' where MODULE = "{}" and DW_TABLE_NAME is not null'.format(module)
    
    return  where_clause

api_url = pc.REST_API_URL
user = pc.REST_API_USER
api_secret = pc.REST_API_SECRET
bicc_file_path = pc.BICC_FILE_PATH
pvolistpath = pc.PVO_LIST_PATH
base_pvo_path = pc.BASE_PVOS_PATH
project_name = pc.DATA_PROJECT_NAME
meta_dataset_name = pc.META_DATASET_NAME


def get_meta_data(data_store_name: str):
    client = secretmanager.SecretManagerServiceClient()
    name = 'projects/{}/secrets/{}/versions/latest'.format(pc.PROJECT_NAME, api_secret)
    response = client.access_secret_version(name=name)
    password = response.payload.data.decode('UTF-8')
    #password = api_secret
    #password = ''
    response = requests.get(api_url + '/meta/datastores/' + data_store_name, auth=HTTPBasicAuth(user, password))
    responsejson = response.json()
    #print(responsejson)
    pvocollist = responsejson['columns']
    df = pd.DataFrame(pvocollist)
    coldf = df.rename(columns={'dataType': 'type'})
    coldf['data_store_name'] = data_store_name
    return coldf


def get_table_name(pvos: list):
    # To get list of PVOs and correspondig Table Names
    biccdf = pd.read_csv(bicc_file_path)
    biccdf_new = biccdf.loc[biccdf['View Object'].isin(pvos)]
    # print(biccdf_new.columns)
    biccdf_keys = biccdf_new.loc[biccdf_new['Primary Key Column'] == 'Yes']
    biccdf_keys = biccdf_keys.loc[:, ['View Object', 'Database Table']].drop_duplicates()
    return biccdf_keys
    # biccdf_keys.to_csv(os.path.join(target_ddl_file_path, 'PVOS_AND_TABLES.csv'))


def stg_integration_id_derive(x):
    if x.type == 'VARCHAR':
        return 'COALESCE(' + x['name'] + ',\'\') '
    elif x.type == 'NUMERIC':
        return 'COALESCE(' + x['name'] + ', 0)'
    else:
        return 'COALESCE(CAST(' + x['name'] + ' as STRING),\'\') '


def primary_incremental_keys(coldf: pd.DataFrame):
    # to derive RNUM to avoid duplication
    primarykeydf = coldf.loc[coldf['isPrimaryKey'], :]
    # primarykeydf.loc[:, 'stg_integration_id'] = primarykeydf.apply(lambda x: stg_integration_id_derive(x), axis=1)

    # stg_integration_id = ",'~',".join(primarykeydf.stg_integration_id.values.tolist())
    # stg_integration_id = 'concat(' + stg_integration_id + ') '
    stg_integration_id = ",".join(primarykeydf.name.values.tolist())
    stg_integration_id = stg_integration_id.upper()

    lastupdatedf = coldf.loc[coldf['isLastUpdateDate'], ['name']]
    incremental_col = ",".join(lastupdatedf.name.values.tolist())
    # incremental_col = lastupdatedf.iloc[0]['name']
    incremental_col = str(incremental_col).upper()
    return stg_integration_id, incremental_col


def derive_col_name(col_name: str):
    chrs = set()
    for match in re.finditer(r'[A-Z][a-z]', str(col_name)):
        chrs.add(match.group())
    for x in chrs:
        col_name = re.sub(x, "_" + x, col_name)
    return col_name.replace('__', '_').upper()[1:]


def derive_table_name(a,table_type,source_type):
  if source_type=='BIP':
    bip_suffix = '_BIP'
  else:
    bip_suffix = ''
  startstr = a.MODULE+'_'
  if table_type=='RAW':
    if a.SOURCE_TABLE_NAME.startswith(startstr):
      return a.SOURCE_TABLE_NAME+bip_suffix
    else:
      return '{}_{}{}'.format(a.MODULE, a.SOURCE_TABLE_NAME,bip_suffix)
  else:
    if a.SOURCE_TABLE_NAME.startswith(startstr):
      return '{}_{}{}'.format(table_type,a.SOURCE_TABLE_NAME,bip_suffix)
    else:
      return '{}_{}_{}{}'.format(table_type,a.MODULE, a.SOURCE_TABLE_NAME,bip_suffix)

max_tableids = dict()

def derive_table_id(x):
    table_id = max_tableids[x.MODULE]
    table_id = table_id+1
    max_tableids[x.MODULE] = table_id
    if x.SOURCE_TYPE=='BICC':
        final_table_id = x.MODULE+'_'+str(table_id).rjust(3,'0')
    else:
        final_table_id = x.MODULE+'_'+x.SOURCE_TYPE+'_'+str(table_id).rjust(3,'0')
    return final_table_id

def generate_table_id(base_df:pd.DataFrame, source_type:str) -> pd.DataFrame :
    module_list = list(base_df.MODULE.unique())
    module_list = "\""+'","'.join(module_list)+"\""

    client = bigquery.Client('helix-data-dev')
    sql = "select MODULE, max(table_id) MAX_TABLE_ID from helix-data-dev.BQ_CTL_METADATA.FUSION_TO_BQ_PVO_DTL \
    WHERE MODULE IN ({0}) AND SOURCE_TYPE='{1}' GROUP BY MODULE ".format( module_list, source_type)

    #print(sql)
    pvodf = client.query(sql).to_dataframe()
    pvodict = pvodf.to_dict('records')

    if len(pvodict)>0:
        for a in pvodict:
            max_tableids[a['MODULE']] = int(a['MAX_TABLE_ID'].split('_')[-1])
    else:
        for module in module_list:
            max_tableids[module] = int(1)
    
    base_df['TABLE_ID'] = base_df.apply(lambda x:derive_table_id(x),axis=1)
    
    return base_df


def bicc_main():
    biccdf = pd.read_csv(bicc_file_path)
    pvos_df = pd.read_csv(pvolistpath)
    pvo_list = pvos_df.pvo_names.to_list()
    modules_set = set(pvos_df.module.to_list())
    for m in modules_set:
        if m not in pc.MODULES_LIST:
            raise Exception("Invalid module name "+m+" provided.Valid module names are "+",".join(pc.MODULES_LIST))

    base_df = get_table_name(pvo_list)
    # print(base_df)
    base_df = pd.merge(base_df, pvos_df, left_on='View Object', right_on='pvo_names')
    base_df = base_df.loc[:, ['View Object', 'Database Table', 'module']]
    base_df.rename(columns={'View Object': 'DATASTORE_NAME', 'Database Table': 'SOURCE_TABLE_NAME', 'module': 'MODULE'},
                   inplace=True)
    
    source_type='BICC'
    base_df['SOURCE_TYPE'] = source_type
    
    base_df = generate_table_id(base_df,source_type)
    
    
    print(base_df)
    for a in pvo_list:
        coldf = get_meta_data(a)

        merged_df = pd.merge(coldf, biccdf, how='left',
                             left_on=['data_store_name', 'name'],
                             right_on=['View Object', 'View Object Attribute'])
        merged_df = merged_df.rename(columns={'Database Column': 'raw_column_name'})
        merged_df = merged_df.loc[:,
                    ['name', 'type', 'isPrimaryKey', 'isLastUpdateDate', 'precision', 'scale',
                     'raw_column_name']]

        duplicate_cols = merged_df.groupby(['raw_column_name'])['raw_column_name'].count()
        duplicate_orig_cols = merged_df.groupby(['name'])['name'].count()
        duplicate_cols = duplicate_cols.loc[lambda x: x > 1].index
        duplicate_orig_cols = duplicate_orig_cols.loc[lambda x: x > 1].index
        raw_series = merged_df.loc[:, ['raw_column_name', 'name']].to_dict('records')
        raw_new_list = []
        # print(duplicate_cols)
        # print(duplicate_orig_cols)
        for b in raw_series:
            if pd.isna(b['raw_column_name']) or b['raw_column_name'] in duplicate_cols or b['raw_column_name'] in \
                    ['INTERVAL'] or b['name'] in duplicate_orig_cols:
                raw_new_list.append(derive_col_name(b['name']))
            else:
                raw_new_list.append(b['raw_column_name'])

        # merged_df['raw_column_name'] = raw_new_list
        # To have Stage table also have column names as that of Source
        merged_df['raw_column_name'] = merged_df['name'].str.upper()
        # merged_df['name'] = raw_new_list

        primary_key, incremental_key = primary_incremental_keys(merged_df)

        base_df.loc[base_df.DATASTORE_NAME == a, ['PRIMARY_KEY']] = primary_key
        base_df.loc[base_df.DATASTORE_NAME == a, ['INCREMENTAL_KEY']] = incremental_key
        print('completed processing pvo :' + a)
    base_df['STG_TABLE_NAME'] = base_df.apply(lambda x : derive_table_name(x,'STG',source_type) , axis=1)
    base_df['RAW_TABLE_NAME'] = base_df.apply(lambda x : derive_table_name(x,'RAW',source_type) , axis=1)
    base_df['DW_TABLE_NAME'] = ''
    base_df['DW_SIL_PROC'] = ''
    base_df['RUN_DDL_FLAG'] = True
    base_df['HIST_TABLE_NAME'] = base_df.apply(lambda x : derive_table_name(x,'DL',source_type) , axis=1)
    base_df['STG_DEL_TABLE_NAME'] = base_df.apply(lambda x : derive_table_name(x,'STG_DEL',source_type) , axis=1)
    base_df['TRACK_NAME'] = base_df['MODULE']
    base_df['SPECIFIC_COLS'] = False
    

    # CHANGE ORDER OF COLUMNS
    base_df = base_df.loc[:, ['TABLE_ID',
                              'SOURCE_TABLE_NAME',
                              'DATASTORE_NAME',
                              'STG_TABLE_NAME',
                              'RAW_TABLE_NAME',
                              'DW_TABLE_NAME',
                              'DW_SIL_PROC',
                              'RUN_DDL_FLAG',
                              'MODULE',
                              'HIST_TABLE_NAME',
                              'PRIMARY_KEY',
                              'INCREMENTAL_KEY',
                              'STG_DEL_TABLE_NAME',
                              'TRACK_NAME',
                              'SOURCE_TYPE',
                              'SPECIFIC_COLS'
                              ]]
    print(base_pvo_path)
    base_df.to_csv(base_pvo_path, index=None)

def bip_main():
    bip_df = pd.read_csv(pc.BIP_LIST_PATH)
    source_type='BIP'
    bip_df['SOURCE_TYPE'] = source_type
    bip_df = generate_table_id(bip_df,source_type)
    bip_df['DATASTORE_NAME'] = bip_df['BIP_REPORT']
    bip_df['STG_TABLE_NAME'] = bip_df.apply(lambda x : derive_table_name(x,'STG',source_type) , axis=1)
    bip_df['RAW_TABLE_NAME'] = bip_df.apply(lambda x : derive_table_name(x,'RAW',source_type) , axis=1)
    bip_df['DW_TABLE_NAME'] = ''
    bip_df['DW_SIL_PROC'] = ''
    bip_df['PRIMARY_KEY'] = ''
    bip_df['INCREMENTAL_KEY'] = ''
    bip_df['RUN_DDL_FLAG'] = True
    bip_df['HIST_TABLE_NAME'] = bip_df.apply(lambda x : derive_table_name(x,'DL',source_type) , axis=1)
    bip_df['STG_DEL_TABLE_NAME'] = bip_df.apply(lambda x : derive_table_name(x,'STG_DEL',source_type) , axis=1)
    bip_df['TRACK_NAME'] = bip_df['MODULE']
    bip_df['SPECIFIC_COLS'] = False

    bip_df = bip_df.loc[:, [  'TABLE_ID',
                              'SOURCE_TABLE_NAME',
                              'DATASTORE_NAME',
                              'STG_TABLE_NAME',
                              'RAW_TABLE_NAME',
                              'DW_TABLE_NAME',
                              'DW_SIL_PROC',
                              'RUN_DDL_FLAG',
                              'MODULE',
                              'HIST_TABLE_NAME',
                              'PRIMARY_KEY',
                              'INCREMENTAL_KEY',
                              'STG_DEL_TABLE_NAME',
                              'TRACK_NAME',
                              'SOURCE_TYPE',
                              'SPECIFIC_COLS'
                              ]]
    print(pc.BASE_BIP_PATH)
    bip_df.to_csv(pc.BASE_BIP_PATH, index=None)


def check_pvo_tb_exists(data_project_name, in_src_file, source_type):
    df = pd.read_csv(in_src_file)
    table_name = df.SOURCE_TABLE_NAME.to_list()
    datastore_name = df.DATASTORE_NAME.to_list()

    if source_type=='insertbip':
        condition = " and SOURCE_TYPE= 'BIP' "
    else:
        condition = " and SOURCE_TYPE= 'BICC' "

    table_where = "where SOURCE_TABLE_NAME in ('" + "','".join(table_name) + "') "+condition
    datastore_where = "where DATASTORE_NAME in ('" + "','".join(datastore_name) + "') "+condition
    sql = "select {column_name} from {project_name}.{meta_dataset}.FUSION_TO_BQ_PVO_DTL {where}"
    tb_query = sql.format(project_name=data_project_name, meta_dataset=meta_dataset_name, column_name= "SOURCE_TABLE_NAME", where=table_where)
    pvo_query = sql.format(project_name=data_project_name, meta_dataset=meta_dataset_name, column_name="DATASTORE_NAME", where=datastore_where)

    client = bigquery.Client(data_project_name)

    tbjob = client.query(tb_query)  # API request.
    tbresult = tbjob.result()
    tbcnt = tbresult.total_rows

    avail_ob = []
    if tbcnt > 0:
        tbdf = tbresult.to_dataframe()
        print(tbcnt)
        print(tbdf)
        print(tbdf.SOURCE_TABLE_NAME.to_list())
        avail_ob.extend(tbdf.SOURCE_TABLE_NAME.to_list())

    pvojob = client.query(pvo_query)  # API request.
    pvo_result = pvojob.result()
    pvocnt = pvo_result.total_rows

    if pvocnt > 0:
        pvodf = pvo_result.to_dataframe()
        print(pvodf.DATASTORE_NAME.to_list())
        avail_ob.extend(pvodf.DATASTORE_NAME.to_list())

    if tbcnt > 0 or pvocnt > 0:
        errormsg = "following PVOs/source_tables already available in meta table \n {avail_obs}"
        errormsg = errormsg.format(avail_obs=",".join(avail_ob))
        return False, errormsg
    else:
        return True, ""


def load_data_to_bq(data_project_name: str, in_table_id: str, in_src_file: str):
    # Construct a BigQuery client object.
    client = bigquery.Client(data_project_name)

    # Load data to to STG Table
    table_id = in_table_id

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("TABLE_ID", "STRING"),
            bigquery.SchemaField("SOURCE_TABLE_NAME", "STRING"),
            bigquery.SchemaField("DATASTORE_NAME", "STRING"),
            bigquery.SchemaField("STG_TABLE_NAME", "STRING"),
            bigquery.SchemaField("RAW_TABLE_NAME", "STRING"),
            bigquery.SchemaField("DW_TABLE_NAME", "STRING"),
            bigquery.SchemaField("DW_SIL_PROC", "STRING"),
            bigquery.SchemaField("RUN_DDL_FLAG", "BOOLEAN"),
            bigquery.SchemaField("MODULE", "STRING"),
            bigquery.SchemaField("HIST_TABLE_NAME", "STRING"),
            bigquery.SchemaField("PRIMARY_KEY", "STRING"),
            bigquery.SchemaField("INCREMENTAL_KEY", "STRING"),
            bigquery.SchemaField("STG_DEL_TABLE_NAME", "STRING"),
            bigquery.SchemaField("TRACK_NAME", "STRING"),
            bigquery.SchemaField("SOURCE_TYPE", "STRING"),
            bigquery.SchemaField("SPECIFIC_COLS","BOOLEAN")
        ],
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV
    )
    uri = in_src_file

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.

    print("Loaded {} rows.".format(destination_table.num_rows))


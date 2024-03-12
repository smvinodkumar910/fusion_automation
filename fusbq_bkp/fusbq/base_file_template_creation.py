#!/usr/bin/env python
# coding: utf-8
"""
Arguments Required :

generate -- Creates BASE_PVOs.csv file which contains details
            to be inserted in tbe table FUSION_TO_BQ_PVO_DTL
insert -- insert records into the table FUSION_TO_BQ_PVO_DTL
data_tasks_json - creates json file contains list of data load tasks (upto RAW layer)
del_tasks_json - creates json file contains list of soft delete tasks (upto RAW layer)

python -m fusbq.base_file_template_creation generate
python -m fusbq.base_file_template_creation insert
python -m fusbq.base_file_template_creation data_tasks_json --module GL
python -m fusbq.base_file_template_creation del_tasks_json --module GL
"""

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import fusbq.path_config as pc
from google.cloud import bigquery
import argparse
import re
import fusbq.ingestion_tasks_creation as itc

import os

pd.set_option('mode.chained_assignment', None)

parser = argparse.ArgumentParser()
subparser = parser.add_subparsers(dest='task_type')
generate = subparser.add_parser('generate')
insert = subparser.add_parser('insert')
data_tasks_json = subparser.add_parser('data_tasks_json')
del_tasks_json = subparser.add_parser('del_tasks_json')

data_tasks_json.add_argument("-module", '--module',
                             help="module - values in FUSION_TO_BQ_PVO_DTL.MODULE column", required=True)
del_tasks_json.add_argument("-module", '--module',
                            help="module - values in FUSION_TO_BQ_PVO_DTL.MODULE column", required=True)

args = parser.parse_args()

task_type = args.task_type

if task_type in ['data_tasks_json', 'del_tasks_json']:
    module = args.module

api_url = pc.REST_API_URL
user = pc.REST_API_USER
bicc_file_path = pc.BICC_FILE_PATH
pvolistpath = pc.PVO_LIST_PATH
base_pvo_path = pc.BASE_PVOS_PATH
project_name = pc.DATA_PROJECT_NAME
meta_dataset_name = pc.META_DATASET_NAME


def get_meta_data(data_store_name: str):
    password = 'svc-eqx_helix1'
    response = requests.get(api_url + '/meta/datastores/' + data_store_name, auth=HTTPBasicAuth(user, password))
    responsejson = response.json()
    pvocollist = responsejson['columns']
    df = pd.DataFrame(pvocollist)
    coldf = df.rename(columns={'dataType': 'type'})
    coldf['data_store_name'] = data_store_name
    return coldf


def get_table_name(pvos: list):
    # To get list of PVOs and correspondig Table Names
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

    lastupdatedf = coldf.loc[coldf['isLastUpdateDate'], ['name']]
    incremental_col = lastupdatedf.iloc[0]['name']
    return stg_integration_id, incremental_col


def derive_col_name(col_name: str):
    chrs = set()
    for match in re.finditer(r'[A-Z][a-z]', str(col_name)):
        chrs.add(match.group())
    for x in chrs:
        col_name = re.sub(x, "_" + x, col_name)
    return col_name.replace('__', '_').upper()[1:]


def main():
    pvos_df = pd.read_csv(pvolistpath)
    pvo_list = pvos_df.pvo_names.to_list()

    base_df = get_table_name(pvo_list)
    # print(base_df)
    base_df = pd.merge(base_df, pvos_df, left_on='View Object', right_on='pvo_names')
    base_df = base_df.loc[:, ['View Object', 'Database Table', 'module']]
    base_df.rename(columns={'View Object': 'DATASTORE_NAME', 'Database Table': 'SOURCE_TABLE_NAME', 'module': 'MODULE'},
                   inplace=True)

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

    base_df['STG_TABLE_NAME'] = 'STG_FUSION_' + base_df['SOURCE_TABLE_NAME']
    base_df['RAW_TABLE_NAME'] = 'FUSION_' + base_df['SOURCE_TABLE_NAME']
    base_df['DW_TABLE_NAME'] = 'DW_FUSION_' + base_df['SOURCE_TABLE_NAME']
    base_df['RAW_SIL_PROC'] = 'PRC_SIL_RAW_' + base_df['SOURCE_TABLE_NAME']
    base_df['DW_SIL_PROC'] = 'PRC_SIL_DW_' + base_df['SOURCE_TABLE_NAME']
    base_df['RUN_DDL_FLAG'] = True
    base_df['HIST_TABLE_NAME'] = 'DL_FUSION_' + base_df['SOURCE_TABLE_NAME']
    base_df['STG_DEL_TABLE_NAME'] = 'STG_FUSION_DEL_' + base_df['SOURCE_TABLE_NAME']
    base_df['RAW_DEL_PLP_PROC'] = 'PRC_PLP_RAW_' + base_df['SOURCE_TABLE_NAME'] + '_DEL'

    # CHANGE ORDER OF COLUMNS
    base_df = base_df.loc[:, ['SOURCE_TABLE_NAME',
                              'DATASTORE_NAME',
                              'STG_TABLE_NAME',
                              'RAW_TABLE_NAME',
                              'DW_TABLE_NAME',
                              'RAW_SIL_PROC',
                              'DW_SIL_PROC',
                              'RUN_DDL_FLAG',
                              'MODULE',
                              'HIST_TABLE_NAME',
                              'PRIMARY_KEY',
                              'INCREMENTAL_KEY',
                              'STG_DEL_TABLE_NAME',
                              'RAW_DEL_PLP_PROC']]
    print(base_pvo_path)
    base_df.to_csv(base_pvo_path, index=None)


def load_data_to_bq(project_name: str, in_table_id: str, in_src_file: str):
    # Construct a BigQuery client object.
    client = bigquery.Client(project_name)

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
            bigquery.SchemaField("RAW_SIL_PROC", "STRING"),
            bigquery.SchemaField("DW_SIL_PROC", "STRING"),
            bigquery.SchemaField("RUN_DDL_FLAG", "BOOLEAN"),
            bigquery.SchemaField("MODULE", "STRING"),
            bigquery.SchemaField("HIST_TABLE_NAME", "STRING"),
            bigquery.SchemaField("PRIMARY_KEY", "STRING"),
            bigquery.SchemaField("INCREMENTAL_KEY", "STRING"),
            bigquery.SchemaField("STG_DEL_TABLE_NAME", "STRING"),
            bigquery.SchemaField("RAW_DEL_PLP_PROC", "STRING")
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


if __name__ == "__main__":
    biccdf = pd.read_csv(bicc_file_path)
    if task_type == 'generate':
        main()
    elif task_type == 'insert':
        table_id = '{}.{}.{}'.format(project_name, meta_dataset_name, 'FUSION_TO_BQ_PVO_DTL')
        load_data_to_bq(project_name, table_id, base_pvo_path)
    elif task_type == 'data_tasks_json':
        itc.create_task_list(task_type, module)
    elif task_type == 'del_tasks_json':
        itc.create_task_list(task_type, module)

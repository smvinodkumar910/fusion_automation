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

python -m fusbq.fusion_automation_local bicc
python -m fusbq.fusion_automation_local bip

"""

import os
import sys
import traceback
import argparse

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from fusbq.bq_ddl_automate.ddl_creation import create_bq_ddl
from fusbq.bq_ddl_automate.bip_ddl_creation import create_bq_ddl_bip,bip_cf_trigger
from fusbq.path_config import DataSets as pc
pc = pc('common')
from google.cloud import bigquery


parser = argparse.ArgumentParser()
subparser = parser.add_subparsers(dest='task_type')
bicc = subparser.add_parser('bicc')
bip = subparser.add_parser('bip')
bipcustom = subparser.add_parser('bipcustom')

args = parser.parse_args()

task_type = args.task_type


base_excel_path = pc.BASE_PVOS_PATH
target_ddl_file_path = os.path.join(pc.ROOT_DIR, pc.DDL_TGT_PATH)
gcs_target_ddl_file_path = pc.DDL_TGT_PATH
bicc_file_path = pc.BICC_FILE_PATH
project_name = pc.PROJECT_NAME
bucket_name = pc.BUCKET_NAME
meta_mode = 'api'
meta_path = ""
temp_path = pc.TEMP_PATH
meta_dataset_name = pc.META_DATASET_NAME
data_project_name = pc.DATA_PROJECT_NAME

api_url = pc.REST_API_URL
api_user = pc.REST_API_USER
api_secret = pc.REST_API_SECRET

# Creates target directory if not exists
os.makedirs(target_ddl_file_path, exist_ok=True)


def get_meta_data_api(data_store_name: str, table_name: str):
    # client = secretmanager.SecretManagerServiceClient()
    # name = 'projects/{}/secrets/{}/versions/latest'.format(project_name, api_secret)
    # response = client.access_secret_version(name=name)
    # password = response.payload.data.decode('UTF-8')
    password = 'Equinix123'
    response = requests.get(api_url + '/meta/datastores/' + data_store_name, auth=HTTPBasicAuth(api_user, password))
    responsejson = response.json()
    pvocollist = responsejson['columns']
    df = pd.DataFrame(pvocollist)
    coldf = df.rename(columns={'dataType': 'type'})
    coldf['data_store_name'] = data_store_name
    coldf['table_name'] = table_name
    return coldf


def get_meta_data_csv(datastore_name: str, table_name: str):
    file_name = datastore_name.lower().replace('.', '_')
    file_name = 'file_' + file_name + '.csv'

    metadf = pd.read_csv(pc.pvo_meta_path + file_name, index_col=False)

    # file_path = os.path.join(meta_path, file_name)
    # print('PROCESSING :' + file_path)
    # metadf = pd.read_csv(file_path, index_col=False)
    metadf['data_store_name'] = datastore_name
    metadf['table_name'] = table_name
    metadf = metadf.rename(columns={'Column Name': 'name', 'Data Type': 'type', 'Primary Key': 'isPrimaryKey',
                                    'Precision': 'precision', 'Scale': 'scale', 'Incremental Key': 'isLastUpdateDate'})
    metadf['isPrimaryKey'] = metadf['isPrimaryKey'].map({'Yes': True, 'No': False})
    metadf['isLastUpdateDate'] = metadf['isLastUpdateDate'].map({'Yes': True, 'No': False})
    return metadf


def ddl_create():
    try:
        client = bigquery.Client(data_project_name)
        sql = 'select TABLE_ID,SOURCE_TABLE_NAME,DATASTORE_NAME,STG_TABLE_NAME,' \
              'RAW_TABLE_NAME, DW_TABLE_NAME,HIST_TABLE_NAME , DW_SIL_PROC,RUN_DDL_FLAG,MODULE, ' \
              'STG_DEL_TABLE_NAME, SPECIFIC_COLS ' \
              'from {}.{}.FUSION_TO_BQ_PVO_DTL ' \
              'WHERE RUN_DDL_FLAG=true and SOURCE_TYPE=\'BICC\'  ORDER BY TABLE_ID ASC '.format(data_project_name, meta_dataset_name)

        pvodf = client.query(sql).to_dataframe()
        #pvodf = pd.read_csv(base_excel_path)
        pvodict = pvodf.to_dict('records')

        # bicc_blob = bucket.get_blob(bicc_file_path)
        # bicc_target_path = os.path.join(pc.ROOT_DIR, 'BICC_FSCM_Database_Mapping_with_ViewObjects.csv')
        # bicc_blob.download_to_filename(bicc_target_path)
        datadictionarydf = pd.read_csv(bicc_file_path)

        datastores = pvodf['DATASTORE_NAME'].dropna().to_list()

        datadictionarydf = datadictionarydf[datadictionarydf['View Object'].isin(datastores)]

        for a in pvodict:
            print('Started Processing ' + a['SOURCE_TABLE_NAME'])
            # Loop thru dict get metadata from Rest API and generate ddl
            if meta_mode == 'api':
                metadf = get_meta_data_api(a['DATASTORE_NAME'], a['SOURCE_TABLE_NAME'])
            # Loop thru dict get metadata from CSV file and generate ddl
            elif meta_mode == 'csv':
                metadf = get_meta_data_csv(a['DATASTORE_NAME'], a['SOURCE_TABLE_NAME'])
            
            if a['SPECIFIC_COLS']==True:

                file_name = a.get('DATASTORE_NAME').lower()
                file_name = 'file_' + file_name.replace('.', '_') + '-batch'
                file_name = file_name + '*.csv'
                metadf['COLUMN_NAME']=metadf['name'].str.upper()
                #Filter required columns:
                print(file_name)
                reqcols = pd.read_csv(pc.SAMPLE_EXTRACT_PATH+file_name,nrows=5)
                selectcols = list(reqcols.columns)
                metadf = metadf.loc[metadf['COLUMN_NAME'].isin(selectcols)]

            # Join BICC file & meta data to get raw_table_column names
            merged_df = pd.merge(metadf, datadictionarydf, how='left',
                                 left_on=['data_store_name', 'name'],
                                 right_on=['View Object', 'View Object Attribute'])
            merged_df = merged_df.rename(columns={'Database Column': 'raw_column_name'})
            merged_df = merged_df.loc[:,
                        ['name', 'type', 'isPrimaryKey', 'isLastUpdateDate', 'precision', 'scale', 'table_name',
                         'raw_column_name']]
            # merged_df.to_csv(os.path.join(target_ddl_file_path, 'merged_df.csv'))

            # Call the create_bq_ddl method to create ddl scripts
            create_bq_ddl(merged_df, a['SOURCE_TABLE_NAME'], a['STG_TABLE_NAME'], a['RAW_TABLE_NAME'],
                          a['DW_TABLE_NAME'], a['HIST_TABLE_NAME'], a['STG_DEL_TABLE_NAME'],
                          target_ddl_file_path,  a['MODULE'])

            print('DDL Creation completed for ' + a['SOURCE_TABLE_NAME'])

    except Exception as e:
        print(type(e))  # the exception instance
        print(e.args)  # arguments stored in .args
        traceback.print_exc()
        sys.exit(1)


def ddl_create_bip_custom():
    try:
        client = bigquery.Client(data_project_name)
        sql = 'select TABLE_ID,SOURCE_TABLE_NAME,DATASTORE_NAME,STG_TABLE_NAME,' \
              'RAW_TABLE_NAME, DW_TABLE_NAME,HIST_TABLE_NAME , DW_SIL_PROC,RUN_DDL_FLAG,MODULE, ' \
              'STG_DEL_TABLE_NAME, SPECIFIC_COLS ' \
              'from {}.{}.FUSION_TO_BQ_PVO_DTL ' \
              'WHERE RUN_DDL_FLAG=true and SOURCE_TYPE=\'BIP\' ORDER BY TABLE_ID ASC '.format(data_project_name, meta_dataset_name)

        pvodf = client.query(sql).to_dataframe()
        #pvodf = pd.read_csv(base_excel_path)
        pvodict = pvodf.to_dict('records')

        '''
        data_reports = dict()
        for a in pvodict:
            data_reports[a['SOURCE_TABLE_NAME']]=a['DATASTORE_NAME']
        payload = pc.BIP_CF_PROPS
        payload['data_reports']=data_reports
        print(payload)
        #Call cloudfunction-soap api to get column details from BIP
        #bip_cf_trigger(payload)
        

        req_cols_df = pd.read_csv(pc.BIP_REQ_COL_TGT_PATH)
        ora_cols = pd.read_csv(pc.BIP_TABLE_SCH_PATH)
        '''


        for a in pvodict:
            print('Start procesing  ' + a['SOURCE_TABLE_NAME'])
            #req_fil_cols_df = req_cols_df.loc[req_cols_df.TABLE==a['SOURCE_TABLE_NAME']]
            #merge_df = pd.merge(req_fil_cols_df,ora_cols,how='left',right_on=['TABLE_NAME','COLUMN_NAME'], left_on=['TABLE','COLUMN'])
            #merge_df = merge_df.loc[:,['TABLE','COLUMN', 'DATA_TYPE','DATA_LENGTH']]
            merge_df = pd.read_csv('C:\\Users\\VinodKumarM\\Downloads\\AR_CHARGE_LINES_STRUCTRUE.csv')

            create_bq_ddl_bip(merge_df,a['SOURCE_TABLE_NAME'], a['STG_TABLE_NAME'], a['RAW_TABLE_NAME'],
                          a['DW_TABLE_NAME'], a['HIST_TABLE_NAME'], a['STG_DEL_TABLE_NAME'],
                          target_ddl_file_path,  a['MODULE'])
            print('DDL Creation completed for ' + a['SOURCE_TABLE_NAME'])

    except Exception as e:
        print(type(e))  # the exception instance
        print(e.args)  # arguments stored in .args
        traceback.print_exc()
        sys.exit(1)


def ddl_create_bip():
    try:
        client = bigquery.Client(data_project_name)
        sql = 'select TABLE_ID,SOURCE_TABLE_NAME,DATASTORE_NAME,STG_TABLE_NAME,' \
              'RAW_TABLE_NAME, DW_TABLE_NAME,HIST_TABLE_NAME , DW_SIL_PROC,RUN_DDL_FLAG,MODULE, ' \
              'STG_DEL_TABLE_NAME, SPECIFIC_COLS ' \
              'from {}.{}.FUSION_TO_BQ_PVO_DTL ' \
              'WHERE RUN_DDL_FLAG=true and SOURCE_TYPE=\'BIP\' ORDER BY TABLE_ID ASC '.format(data_project_name, meta_dataset_name)

        pvodf = client.query(sql).to_dataframe()
        #pvodf = pd.read_csv(base_excel_path)
        pvodict = pvodf.to_dict('records')

        data_reports = dict()
        for a in pvodict:
            data_reports[a['SOURCE_TABLE_NAME']]=a['DATASTORE_NAME']
        payload = pc.BIP_CF_PROPS
        payload['data_reports']=data_reports
        print(payload)
        #Call cloudfunction-soap api to get column details from BIP
        #bip_cf_trigger(payload)

        req_cols_df = pd.read_csv(pc.BIP_REQ_COL_TGT_PATH)
        ora_cols = pd.read_csv(pc.BIP_TABLE_SCH_PATH)


        for a in pvodict:
            print('Start procesing  ' + a['SOURCE_TABLE_NAME'])
            req_fil_cols_df = req_cols_df.loc[req_cols_df.TABLE==a['SOURCE_TABLE_NAME']]
            merge_df = pd.merge(req_fil_cols_df,ora_cols,how='left',right_on=['TABLE_NAME','COLUMN_NAME'], left_on=['TABLE','COLUMN'])
            merge_df = merge_df.loc[:,['TABLE','COLUMN', 'DATA_TYPE','DATA_LENGTH']]

            create_bq_ddl_bip(merge_df,a['SOURCE_TABLE_NAME'], a['STG_TABLE_NAME'], a['RAW_TABLE_NAME'],
                          a['DW_TABLE_NAME'], a['HIST_TABLE_NAME'], a['STG_DEL_TABLE_NAME'],
                          target_ddl_file_path,  a['MODULE'])
            print('DDL Creation completed for ' + a['SOURCE_TABLE_NAME'])

    except Exception as e:
        print(type(e))  # the exception instance
        print(e.args)  # arguments stored in .args
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    if task_type.lower()=='bicc':
        ddl_create()
    elif task_type.lower()=='bip':
        ddl_create_bip()
    elif task_type.lower()=='bipcustom':
        ddl_create_bip_custom()
        
    

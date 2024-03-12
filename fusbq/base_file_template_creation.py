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

python -m fusbq.base_file_template_creation generatebip
python -m fusbq.base_file_template_creation insertbip

python -m fusbq.base_file_template_creation data_tasks_json --module GL
python -m fusbq.base_file_template_creation del_tasks_json --module GL
python -m fusbq.base_file_template_creation tranformation_tasks_json --module "AR"

python -m fusbq.base_file_template_creation data_tasks_json --tables "GL_TEST01,GL_TEST02"
python -m fusbq.base_file_template_creation del_tasks_json --tables "GL_TEST01,GL_TEST02"

python -m fusbq.base_file_template_creation tranformation_tasks_json --tables "GL_TEST01,GL_TEST02"

"""

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
import fusbq.base_file_template_fn as basepy

pd.set_option('mode.chained_assignment', None)

parser = argparse.ArgumentParser()
subparser = parser.add_subparsers(dest='task_type')
generate = subparser.add_parser('generate')
generatebip = subparser.add_parser('generatebip')
insert = subparser.add_parser('insert')
insertbip = subparser.add_parser('insertbip')
data_tasks_json = subparser.add_parser('data_tasks_json')
del_tasks_json = subparser.add_parser('del_tasks_json')
transform_tasks_json = subparser.add_parser('tranformation_tasks_json')

data_grp = data_tasks_json.add_mutually_exclusive_group()
del_grp = del_tasks_json.add_mutually_exclusive_group()
transform_grp = transform_tasks_json.add_mutually_exclusive_group()

data_grp.add_argument("-module", '--module',
                      help="module - values in FUSION_TO_BQ_PVO_DTL.MODULE column")
data_grp.add_argument("-tables", '--tables',
                      help="List of tables - values in FUSION_TO_BQ_PVO_DTL.TABLE_ID column")
del_grp.add_argument("-module", '--module',
                     help="module - values in FUSION_TO_BQ_PVO_DTL.MODULE column")
del_grp.add_argument("-tables", '--tables',
                     help="List of Tables - values in FUSION_TO_BQ_PVO_DTL.TABLE_ID column")

transform_grp.add_argument("-module", '--module',
                     help="module - values in FUSION_TO_BQ_PVO_DTL.MODULE column")
transform_grp.add_argument("-tables", '--tables',
                     help="List of Tables - values in FUSION_TO_BQ_PVO_DTL.TABLE_ID column")


args = parser.parse_args()

task_type = args.task_type
if (task_type in ['data_tasks_json','del_tasks_json','transform_tasks_json']):
    module = args.module
    tables = args.tables

api_url = pc.REST_API_URL
user = pc.REST_API_USER
api_secret = pc.REST_API_SECRET
bicc_file_path = pc.BICC_FILE_PATH
pvolistpath = pc.PVO_LIST_PATH
base_pvo_path = pc.BASE_PVOS_PATH
project_name = pc.DATA_PROJECT_NAME
meta_dataset_name = pc.META_DATASET_NAME


if __name__ == "__main__":
    if task_type == 'generate':
        biccdf = pd.read_csv(bicc_file_path)
        basepy.bicc_main()
    elif task_type == 'generatebip':
        basepy.bip_main()
    elif task_type == 'insert':
        table_id = '{}.{}.{}'.format(project_name, meta_dataset_name, 'FUSION_TO_BQ_PVO_DTL')
        can_proceed, error_msg = basepy.check_pvo_tb_exists(project_name,base_pvo_path,task_type)
        if can_proceed:
            basepy.load_data_to_bq(project_name, table_id, base_pvo_path)
        else:
            raise Exception(error_msg)
    elif task_type=='insertbip':
        table_id = '{}.{}.{}'.format(project_name, meta_dataset_name, 'FUSION_TO_BQ_PVO_DTL')
        can_proceed, error_msg = basepy.check_pvo_tb_exists(project_name,pc.BASE_BIP_PATH,task_type)
        if can_proceed:
            basepy.load_data_to_bq(project_name, table_id, pc.BASE_BIP_PATH)
        else:
            raise Exception(error_msg)
    elif task_type == 'data_tasks_json':
        itc.create_task_list(task_type, basepy.derive_where_clause(task_type,module,tables))
    elif task_type == 'del_tasks_json':
        itc.create_task_list(task_type, basepy.derive_where_clause(task_type,module,tables))
    elif task_type == 'tranformation_tasks_json':
        itc.create_task_list(task_type, basepy.derive_where_clause(task_type,module,tables))

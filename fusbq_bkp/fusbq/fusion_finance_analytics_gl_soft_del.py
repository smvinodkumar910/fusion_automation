import os
from datetime import datetime, timezone
from airflow import models

from airflow.providers.google.cloud.operators.dataflow import (
    DataflowStartFlexTemplateOperator
)

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator
)
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from google.cloud import bigquery
from fusbq import path_config as pc
import time
from google.cloud import bigquery
import json
import google.auth.transport.requests
from google.auth.transport.requests import AuthorizedSession

SCRIPT_NAME = os.path.basename(__file__)

task_configs = pc.get_configs(SCRIPT_NAME)

PLATFORM_PROJECT_ID = task_configs["platformProjectId"]
DATA_BUCKET = task_configs["bucketName"]
INBOUND_FOLDER_PATH = task_configs["inboundFolderPath"]
PROCESSED_FOLDER_PATH = task_configs["processedFolderPath"]
JSON_TO_CSV_FOLDER_PATH = task_configs["jsonToCsvFolderPath"]
METADATA_TO_CSV_FOLDER_PATH = task_configs["metadataToCsvFolderPath"]
CONTROL_SCHDULE_TABLE_NAME = task_configs["controlSchduleTableName"]
SOAP_END_POINT_URL = task_configs["soapEndPointUrl"]
SOAP_ACTION = task_configs["soapAction"]
SOAP_ENV = task_configs["soapEnv"]
SOAP_UCM = task_configs["soapUcm"]
USERNAME = task_configs["username"]
SECRET = task_configs["secret"]
REGION = task_configs["region"]
SUBNETWORK = task_configs["subnetwork"]
DATASET_NAME = task_configs["dataset_name"]
CONTROL_LOGIN = task_configs["control_login"]
BQ_DM_DATASET_NAME = task_configs["bq_dm_dataset_name"]
BQ_STG_DATASET_NAME = task_configs["bq_stg_dataset_name"]
FUSION_SCHEMA = task_configs["fusion_schema"]
DATA_PROJECT_ID = task_configs["dataProjectId"]
BIP_FILES = task_configs["bip_files"]
bicc_schedule_names = task_configs["bicc_schedule_names"]
archive_path = task_configs['archive_path']
timestr = time.strftime("-%Y%m%d-%H%M%S")
flex_template_bucket = task_configs['flex_template_bucket']


def task_failure(context):
    """failure setup"""
    error = context.get("exception")
    error_message = error.replace("\n", "")

    query_stmt = """CALL `{v_project_id}.{v_dataset_name}.BQ_CTL_PROC_LOGGING`\
                    ('{V_Batch_Name}','{V_Task_Name}',null,null,'{V_Task_Name}','FAILED'\
                    ,'{v_start_date}','{v_end_date}',null,'{V_ERROR_MSG}',null);"""
    query_task_failure = query_stmt.format(V_Batch_Name=context['dag'].dag_id,
                                           V_Task_Name=context['task_instance'].task_id,
                                           v_start_date=context['task_instance'].start_date,
                                           v_end_date=context['task_instance'].end_date, v_project_id=DATA_PROJECT_ID,
                                           V_ERROR_MSG=error_message, v_dataset_name=DATASET_NAME,
                                           v_control_login=CONTROL_LOGIN)

    print("Query for task_failure" + query_task_failure)
    print("error_msg is " + context.get("exception"))
    bq_client = bigquery.Client(project=DATA_PROJECT_ID)
    bq_client.query(query_task_failure).result()


def task_success(context):
    """task success steps"""
    query_stmt = """CALL `{v_project_id}.{v_dataset_name}.BQ_CTL_PROC_LOGGING`\
                    ('{V_Batch_Name}','{V_Task_Name}',null,null,'{V_Task_Name}','COMPLETED'\
                    ,'{v_start_date}','{v_end_date}',null,null,null);"""
    query_task_success = query_stmt.format(V_Batch_Name=context['dag'].dag_id,
                                           V_Task_Name=context['task_instance'].task_id,
                                           v_start_date=context['task_instance'].start_date,
                                           v_end_date=context['task_instance'].end_date, v_project_id=DATA_PROJECT_ID,
                                           v_dataset_name=DATASET_NAME, v_control_login=CONTROL_LOGIN)

    print("Query for task_success" + query_task_success)
    bq_client = bigquery.Client(project=DATA_PROJECT_ID)
    bq_client.query(query_task_success).result()


def pre_batch(**kwargs):
    """pre batch setup"""
    context = kwargs
    dag_run_id = context['dag_run'].run_id
    print(context)
    query_stmt = """CALL `{v_project_id}.{v_dataset_name}.BQ_PRE_BATCH_CTL_PROC_LOGGING`
    ('{V_Batch_Name}','{v_dag_run_id}');"""

    query_stmt_2 = """UPDATE `{v_project_id}.{v_dataset_name}.{v_control_login}`
    set DAG_RUN_ID = '{v_dag_run_id}' {bicc_schedule_names};"""
    print('dag_id :' + context['dag'].dag_id)
    query_pre_batch_2 = query_stmt_2.format(V_Batch_Name=context['dag'].dag_id, v_project_id=DATA_PROJECT_ID,
                                            v_dataset_name=DATASET_NAME, v_control_login=CONTROL_LOGIN,
                                            v_dag_run_id=dag_run_id,bicc_schedule_names=bicc_schedule_names)

    query_pre_batch = query_stmt.format(V_Batch_Name=context['dag'].dag_id, v_project_id=DATA_PROJECT_ID,
                                        v_dataset_name=DATASET_NAME, v_dag_run_id=dag_run_id)

    print("Query for pre_batch" + query_pre_batch)
    bq_client = bigquery.Client(project=DATA_PROJECT_ID)
    bq_client.query(query_pre_batch_2).result()
    bq_client.query(query_pre_batch).result()


def post_batch(**kwargs):
    """post batch setup"""
    context = kwargs
    print(context)
    query_stmt = """CALL `{v_project_id}.{v_dataset_name}.BQ_POST_BATCH_CTL_PROC_LOGGING`('{V_Batch_Name}','COMPLETED');"""
    query_post_batch = query_stmt.format(V_Batch_Name=context['dag'].dag_id, v_project_id=DATA_PROJECT_ID,
                                         v_dataset_name=DATASET_NAME)
    print("Query for post_batch" + query_post_batch)
    bq_client = bigquery.Client(project=DATA_PROJECT_ID)
    bq_client.query(query_post_batch).result()

def task_failemail_notification(self):
    current_utc_time = datetime.now(timezone.utc)
    current_utc_time = current_utc_time.strftime("%d-%b-%Y %H:%M:%S")
    html1 = "<html>Failure of DAG run has been encountered! Kindly have a look.<br><br>DAG: fusion_finance_analytics_gl<br></html>"
    html2 = current_utc_time+" UTC"
    html3 = "<html><br><br><br><br>Thanks</html>"
    print(current_utc_time)
    final_html = html1+html2+html3
    print(final_html)
    url = "https://us-west1-it-helix-platform-dev.cloudfunctions.net/mail-notification"
    request = google.auth.transport.requests.Request()
    id_token_credentials = id_token_credential_utils.get_default_id_token_credentials(
        url, request=request)
    resp = AuthorizedSession(id_token_credentials).get(url=url, params={
        "subject": "[SIT] DAG Failure :: DAG fusion_finance_analytics_gl in helix-platform-sit",
        "body": final_html,
        "recipients": "aayushi@equinix.com,vmadhavan@equinix.com",
        "sender": "equinix_gcp_cloud_composer_dev@ap.equinix.com"
    })
    if resp.status_code != 200:
        raise Exception("Cloud function failure.")

def task_successemail_notification(self):
    current_utc_time = datetime.now(timezone.utc)
    current_utc_time = current_utc_time.strftime("%d-%b-%Y %H:%M:%S")
    html1 = "<html>Fusion Finance GL DAG Completed successfully<br><br>DAG: fusion_finance_analytics_gl<br></html>"
    html2 = current_utc_time+" UTC"
    html3 = "<html><br><br><br><br>Thanks</html>"
    print(current_utc_time)
    final_html = html1+html2+html3
    print(final_html)
    url = "https://us-west1-it-helix-platform-dev.cloudfunctions.net/mail-notification"
    request = google.auth.transport.requests.Request()
    id_token_credentials = id_token_credential_utils.get_default_id_token_credentials(
        url, request=request)
    resp = AuthorizedSession(id_token_credentials).get(url=url, params={
        "subject": "[SIT] DAG Success :: DAG fusion_finance_analytics_gl in helix-platform-sit",
        "body": final_html,
        "recipients": "aayushi@equinix.com,vmadhavan@equinix.com",
        "sender": "equinix_gcp_cloud_composer_dev@ap.equinix.com"
    })
    if resp.status_code != 200:
        raise Exception("Cloud function failure.")

def post_batch_fail(**kwargs):
    """post batch setup"""
    context = kwargs
    print(context)
    query_stmt = """CALL `{v_project_id}.{v_dataset_name}.BQ_POST_BATCH_FAIL_CTL_PROC_LOGGING`('{V_Batch_Name}', 'FAILED');"""
    query_post_batch_fail = query_stmt.format(V_Batch_Name=context['dag'].dag_id, v_project_id=DATA_PROJECT_ID,
                                              v_dataset_name=DATASET_NAME)
    print("Query for post_batch_fail" + query_post_batch_fail)
    bq_client = bigquery.Client(project=DATA_PROJECT_ID)
    bq_client.query(query_post_batch_fail).result()


dag = models.DAG(
    f"fusion_finance_analytics_gl_soft_del",
    # schedule_interval='0 1/3 * * *',
    schedule_interval=None,
    start_date=datetime(year=2022, month=11, day=7, hour=0, minute=0, second=0),
    catchup=False,
    tags=['fusion_finance_analytics'],
)


def getLastRunTimeForBipFiles(column_name, where_clause):
    from google.cloud import bigquery
    client = bigquery.Client(project=DATA_PROJECT_ID)
    job = client.query('select {column_name} from {project_id}.{dataset_id}.{table_name} {where_clause}'.format(
        column_name=column_name, project_id=DATA_PROJECT_ID, dataset_id=DATASET_NAME, table_name=CONTROL_LOGIN,
        where_clause=where_clause))
    for row in job.result():
        return (row[0])


FileExtract = DataflowStartFlexTemplateOperator(
    task_id=f"fusion_to_gcs_file_transfer",
    body={
        "launchParameter": {
            "containerSpecGcsPath": "gs://"+flex_template_bucket+"/templates/fusiontogcp.json",
            "jobName": f"fusion-gcp-dataflow-{datetime.today().strftime('%Y%m%d-%H%M%S')}-123",
            "environment": {
                "stagingLocation": f"gs://" + DATA_BUCKET + "/staging",
                "tempLocation": f"gs://" + DATA_BUCKET + "/temp",
                "subnetwork": SUBNETWORK
            },
            "parameters": {
                "projectId": PLATFORM_PROJECT_ID,
                "bucketName": DATA_BUCKET,
                "inboundFolderPath": INBOUND_FOLDER_PATH,
                "processedFolderPath": PROCESSED_FOLDER_PATH,
                "jsonToCsvFolderPath": INBOUND_FOLDER_PATH + "/metadata_json",
                "metadataToCsvFolderPath": INBOUND_FOLDER_PATH + "/metadata_table",
                "controlSchduleTableName": DATA_PROJECT_ID + "." + DATASET_NAME + "." + CONTROL_LOGIN,
                "soapEndPointUrl": SOAP_END_POINT_URL,
                "soapAction": SOAP_ACTION,
                "soapEnv": SOAP_ENV,
                "soapUcm": SOAP_UCM,
                "username": USERNAME,
                "secret": SECRET,
                "bip_files": BIP_FILES,
                "bicc_schedule_names": bicc_schedule_names,
                "currentTime": datetime.now(timezone.utc).strftime("""%#m/%#d/%y %#I:%#M:%#S %p"""),
                "bip_lastRunTime": getLastRunTimeForBipFiles("LAST_RUN_TIME", "where IS_ACTIVE=True LIMIT 1"),
                "bip_dag_run_id": getLastRunTimeForBipFiles("DAG_RUN_ID", "where IS_ACTIVE=True LIMIT 1")
            }
        }
    },
    do_xcom_push=True,
    location='us-west1',
    wait_until_finished=True,
    project_id=PLATFORM_PROJECT_ID,
    dag=dag
)

step1 = GCSToBigQueryOperator(
    task_id='Loading_BICC_UCM_FILE_EXTRACT',
    bucket=DATA_BUCKET,
    source_objects=[INBOUND_FOLDER_PATH + '/metadata_table/*'],
    schema_fields=None,
    schema_object=FUSION_SCHEMA + '/BICC_UCM_FILE_EXTRACT.json',
    destination_project_dataset_table=DATA_PROJECT_ID + "." + BQ_DM_DATASET_NAME + '.BICC_UCM_FILE_EXTRACT',
    source_format='CSV',
    create_disposition='CREATE_IF_NEEDED',
    skip_leading_rows=1,
    autodetect=False,
    write_disposition='WRITE_APPEND',
    allow_quoted_newlines=True,
    retries=0,
    on_failure_callback=task_failure,
    on_success_callback=task_success
    , dag=dag
)

step2 = GCSToBigQueryOperator(
    task_id='Loading_BICC_UCM_JSON_DETAILS',
    bucket=DATA_BUCKET,
    source_objects=[INBOUND_FOLDER_PATH + '/metadata_json/*'],
    schema_fields=None,
    schema_object=FUSION_SCHEMA + '/BICC_UCM_JSON_DETAILS.json',
    destination_project_dataset_table=DATA_PROJECT_ID + "." + BQ_DM_DATASET_NAME + '.BICC_UCM_JSON_DETAILS',
    source_format='CSV',
    create_disposition='CREATE_IF_NEEDED',
    skip_leading_rows=1,
    autodetect=False,
    write_disposition='WRITE_APPEND',
    allow_quoted_newlines=True,
    retries=0,
    on_failure_callback=task_failure,
    on_success_callback=task_success,
    dag=dag
)

PRE_BATCH_STEP = PythonOperator(
    task_id="PRE_BATCH_STEP",
    python_callable=pre_batch,
    provide_context=True,
    retries=0,
    dag=dag)

POST_BATCH_STEP = PythonOperator(
    task_id="POST_BATCH_STEP",
    python_callable=post_batch,
    provide_context=True,
    trigger_rule='all_success',
    on_success_callback=task_successemail_notification,
    retries=0,
    dag=dag)

STG_COMPLETION_CHECK = DummyOperator(
    task_id="STG_COMPLETION_CHECK",
    trigger_rule='all_success',
    on_failure_callback=task_failure,
    on_success_callback=task_success,
    dag=dag)

POST_BATCH_FAIL_STEP = PythonOperator(
    task_id="POST_BATCH_FAIL_STEP",
    python_callable=post_batch_fail,
    provide_context=True,
    trigger_rule='one_failed',
    on_success_callback=task_failemail_notification,
    retries=0,
    dag=dag)

move_files_to_archive = GCSToGCSOperator(
    task_id="INBOUNDBQ_TO_ARCHIVE",
    source_bucket=DATA_BUCKET,
    source_objects=[INBOUND_FOLDER_PATH+"/*"],
    destination_bucket=DATA_BUCKET,
    destination_object=archive_path+"/",
    move_object=True,
    dag=dag
)

def create_dynamic_tasks():
    client = bigquery.Client(DATA_PROJECT_ID)
    sql = 'select table_id, datastore_name, stg_table_name, hist_table_name, ' \
          'STG_DEL_TABLE_NAME, RAW_DEL_PLP_PROC, raw_sil_proc ' \
          'from {}.{}.FUSION_TO_BQ_PVO_DTL ' \
          'WHERE MODULE=\'GL\' ORDER BY TABLE_ID'.format(DATA_PROJECT_ID, DATASET_NAME)

    output_df = client.query(sql).to_dataframe()
    output_df['file_name'] = output_df['datastore_name'].str.lower()
    output_df['file_name'] = 'file_' + output_df['file_name'].str.replace('.', '_', regex=False)
    output_dict = output_df.to_dict('records')

    for a in output_dict:
        stg_table_load = GCSToBigQueryOperator(
            task_id='fusion_bq_stg_del_' + a['table_id'],
            bucket=DATA_BUCKET,
            source_objects=[
                INBOUND_FOLDER_PATH + '/' + a['file_name'] + '*.pecsv'],
            schema_fields=None,
            schema_object=FUSION_SCHEMA + '/' + a['STG_DEL_TABLE_NAME'] + '.json',
            destination_project_dataset_table=DATA_PROJECT_ID + "." + BQ_STG_DATASET_NAME + '.' + a['STG_DEL_TABLE_NAME'],
            source_format='CSV',
            create_disposition='CREATE_IF_NEEDED',
            skip_leading_rows=1,
            autodetect=False,
            write_disposition='WRITE_TRUNCATE',
            allow_quoted_newlines=True,
            retries=0,
            on_failure_callback=task_failure,
            on_success_callback=task_success,
            dag=dag
        )


        raw_del_proc_exec = BigQueryExecuteQueryOperator(
            task_id="raw_soft_del_" + a['table_id'],
            sql="CALL `" + DATA_PROJECT_ID + ".helix_raw." + a['RAW_DEL_PLP_PROC'] + "`()",
            use_legacy_sql=False,
            on_failure_callback=task_failure,
            on_success_callback=task_success,
            retries=0,
            dag=dag
        )

        step2.set_downstream(stg_table_load)
        stg_table_load.set_downstream(raw_del_proc_exec)
        raw_del_proc_exec.set_downstream(STG_COMPLETION_CHECK)


PRE_BATCH_STEP >> FileExtract >> step1 >> step2
create_dynamic_tasks()
STG_COMPLETION_CHECK >> move_files_to_archive
move_files_to_archive >> POST_BATCH_FAIL_STEP
move_files_to_archive >> POST_BATCH_STEP

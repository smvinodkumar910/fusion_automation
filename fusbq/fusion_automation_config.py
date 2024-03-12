from airflow.decorators import task
from airflow import DAG
from airflow.models.param import Param, DagParam
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

#import fusbq.base_file_template_fn as basepy 
#import fusbq.ingestion_tasks_creation as itc
from fusbq.path_config import DataSets
pc = DataSets('common')


project_name = pc.DATA_PROJECT_NAME
meta_dataset_name = pc.META_DATASET_NAME
base_pvo_path = pc.BASE_PVOS_PATH


def derive_where_clause_dag(command) -> str:
    options = command.split()
    if options[1]=='--module':
        module = options[2]
        tables = None
    elif options[1]=='--tables':
        module = None
        tables = options[2]

    task_type=options[0]
    
    if task_type in ['data_tasks_json', 'del_tasks_json']:
        if module is None:
            tables = '","'.join(tables.split(','))
            tables = '"' + tables + '"'
            where_clause = ' where TABLE_ID in ({})'.format(tables)
        else:
            where_clause = ' where MODULE = "{}" '.format(module)

    if task_type in ['tranformation_tasks_json']:
        if module is None:
            tables = '","'.join(tables.split(','))
            tables = '"' + tables + '"'
            where_clause = ' where TABLE_ID in ({}) and DW_TABLE_NAME is not null'.format(tables)
        else:
            where_clause = ' where MODULE = "{}" and DW_TABLE_NAME is not null'.format(module)
    
    return  where_clause



def process_insert(task_type):
    from fusbq.base_file_template_fn import check_pvo_tb_exists, load_data_to_bq
    if task_type == 'insert':
        table_id = '{}.{}.{}'.format(project_name, meta_dataset_name, 'FUSION_TO_BQ_PVO_DTL')
        can_proceed, error_msg = check_pvo_tb_exists(project_name,base_pvo_path,task_type)
        if can_proceed:
            load_data_to_bq(project_name, table_id, base_pvo_path)
        else:
            raise Exception(error_msg)
    elif task_type=='insertbip':
        table_id = '{}.{}.{}'.format(project_name, meta_dataset_name, 'FUSION_TO_BQ_PVO_DTL')
        can_proceed, error_msg = check_pvo_tb_exists(project_name,pc.BASE_BIP_PATH,task_type)
        if can_proceed:
            load_data_to_bq(project_name, table_id, pc.BASE_BIP_PATH)
        else:
            raise Exception(error_msg)

doc_md_DAG = """
### fusion_automation_config DAG
### params options:

This DAG has to be triggered with any one of the below configs, as below:
**{
"command":"<*your_command*>"
}**

*DDL Config commands:*
1. **generate** -- Creates BASE_PVOs.csv file which contains details to be inserted in the table FUSION_TO_BQ_PVO_DTL
2. **generatebip** -- Creates BASE_BIP.csv file which contains details to be inserted in the table FUSION_TO_BQ_PVO_DTL
3. **insert** -- inserts BASE_PVOs.csv file into the table FUSION_TO_BQ_PVO_DTL
4. **insertbip** -- inserts BASE_BIP.csv file into the table FUSION_TO_BQ_PVO_DTL

*JSON Config Commands:*
1. **data_tasks_json** - creates json file contains list of data load tasks (upto RAW layer)
2. **del_tasks_json** - creates json file contains list of soft delete tasks (upto RAW layer)
3. **tranformation_tasks_json** - creates json file contains transofrmation tasks (raw to DW layer)

JSON Config commands should be associated with **module** or **tables** options as below:

data_tasks_json --module GL
del_tasks_json --module GL
tranformation_tasks_json --module AR

data_tasks_json --tables GL_TEST01,GL_TEST02
del_tasks_json --tables GL_TEST01,GL_TEST02
tranformation_tasks_json --tables GL_TEST01,GL_TEST02

**Example :** { "command":"data_tasks_json --tables GL_TEST01,GL_TEST02"}

"""

with DAG(
        "fusion_automation_config",
        params={
         "command": Param("generate", type="string")
        },
        schedule_interval=None,
        doc_md = doc_md_DAG,
        start_date=days_ago(1)
) as dag:
    
    @task.branch(task_id="check_command")
    def check_command(**context):
        command = context["params"]["command"]
        if (str(command).lower() == 'generate' or str(command).lower() == 'generatebip'):
            return "ddl_config_group.generate_base_file"
        elif (str(command).lower() == 'insert' or str(command).lower() == 'insertbip'):
            return "ddl_config_group.direct_insert"
        elif str(command).lower().startswith('data_tasks_json'):
            return "json_config_group.data_tasks_json"
        elif str(command).lower().startswith('del_tasks_json'):
            return "json_config_group.del_tasks_json"
        elif str(command).lower().startswith('tranformation_tasks_json'):
            return "json_config_group.tranformation_tasks_json"
        else:
            raise Exception('Invalid command')
    
    @task(task_id='generate_base_file')
    def generate_base_file(**context):
        from fusbq.base_file_template_fn import bicc_main, bip_main
        task_type = context["params"]["command"]
        if task_type=='generate':
            bicc_main()
        elif task_type=='generatebip':
            bip_main()
        
    @task(task_id='insert_base_file')
    def insert_base_file_fn(**context):
        task_type = context["params"]["command"]
        if task_type=='generate':
            process_insert('insert')
        elif task_type=='generatebip':
            process_insert('insertbip')


    @task(task_id='direct_insert')
    def direct_insert(**context):
        task_type = context["params"]["command"]
        process_insert(task_type)
    
    @task(task_id='data_tasks_json')
    def data_tasks_json(**context):
        from fusbq.ingestion_tasks_creation import create_task_list
        command = context["params"]["command"]
        print("insdie task: "+command)
        options = command.split()
        task_type = options[0]
        create_task_list(task_type, derive_where_clause_dag(command))
    
    @task(task_id='del_tasks_json')
    def del_tasks_json(**context):
        from fusbq.ingestion_tasks_creation import create_task_list
        command = context["params"]["command"]
        options = command.split()
        task_type = options[0]
        create_task_list(task_type, derive_where_clause_dag(command))
    
    @task(task_id='tranformation_tasks_json')
    def tranformation_tasks_json(**context):
        from fusbq.ingestion_tasks_creation import create_task_list
        command = context["params"]["command"]
        options = command.split()
        task_type = options[0]
        create_task_list(task_type, derive_where_clause_dag(command))


    CHECK_COMMAND = check_command()

    with TaskGroup(group_id='ddl_config_group') as grp1:
        GENERATE_BASE_FILE = generate_base_file()
        INSERT_BASE_FILE = insert_base_file_fn()
        DIRECT_INSERT = direct_insert()

        ddl_automation_dag_trigger = TriggerDagRunOperator(
        task_id=f'ddl_automation_dag_trigger',
        trigger_dag_id="fusion_automation",
        wait_for_completion=False,
        wait_for_downstream=True,
        dag=dag
        )

        join_task = EmptyOperator(
            task_id="join",
            trigger_rule="none_failed_min_one_success"
        )

    with TaskGroup(group_id='json_config_group') as grp1:
        DATA_TASKS_JSON = data_tasks_json()
        DEL_TASKS_JSON = del_tasks_json()
        TRANSFORM_TASKS_JSON = tranformation_tasks_json()
    

CHECK_COMMAND >> [GENERATE_BASE_FILE, DIRECT_INSERT, DATA_TASKS_JSON, DEL_TASKS_JSON, TRANSFORM_TASKS_JSON]
GENERATE_BASE_FILE >> INSERT_BASE_FILE >> join_task
DIRECT_INSERT >> join_task
join_task >> ddl_automation_dag_trigger
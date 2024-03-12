
# Fusion Automation Documentation 

This file describes functionality of the automation code and steps involved in utilizing this code to load fusion tables to Bigquery.


## Control Tables in Use

1. BQ_CTL_METADATA.BQ_CTL_BATCH_MASTER #Stores the BATCH/DAG related details, last_run_time, batch_name, last_run dag_id
2. BQ_CTL_METADATA.BQ_CTL_PROC_LOGGING  #stores BICC Scheudle related information, updated with LAST_UPDATE_DATE &
                                        #latest DAG RUN_ID This table get updates thru dataflow job, and a direct update statement in DAG.
3. BQ_CTL_METADATA.BQ_CTL_BATCH_RUN #stores status each run of the DAG updated with the procedures BQ_PRE_BATCH_CTL_PROC_LOGGING
4. BQ_CTL_METADATA.BQ_CTL_TASK_LOG #stores details related to each task in the batch.
5. BQ_CTL_METADATA.FUSION_TO_BQ_PVO_DTL #Contais the list of all PVO objects/Tables/PROCs details. RUN_DDL_FLAG column in this table is updated by fusion_automation.py dag. 

Meta-control Tables update Procs in Use:
-----------------------------------------
1. BQ_CTL_METADATA.BQ_CTL_PROC_LOGGING -- updates the table BQ_CTL_TASK_LOG (task level success or failure)
2. BQ_CTL_METADATA.BQ_PRE_BATCH_CTL_PROC_LOGGING -- updates the tables BQ_CTL_BATCH_RUN & BQ_CTL_BATCH_MASTER (batch level update before run)
3. BQ_CTL_METADATA.BQ_POST_BATCH_CTL_PROC_LOGGING --updates the tables BQ_CTL_BATCH_RUN & BQ_CTL_BATCH_MASTER (batch level update after run)
4. BQ_CTL_METADATA.BQ_POST_BATCH_FAIL_CTL_PROC_LOGGING -- updates the tables BQ_CTL_BATCH_RUN & BQ_CTL_BATCH_MASTER (batch level update after run status as fail)

Steps to sync a new Set of Tables from Fusion to BigQuery: 
----------------------------------------------------------

BICC_Side:
-----------
1. Create offerings, Job, Schedule and run it
2. get Schedule_id, job_id, schedule_name -- to put entry in BQ Table BQ_CTL_METADATA.BQ_CTL_PROC_LOGGING



GCP_Side:
---------
1. Create list of PVOs.
2. Get corresponding source_tables using script from BICC file. (generate_base_file.py)
3. Create the BASE_PVOs.csv file.
4. Upload it to GCS Bucket gs://helix-finance-test/helix_automation_resource/
5. Run generate_base_file.py script to load the BASE_PVO.csv to the bq table it-helix-platform-dev.CTL_METADATA.FUSION_TO_BQ_PVO_DTL
6. Run bq_ddl_automate DAG in composer -- this will create stg_tables, raw_tables, raw_sil_scripts.

7. Put entry in helix-data-dev.CTL_METADATA.BQ_CTL_BATCH_MASTER and helix-data-dev.CTL_METADATA.BQ_CTL_PROC_LOGGING using scheudle_id, job_id, scheudle_name details got from BICC
8. Create the main DAG for the Pipeline
9. Run the pipeline

# PHASE 1 Changes

# UAT : -- DONE
1. Deploy Composer DAG to UAT 
   (update statement for BQ_CTL_METADATA.BQ_CTL_PROC_LOGGING table & bicc_schedule_names config file change)

# PROD :
1. Deploy Dataflow code changes 
2. Deploy Composer DAG
   (update statement for BQ_CTL_METADATA.BQ_CTL_PROC_LOGGING table & bicc_schedule_names config file change)



# BRING_ALL_TABLES 

1. create a new GCS Bucket : helix-finance-uat in helix-platform-uat -- DONE
2. create a new GCS Bucket : helix-finance-prod in helix-platform-prod  --- DONE
3. Upload BICC Excel file to helix_automation_resource folder UAT bucket. -- IN PROGRESS
4. Ensure secret manager is having the BICC credentials. if not has to be added -- IN PROGRESS
5. Upload BICC Excel file to helix_automation_resource folder in PROD bucket. -- Have to create RITM


# PROD:
1. In prod BICC -- create schedule and run it.
    confirm with Saravana on last_extract_date in job_level.

2. Create a new branch from Prod, and migrate the below files :
    Bigquery deployment:
     a. tables > oracle-fusion-bq-ctl-batch-master_fusion
                 oracle-fusion-bq-ctl-proc-logging
        gcs_files > fusion_schema/BICC_UCM_FILE_EXTRACT.json
                    fusion_schema/BICC_UCM_JSON_DETAILS.json
    
    Composer deployment :
    a. fusbq/fusion_finance_analytics_gl_prod_to_uat.py
    b. fusbq/path_config.py
    c. fusbq/config/fusion_to_bq_sync_config.json

3. migrate these files to Master branch and run deployment wf.

# UAT:

1. Create a new branch from UAT.
2. deploy Bigquery objects --
    bigquery/tables/oracle-fusion-bq-ctl-batch-master_fusion.sql
    bigquery/tables/oracle-fusion-to-bq-pvo-dtl.sql
    bigquery/stored_procedures/oralce-fusion-stg-hist-load-proc.sql
    gcs_files > fusion_schema/BICC_UCM_FILE_EXTRACT.json
                fusion_schema/BICC_UCM_JSON_DETAILS.json

3. deploy composer objects --
   fusbq/config/fusion_to_bq_sync_config.json
   fusbq/bq_ddl_automate
   fusbq/fusion_finance_analytics_gl_without_java.py
   fusbq/fusion_finance_analytics_gl_without_java.py
   fusbq/path_config.py

4. migrate these files to Master branch and run deployment wf.

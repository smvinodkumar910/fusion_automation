INSERT INTO helix-data-dev.BQ_CTL_METADATA.CTL_BQ_EXA_SYNC_TASK_LIST
(BATCH_NAME,
TASK_NAME,
TABLE_TYPE,
BQ_PROJECT_ID,
BQ_DATASET_ID,
BQ_TABLE_NAME,
EXA_STG_SCHEMA_NAME,
EXA_STG_TABLE_NAME,
EXA_DM_SCHEMA_NAME,
EXA_DM_TABLE_NAME,
ALWAYS_FULL_LOAD_FLAG,
IS_SNAPSHOT,
--SNAPSHOT_PARTITION_FIELD
PRIMARY_KEY_IDENTIFIER,
INCREMENTAL_DATE_IDENTIFIER,
SKIP_TASK,
PREVIOUS_RUN_EXTRACT_DT
CURRENT_RUN_EXTRACT_DT
NEXT_RUN_EXTRACT_DT
--CUSTOM_LAST_EXTRACT_DATE
FORCE_CUSTOM_DT_FLG,
CUSTOM_FILTER_CONDITION,
--CUSTOM_SNAPSHOT_LIST
TABLE_OWNER,
CREATE_DT,
UPDATE_DT,
--SCH_SYNC_REQUIRED
FULL_DATA_SYNC_FLAG,
--FULL_COLUMN_SYNC
--COLUMN_LIST
NOTIFICATION_EMAIL_ADDRESS
)
VALUES
(
'AR_BQ_TO_EXASOL_SYNC',
'bq_exa_sync_helix-data-dev_helix_dw_dw_invoice_header',
'REGULAR',
'helix-data-dev',
'helix_dw',
'DW_INVOICE_HEADER',
'EXA_STG',
'STG_FUSION_INVOICE_HEADER',
'EXA_DM',
'DM_FUSION_INVOICE_HEADER',
'N',
'N',
'INTEGRATION_ID,DATA_SOURCE_NUM_ID',
'TGT_UPDATE_DT',
'N',
DATETIME '2000-01-01 00:00:00',
DATETIME '2000-01-01 00:00:00',
DATETIME '2000-01-01 00:00:00',
'N',
'DATA_SOURCE_NUM_ID = 34',
'VMADHAVAN',
CURRENT_DATE,
CURRENT_DATE,
'N',
'vmadhavan@equinix.com'
);





select * from
helix-data-uat.BQ_CTL_METADATA.BQ_CTL_BATCH_RUN-- set batch_status='COMPLETED'
                    WHERE
                        batch_name like  '%AR_BQ_TO_EXASOL_SYNC%'
                        ---and batch_run_id=-2024031107592145
                        order by tgt_update_dt desc



select * from
helix-data-uat.BQ_CTL_METADATA.BQ_CTL_BATCH_RUN-- set batch_status='COMPLETED'
                    WHERE
                        batch_name like  '%MS_BQ_TO_EXASOL_SYNC%'
                        and batch_run_id=-2024031109050500
                        order by tgt_update_dt desc 

SELECT* FROM`helix-data-uat.BQ_CTL_LOGGING.GCP_JOB_DETAIL_LOG` where 1=1 AND ETL_PROC_WID IN (2024021103033339)
and log_level = 'ERROR' LIMIT 1000;



SELECT
                        COUNT(CASE WHEN batch_status = 'IN_PROGRESS' THEN 1 ELSE NULL END ) AS INP_COUNT,
                        COUNT(CASE WHEN batch_status = 'IN_PROGRESS' AND batch_run_id <> 2024021409000000 THEN 1 ELSE NULL END ) AS INP_COUNT_EX_TID,
                        MAX(CASE WHEN batch_run_id = -2024021409000000 THEN batch_status ELSE 'A' END ) AS BATCH_STATUS,
                        MAX(CASE WHEN batch_run_id <> -2024021409000000 THEN batch_status ELSE 'A' END ) AS BATCH_STATUS_EX_TID,
                        (SELECT COUNT(DISTINCT TASK_NAME) FROM helix-data-uat.BQ_CTL_METADATA.CTL_BQ_EXA_SYNC_TASK_LIST WHERE BATCH_NAME = 'MS_BQ_TO_EXASOL_SYNC' AND SKIP_TASK = 'N') AS TASK_COUNT
                    FROM (
                    (SELECT
                        batch_name,
                        (CASE WHEN batch_status = 'COMPLETED' THEN 'SUCCESS' WHEN batch_status = 'INPROGRESS' THEN 'IN_PROGRESS' ELSE batch_status END) AS batch_status,
                        batch_run_id 
                    FROM
                    helix-data-uat.BQ_CTL_METADATA.BQ_CTL_BATCH_RUN
                    WHERE
                        batch_name = 'EXA_BQ_SYNC_MS_BQ_TO_EXASOL_SYNC'
                        ORDER BY tgt_update_dt desc
                    LIMIT 1)
                    UNION ALL 
                    (SELECT
                        batch_name,
                        (CASE WHEN batch_status = 'COMPLETED' THEN 'SUCCESS' WHEN batch_status = 'INPROGRESS' THEN 'IN_PROGRESS' ELSE batch_status END) AS batch_status,
                        batch_run_id 
                    FROM
                        helix-data-uat.BQ_CTL_METADATA.BQ_CTL_BATCH_RUN
                    WHERE
                        batch_name = 'EXA_BQ_SYNC_MS_BQ_TO_EXASOL_SYNC'
                        and batch_run_id = 2024021409000000
                    ORDER BY tgt_update_dt desc
                    LIMIT 1
                    )
                    UNION ALL 
                    (SELECT
                        'A' AS batch_name,
                        'A' AS batch_status,
                        -1 AS batch_run_id 
                    )
                    );





"batch_name": "MS_BQ_TO_EXASOL_SYNC",
"etl_proc_wid": "2024031101592637",
"ignore_previous_failures":true



"batch_name": "AR_BQ_TO_EXASOL_SYNC",
"etl_proc_wid": "2024031103593552",
"ignore_previous_failures":true
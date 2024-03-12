

update `helix-data-dev.CTL_METADATA.BQ_CTL_PROC_LOGGING` set IS_ACTIVE=false where IS_ACTIVE=true;

insert into `helix-data-dev.BQ_CTL_METADATA.BQ_CTL_PROC_LOGGING`
(SCHEDULE_ID, SCHEDULE_NAME, JOB_ID, IS_ACTIVE, LAST_RUN_TIME)
VALUES
('836702','EXA_FA_SH_19122022','908026116261662', true, '12/14/22 00:00:00 AM');

insert into `helix-data-sit.BQ_CTL_METADATA.BQ_CTL_PROC_LOGGING`
(SCHEDULE_ID, SCHEDULE_NAME, JOB_ID, IS_ACTIVE, LAST_RUN_TIME)
VALUES
('1983144','EQX_GL_ALL_PK_EXTRACT_SCHEDULE','179857426051923',true,'04/12/23 01:00:00 AM');


INSERT INTO `helix-data-dev.BQ_CTL_METADATA.BQ_CTL_BATCH_MASTER`
(batch_name, batch_description, tgt_create_dt, tgt_update_dt, last_batch_run_id, last_extract_dt, prune_minutes, schedule_interval_minutes)
values('fusion_finance_analytics_fa','Extracts Fixed Asset file from Fusion to BQ', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1,CURRENT_TIMESTAMP , 0, 0);



Analysis queries:


select * from helix-data-sit.BQ_DM.BICC_UCM_FILE_EXTRACT;


select NAME, ROWCOUNT from helix-data-sit.BQ_DM.BICC_UCM_JSON_DETAILS;

select * from helix-data-sit.CTL_METADATA.BQ_CTL_PROC_LOGGING;


select table_id, row_count, size_bytes from helix-data-sit.helix_raw.__TABLES__
where table_id in (SELECT RAW_TABLE_NAME FROM `helix-data-sit.CTL_METADATA.FUSION_TO_BQ_PVO_DTL`
WHERE MODULE='GL')
order by row_count desc


SELECT * FROM `helix-data-dev.helix_orchestration_framework.task_details`
where dag_id in(
"fusion_sql_test")

SELECT * FROM `helix-data-dev.helix_orchestration_framework.adhoc_load_config`
INSERT INTO `helix-data-dev.helix_orchestration_framework.adhoc_load_config`
VALUES('ADHOC_SQL_TEST', 'fusion_sql_test', 'AR_046_DW_PROC_EXEC', true, '2023-05-15 00:00:00', '2023-05-25 00:00:00', 'Updated by vmadhavan to test adhoc load with multi line sql', 'vmadhavan', current_timestamp, 'NOT EXECUTED' );


INSERT INTO `helix-data-dev.helix_orchestration_framework.adhoc_load_config`
VALUES('ADHOC_SQL_TEST', 'Account_Receivable_Fusion_Finance_Analytics', 'UCM_TO_GCS', true, '2023-06-14 00:00:00', '2023-06-15 00:00:00', 'Updated by vmadhavan to Load data', 'vmadhavan', current_timestamp, 'NOT EXECUTED' );

select * from `helix-data-dev.BQ_CTL_METADATA.BQ_CTL_BATCH_RUN` WHERE batch_name LIKE '%AR_BQ_TO_EXASOL_SYNC%';



merge into helix-data-dev.BQ_CTL_METADATA.FUSION_TO_BQ_PVO_DTL tgt 
using
(select table_id, source_table_name,SOURCE_TYPE, 'MS_BIP_'||LPAD(cast(row_num as string),3,'000') as new_table_id from
(select table_id,source_table_name,SOURCE_TYPE,row_number() over(order by table_id) as row_num from helix-data-dev.BQ_CTL_METADATA.FUSION_TO_BQ_PVO_DTL a
where module='MS'
and source_type='BIP'
)
)src on tgt.source_table_name=src.source_table_name AND TGT.SOURCE_TYPE=SRC.SOURCE_TYPE
when matched then update set 
tgt.table_id=src.new_table_id


select dev.table_name, dev.column_name, sit.table_name, sit.column_name from
(select table_name, column_name from helix-data-dev.helix_fin_ms_dw.INFORMATION_SCHEMA.COLUMNS)dev
full outer join
(select table_name, column_name from helix-data-sit.helix_fin_ms_dw.INFORMATION_SCHEMA.COLUMNS) sit 
on dev.table_name = sit.table_name and dev.column_name=sit.column_name




update helix-data-dev.BQ_CTL_METADATA.FUSION_TO_BQ_PVO_DTL set PRIMARY_KEY='CODE_ASSIGNMENT_ID',INCREMENTAL_KEY='LAST_UPDATE_DATE'
WHERE PRIMARY_KEY is null
AND source_table_name='HZ_CODE_ASSIGNMENTS';
update helix-data-dev.BQ_CTL_METADATA.FUSION_TO_BQ_PVO_DTL set PRIMARY_KEY='TAX_ACCOUNT_ID',INCREMENTAL_KEY='TAX_ACCOUNT_ID'
WHERE PRIMARY_KEY is null
AND source_table_name='ZX_ACCOUNTS';
update helix-data-dev.BQ_CTL_METADATA.FUSION_TO_BQ_PVO_DTL set PRIMARY_KEY='LINES_DET_FACTOR_ID',INCREMENTAL_KEY='LAST_UPDATE_DATE'
WHERE PRIMARY_KEY is null
AND source_table_name='ZX_LINES_DET_FACTORS';
update helix-data-dev.BQ_CTL_METADATA.FUSION_TO_BQ_PVO_DTL set PRIMARY_KEY='PARTY_TAX_PROFILE_ID',INCREMENTAL_KEY='PARTY_TAX_PROFILE_ID'
WHERE PRIMARY_KEY is null
AND source_table_name='ZX_PARTY_TAX_PROFILE';
update helix-data-dev.BQ_CTL_METADATA.FUSION_TO_BQ_PVO_DTL set PRIMARY_KEY='REC_NREC_TAX_DIST_ID',INCREMENTAL_KEY='REC_NREC_TAX_DIST_ID'
WHERE PRIMARY_KEY is null
AND source_table_name='ZX_REC_NREC_DIST';




select 'select \''||table_name||'\', count(1) from '||table_catalog||'.'||table_schema||'.'||table_name||' where data_source_num_id=34;' ,table_catalog, table_schema, table_name from helix-data-sit.helix_fin_ms_dw.INFORMATION_SCHEMA.TABLES

DECLARE p_from_delta STRING DEFAULT NULL;
DECLARE p_to_delta STRING DEFAULT NULL;
DECLARE p_bq_job_id STRING DEFAULT NULL;
DECLARE p_job_status STRING DEFAULT NULL;
DECLARE p_job_message STRING DEFAULT NULL;

CALL `helix-data-dev.helix_dw.PRC_SIL_DW_INVOICE_HEADER` ('10001', '2023-05-23 11:22:52 UTC', '2023-05-24 11:55:16 UTC', p_bq_job_id, p_job_status, p_job_message);

select p_bq_job_id,p_job_status,p_job_message;

CREATE OR REPLACE PROCEDURE `helix-data-dev.helix_stg.test_proc`(IN p_params STRING, IN p_from_delta STRING, IN p_to_delta STRING, OUT p_bq_job_id STRING, OUT p_job_status STRING, OUT p_job_message STRING)
begin

UPDATE  `helix-data-dev.BQ_CTL_METADATA.FUSION_TO_BQ_PVO_DTL`  SET RUN_DDL_FLAG=True
WHERE MODULE='AR';

SET p_bq_job_id = @@last_job_id;
SET p_job_status  = "SUCCESS";
SET p_job_message = "";

EXCEPTION WHEN ERROR THEN
  SET p_bq_job_id = @@script.job_id;
  SET p_job_status  = "FAILED";
  SET p_job_message = (SELECT @@error.message);

END

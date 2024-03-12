

Secret Manager:
---------------
1. Raise RITM to create entry in secret manager in project helix-platform-sit.


GCS :
----
1. Create GCS Bucket - helix-finance-sit
2. Upload fusion_schema and helix_automation_resources folders.


DataFlow:
---------
1. Build flex template with latest code and store template in the path gs://eqx-helix-fusion-sit/templates/fusiontogcp.json 
   (already existing - will be replaced after creating new template)
   
  
Composer :
----------

1. Upload the fusion_to_bq_sync package -- 
2. inside config folder -- keep sit file and rename it.


BQ :
-----
1. Create helix-data-sit.CTL_METADATA.FUSION_TO_BQ_PVO_DTL table.
2. put entries in all meta tables -> CTL_METADATA.BQ_CTL_BATCH_MASTER  CTL_METADATA.BQ_CTL_PROC_LOGGING


Creating Flex Templates :
-------------------------

1. mvn clean package
2. export TEMPLATE_PATH="gs://eqx-helix-fusion-sit/templates/fusiontogcp_15022023.json"
3. export TEMPLATE_IMAGE="gcr.io/helix-platform-sit/fusiontogcp/fusion_to_gcs_template_15022023:latest"

4. configure docker : gcloud auth configure-docker
5. Create template :
    gcloud dataflow flex-template build $TEMPLATE_PATH \
          --image-gcr-path "$TEMPLATE_IMAGE" \
          --sdk-language "JAVA" \
          --flex-template-base-image JAVA11 \
          --metadata-file "metadata.json" \
          --jar "target/FusionToGcp-bundled-1.0.0.jar" \
          --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="com.equinix.fusionGcp.flexTemplate.MainClass"
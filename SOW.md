SOW
1. Realtime Data Pipeline 
    1. Ingestion new source to refined
        1. short term
        2. mid term + auth view
        3. long term + auth view
    2. Transformation analytic layer 
2. Data Governance 
    1. Data Quality with dataplex rule 
    2. Data Lineage : enable done

<!-- ------------------------------------------------------------------------------------------------------- -->
<!-- ---------------------------------------------- INGESTION ---------------------------------------------- -->
<!-- ------------------------------------------------------------------------------------------------------- -->
<!-- SHORT TERM -->
Ingestion Data Pipeline Short Term : Batch ingestion new source incremental and reconcile 
    1. Pipeline start with Composer airflow trigger hourly (30 * * * *) 
    2. Composer airflow Dags Run 
        1. ingest full table from original table ( AWS MS_MEMBER ) 
        2. ingest full table from mapping table ( AWS MAPPING_RECONCILE ) 
        3. deploy dataflow pipeline using 
            1. dataflow scripts member 
            2. install lib (from wheel on gcs )in airflow workor for 
            3. using orcastrate module for build task step from config yaml file 
            3. install lib (from Docker image) in dataflow worker 
            4. After Deploy successed. Run Dataflow 
        4. In Dataflow 
            1. ReadBQQuery_mapping_rows : get mapping 
            2. BuildMappingDict_mapping_dict_ToList : convert mapping to list (In Module BuildMappingDictStep)
            3. BuildMappingDict_mapping_dict_BuildDict : convert list mapping to dict (In Module BuildMappingDictStep)
            4. ReadBQQuery_source_rows : get new source personas -2h 
            5. ParseJson_source_rows_Parse : explode personas column (json) 
            6. MapRecord : mapping column from new source to origin column from mapping 
            7. ReadBQQuery_origin_rows : read origin table 
            8. KVPairs_kv_new_KV + KVPairs_kv_old_KV : map key (pk) and value of message from new source and origin 
            9. KVPairs_kv_new_DropNoneKey + KVPairs_kv_old_DropNoneKey : clean pk null 
            10. CoGroupByKey_grouped_CoGroupByKey : group pk from new source personas and map column for full fill origin 
            11. CoalesceByMapping_reconcile_rows_Coalesce : merge 10 to result message ( column reconcile from new source personas and origin column from origin source )
            12. CoalesceByMapping_reconcile_rows_FilterNone : cleaning key None 
            13. NormalizeToSchema_reconcile_casted_Normalize : cast datatype messsage for write to target s3 
            14. WriteParquet_reconcile_casted : write to target s3 
        5. Done 
<!-- MID TERM -->
Ingestion Data Pipeline Mid Term : Realtime ingestion new source incremental and fallback to origin (because next step is point to origin target ) 
    1. Pipeline start with Composer airflow trigger hourly (30 * * * *) 
    2. Composer airflow Dags Run 
        1. deploy dataflow pipeline using 
            1. dataflow scripts member 
            2. install lib (from wheel on gcs )in airflow workor for 
            3. using orcastrate module for build task step from config yaml file 
            3. install lib (from Docker image) in dataflow worker 
            4. After Deploy successed. Run Dataflow 
        2. In Dataflow 
            1. XXXXX : Consume Notification and handle DLQ  
            2. XXXXX : Extract PK  
            3. XXXXX : Get information from Big Table with PK 
            4. ParseJson_source_rows_Parse : explode personas column (json) (from big table)  
            <!-- For Patch To Origin Table AWS MS_MEMBER -->
            4. XXXXX : get mapping with config 
            6. MapRecord : mapping column from new source to origin column from mapping 
            7. CoalesceByMapping_ms_master_rows_FilterNone : cleaning key None 
            8. NormalizeToSchema_ms_master_casted_Normalize : cast datatype messsage for write to target s3 
            9. WriteParquet_ms_master_casted : write to target s3 
            <!-- -------------------------------------- -->
            10. XXXXX : Directed write to Big Query refined : ms_personas
        5. Done 
<!-- LONG TERM -->
Ingestion Data Pipeline Long Term : Realtime ingestion new source incremental (because next step is point to origin target ) 
    1. Pipeline start with Composer airflow trigger hourly (30 * * * *) 
    2. Composer airflow Dags Run 
        1. deploy dataflow pipeline using 
            1. dataflow scripts member 
            2. install lib (from wheel on gcs )in airflow workor for 
            3. using orcastrate module for build task step from config yaml file 
            3. install lib (from Docker image) in dataflow worker 
            4. After Deploy successed. Run Dataflow 
        2. In Dataflow 
            1. XXXXX : Consume Notification and handle DLQ  
            2. XXXXX : Extract PK  
            3. XXXXX : Get information from Big Table with PK 
            5. ParseJson_source_rows_Parse : explode personas column (json) (from big table)  
            6. MapRecord : mapping column from new source to origin column from mapping 
            7. CoalesceByMapping_ms_master_rows_FilterNone : cleaning key None 
            8. NormalizeToSchema_ms_master_casted_Normalize : cast datatype messsage for write to target s3 
            9. WriteParquet_ms_master_casted : write to target s3 
            10. XXXXX : Directed write to Big Query refined : ms_personas
        5. Done 
<!-- -------------------------------------------- DEPLOYMENT ------------------------------------------- -->
<!-- SHORT TERM -->
    1. Deploy composer service 
    2. Deploy scripts all 
    3. init table bq (target) : stg_ms_member , stg_mapping_reconcile , audit_log , audit_data_quality
<!-- MID TERM -->
    1. Deploy scripts : mid term scripts  :
        - dags 
        - config yaml
        - dataflow scripts
        - docker image (enahnce module for realtime)
        - wheel (for airflow worker)
    2. init table bq (target) : ms_personas
    3. init auth view for ms_personas
<!-- LONG TERM -->
    1. Deploy scripts : long term scripts  :
        - dags 
        - config yaml
        - dataflow scripts
        - docker image (enahnce module for realtime)
        - wheel (for airflow worker)

<!-- ------------------------------------------------------------------------------------------------------- -->
<!-- ------------------------------------------------------------------------------------------------------- -->

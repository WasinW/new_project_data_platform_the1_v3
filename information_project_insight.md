Scope ของ โปรเจ็คนี้คือการ ที่ Tech Enterprise ต้องการเป็น center of tech platform 
Data Team เดิมที จะคลายเป็น analytics team ที่ทำหน้าที่ในการ สร้าง data pipeline เพื่อ support business unit ต่างๆ 
โดยจะ ingestion data จาก source system ต่างๆ เข้ามาใน  transform data เพื่อให้เป็น analytic layer 
โดย จะมี data zone  เป็น raw , persistent , refined  และ analytic 

ทีนี้เมื่อ tech enterprise ต้องการเป็น center of tech platform เค้าต้องการให้ tech เองเป็น source of truth
โดยการที่ data team จะต้อง ingestion data จาก tech เองเข้ามาใน zone refined และเอามาทำ analytic layer เอง 
คือ tech จะ provide data source ให้ data team เอง 
(โดยจาก pipeline member. tech จะ provide มาไว้ที่ big query ก่อน ในช่วง short term และจะ provide มาไว้ที่ big table ในช่วง mid term และ long term เพราะบาง source อย่าง member ต้องการเป็น realtime แต่ในช่วงแรก tech provide มาให้ไม่ทัน เลยต้องเอาจาก big query ก่อน)

ทีนี้ในส่วนที่ผมกำลังทำคือ ส่วนที่ก้ำกึ่งกันระหว่าง tech กับ data team คือ 
pipeline ตัวอย่างข้างล่าง โดย 
data pipeline ที่จะ ingestion data จาก tech (big query หรือ big table) มาไว้ที่ zone refined และทำ analytic layer เอง
โดย ingestion pipeline นี้จะมองอยู่อาจจะอยู่ฝั่ง tech ก็ได้ อาจจะเรียกได้ว่าเป็น outbound pipeline จาก tech ก็ได้
และ จะทำ auth view จาก refined outbound อันนี้ให้ data team (เราจะเรียกว่า data hub) ไปทำ analytic layer ต่อเอง อาจจะใช้เป็น dataflow ต่อ หรือ dataform ต่อก็ได้

ซึ่ง ตัวอย่างนี้เป็นเพียงแค่ source เดียวที่ กำลังจะมี ยังมี source อื่นๆ อีกมากมาย ที่จะต้องทำแบบนี้เหมือนกัน
แต่ รูปแบบจะไม่เหมือนตัวนี้ 
บางตัวที่เป็น batch อาจจะง่ายหน่อย โดย tech อาจจะ provide refined มาให้เลย เราทำแค่ auth view ให้ data hub ไปทำ analytic ด้วย dataform ต่อ 
หรือ ถ้าต้อง ingest เพิ่ม อาจจะใช้ dataproc ingest มา ก่อนจะใช้ dataflow transform ต่อก็ได้ 


แต่ที่เล่ามานี่คือที่มาที่ไป ของ scope งานที่ผมทำ
<!-- ------------------------------------------------------------------------------------------------------- -->
Scope of work 
1. Realtime Data Pipeline 
    1. Ingestion new source to refined
        1. short term
        2. mid term + auth view
        3. long term + auth view
    2. Transformation analytic layer 
2. Data Governance 
    1. Data Quality with dataplex rule 
    2. Data Lineage : enable done
3. Step Audit dataflow pipeline 
4. Data Catalog : will be use iceberg table (but refined zone using native table on big query)
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
            2. BuildMappingDict_mapping_dict_ToList : convert mapping to list 
            3. BuildMappingDict_mapping_dict_BuildDict : convert list mapping to dict 
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
**  XXXXX คือยังไม่มีชื่อ module ที่แน่นอน แต่การทำงานประมาณนี้ 
** Audit log 
** สำหรับ batch จะเปิด ตอนเริ่มรัน ก่อน mapping และ จะ count records source และ target write และ log ลง big query table ตอนจบ pipeline 
** สำหรับ realtime จะเปิด ตอนเริ่มรันเลย โดยจะเปิดเป็น windowing ตาม config(default 1 hour ) และ จะ count records source และ target write และ log ลง big query table ก่อนปิด windowing แล้วเปิดใหม่นับใหม่ วนไป 
<!-- DEPLOY INIT -->
1. Create SA 
2. Create GCS bucket 
    1. airflow 
        - dags 
        - config 
        - packages
    2. dataflow 
        - scripts 
        - lib (wheel)
    3. dataproc  <!-- maybe -->
        - scripts 
        - lib (jar)
    4. dataform <!-- maybe -->
        - scripts
    5. dataplex <!-- maybe -->
        - scripts
<!-- ตอนนี้ design bucket กำลังคิดอยู่ว่า part analytic dataproc + dataform อาจจะต้องอยู่แยก project  -->
<!-- ตอนนี้เหมือน ทำงานภายใต้ โปรเจ็ค insight ถ้างั้น 3-4 อาจจะต้องอยู่แยกไป (คำถามคือ ถ้าต้องมีอีก platform ของ data hub ไว้สำหรับทำ analytics จาก source ที่เป็น auth view จาก refined ตรงนี้ คุณมีไอเดีย design scripts path โครงสร้างโปรเจ็คมั้ยครับ และต้องมี audit log + dataplex data quality ด้วย ) อันนี้จะเป็น part Transformation analytic layer   -->
3. Create Composer environment 
4. Create Big Query dataset 
    1. insight 
        - table : audit_log
        - table : stg_ms_member
        - table : stg_mapping_reconcile
        - table : ms_personas (refined)
<!-- ตอนนี้ design dataset ยังไม่ common เลย อาจจะเป็นเพราะ ตอนนี้ ทำงานภายใต้ โปรเจ็ค insight part นี้เลยอยู่ภายใต้ insight -->
<!-- ------------------------------------------------------------------------------------------------------- -->

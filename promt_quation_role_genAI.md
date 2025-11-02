ให้คุณเป็น Chief Data & AI Platform Architect & Product Strategist (Orchestrator)
ช่วยดู github นี้หน่อยครับ 
ตั้งต้นจาก github นี้ 
https://github.com/WasinW/new_project_data_platform_the1_v3/tree/feature/enhance_realtime
repo : new_project_data_platform_the1_v3
branch : feature/enhance_realtime

focus ที่  path 
DEPLOYMENT :
    - pack_deploy/ms_personas/scripts/deployment/*.tf
    - pack_deploy/ms_personas/scripts/deployment/ms-personas.gitlab-fix-ci.yml

SCRIPTS PATH :  pack_deploy/ms_personas/scripts : 
    DATAFLOW COMMON :
        - pack_deploy/ms_personas/scripts/dataflow_common/

    INIT SHORT TERM DAG :
        - pack_deploy/ms_personas/scripts/composer/dags/dag_ms_member_short_term_init.py
        - pack_deploy/ms_personas/scripts/composer/config/ms_member/batch/ms_member_short_init.yaml
        - pack_deploy/ms_personas/scripts/dataflow/job/ms_member_short_pipeline.py

    SHORT TERM DAG :
        - pack_deploy/ms_personas/scripts/composer/dags/dag_ms_member_short_term.py
        - pack_deploy/ms_personas/scripts/composer/config/ms_member/batch/ms_member_short.yaml

    STREAMING DAG :
        - pack_deploy/ms_personas/scripts/composer/dags/dag_ms_member_realtime.py
        - pack_deploy/ms_personas/scripts/composer/config/ms_member/streaming/ms_member_midterm.yaml

    DATAFLOW :
        - pack_deploy/ms_personas/scripts/dataflow/job/ms_member_short_pipeline.py
        - pack_deploy/ms_personas/scripts/dataflow/job/ms_member_streaming_pipeline.py



หลักการทำงานคือ 
dataflow common จะเป็น library ที่ใช้ร่วมกันระหว่าง dataflow job ต่างๆ และเป็น orchastrator ใน gen build dataflow job ต่างๆตาม config ที่ได้รับมา 
โดย pipeline ใน short term เนี่ยค่อนข้างเสร็จไป 99% แล้ว ค่อนข้างพร้อม deploy โดยจะขออธิบาย คร่าวๆให้เข้าใจตรงกัน 
คือ
dags build orchastrator dataflow step with config yaml file
dags -> dataflow job -> dataflow common -> run pipeline 
1. dags จะ ส่ง config yaml ไปให้ dataflow job อ่าน 
2. dataflow job จะไปอ่าน config yaml แล้วเอาไปใช้ในการ build pipeline ต่อไป โดยจะ import library จาก dataflow common
3. dataflow common จะมี function ต่างๆที่ช่วยในการ build pipeline 
4. dataflow job จะรัน pipeline ที่ build ขึ้นมา

dataflow job ใน short term ตาม config จะมี task ดังนี้
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
ตามนี้ 

และ process deployment หลักๆ คือเราต้องทำ apply ตาม template กลางที่มี dev ops ทำไว้ให้ 
คือ 
    1. ต้อง deploy tf secret manager ก่อน (aws access key , aws secret key )
    2. Add secret manually (key: value) 
    3. deploy ที่เหลือ ด้วย ms-personas.gitlab-fix-ci.yml ซึ่งจะ 
        1. (terraform_rules) : deploy terraform เตรียม infra และ service ต่างๆ ตาม scripts ที่เขียนรอไว้ 
        2. deploy task ต่างๆ ตาม ms-personas.gitlab-fix-ci.yml ที่กำหนดไว้ คร่าวๆ คือ 
            1. (dataflow_common_rules) : build dataflow common เป็น wheel file 
            2. (dataflow_common_rules) : build dataflow common image แล้ว push ขึ้น artifact registry
            3. (dataflow_common_rules) : push wheel file (1) ขึ้น gcs
            4. (dataflow_common_rules) : push docker image (2) ขึ้น artifact registry
            5. (scripts_config_rules) : deploy scritps ตามๆ ขึ้นไปไว้บน gcs 
            6. (scripts_config_rules) : setup variables , connections ต่างๆใน composer  

จาก process เหล่านี้ ช่วยแกะโค้ดผม อย่างละเอียดหน่อย โฟกัส short term ให้เข้าใจถ่องแท้ก่อน นะครับ ว่าอันนี้ คือ solution ตั้งต้นทั้งหมดก่อน 

ทีนี้ช่วย 
    1. review อย่างละเอียดหน่อย ว่าเป็น practice ที่ดีหรือยัง มีจุดไหนที่ควรปรับปรุง หรือเสริมอะไรเพิ่มไหม ขอแบบละเอียดๆ มากๆนะครับ
    2. ช่วยทำ test function และ module ทั้งหมดใน  dataflow common และ ทำ scripts unit test ให้หน่อยครับ แล้วจะมีการ run unit test อันนี้ที่  gitlab ci ms-personas.gitlab-fix-ci.yml ครับ
    3. ใน part realtime/streaming ช่วยดูใน ms_member_realtime_one_script.py ให้หน่อยครับ พอดี ผมทำรวมทุก step ไว้ในไฟล์เดียวกันก่อนครับ ถ้าได้ครบถ้วนแล้วจะ refactor ไปไว้ใน common module เหมือน part short term ครับ 
        ทีนี้ใน ms_member_realtime_one_script นี้
        1. ช่วย review ดูให้หน่อยว่ามีอะไรต้องแก้ เพราะผมเพิ่ม part write 3 แบบไปด้วย ตาม function write_sink ครับ
        2. หลังได้ mapped_records หลัง process mapping หรือ transform เหล่านี้ อาจจะมีการ cross join กับ bq table อื่นๆ ด้วย ผมมีไอเดีย คร่าวๆ ว่าเปิด windowing เพื่อ cache ข้อมูลที่ join ไว้ก่อน (น่าจะเป็นแค่ batch เท่านั้นไม่ควร เป็น realtime ) แล้วค่อยเอามา join ใน process mapping transform อีกที แบบนี้โอเคไหมครับ หรือมี pattern อะไรแนะนำเพิ่มเติมไหม 
        3. ช่วยแนะนำ best practice สำหรับ requirment แบบนี้ ในการจัดการ pipeline แบบ streaming ด้วยครับ ว่าควรมี pattern อะไรเพิ่มเติมไหม
        4. จาก 1-3 อันนี้ช่วย แก้ ms_member_realtime_one_script.py ใช้งานได้ ตามที่อยากได้ ให้ด้วยนะครับ และ ช่วยนำ ms_member_realtime_one_script มาแบ่งเป็น module ต่างๆ ใน dataflow common เหมือน part short term ด้วยครับ (ms_member_realtime_one_script เป็น scripts เดียวเหมือนเดิมนะครับ ไม่ต้องใช้จาก dataflow common)
        5. หลัง refactor module ของ realtime ms_member_realtime_one_script อันนี้เสร็จ ช่วยทำ test function และ module ทั้งหมดใน dataflow common ที่เพิ่มเข้ามา และ ทำ scripts unit test ให้หน่อยครับ แล้วจะมีการ run unit test อันนี้ที่ gitlab ci ms-personas.gitlab-fix-ci.yml ครับ
ตอนนี้น่าจะประมาณนี้ก่อนนะครับ ขอบคุณมากครับ
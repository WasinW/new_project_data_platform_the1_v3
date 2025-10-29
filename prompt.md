จาก github อันนี้ มี flow ตามนี้ 
airflow dags 
    > sent param config to scripts dataflow 
    > script dataflow call dataflow_common for build dataflow pipeline with orchestrate follow by config file (Config step)

และมี step short term ตาม config เลย 
ผมอยากให้ ลองช่วยอ่านโค้ดการทำงานทุกอย่าง อย่างละเอียด ทั้ง part dags , config , dataflow , dataflow_common (ทั้ง part orchestrate และ part module ต่างๆ ที่เกี่ยวข้อง)

คำถามคือ 
1. ช่วยทำ Design Architecture ของโปรแกรมนี้ 
2. ช่วยทำ flow การทำงานอย่างละเอียดของโปรแกรมเหล่านี้ มาให้หน่อย โดยแยกเป็นแต่ละ part ที่กล่าวมา 
3. จาก code เหล่านี้ ช่วย generate scripts dataflow สำหรับ real-time แบบง่ายๆ มาให้หน่อย อ้างอิงจาก step เดิมที่มีใน short term config 
และ โดยเปลี่ยน 
    1. step ReadBQQuery mapping_rows ให้ มีการเปิด windowing เพื่อเปิด client ไว้สำหรับ ไปคิวรี่ที่ bigquery ทุกๆ 1 hour แค่ชั่วโมงละครั้งพอ (BuildMappingDict ทำไปด้วยเลย เก็บ mapping_dict เป็น cache ไว้ก่อน)
    2. ReadBQQuery personas_rows เปลี่ยนเป็นงี้ 
        2.1 consume จาก PubSub subscription โดยจะเป็น notification ที่มีการ update ใหม่เข้ามา
        2.2 extract ข้อมูล personas_rows จาก payload ที่ได้รับมา เอา pk (member_id) มา
        2.3 นำ pk (member_id) ไปคิวรี่ที่ big table เพื่อดึงข้อมูล personas_rows มา 
            - ตรงนี้ช่วยดู ตรง module BigTableConnector ให้หน่อย ว่าควรใช้แบบนี้มั้ย หรือควรใช้แบบไหน ให้ง่ายๆกว่านี้ เพราะไม่อยากใช้ client เพราะมันเป็น realtime ต้องเปิดตลอด 
        2.4 นำ personas_rows ที่ได้มา มา map schemas เข้ากับ mapping_dict ที่ได้จาก step แรก ให้ได้ message ที่มี schemas เหมือนกับ mapping_dict ที่ได้มา column ไหนว่างๆ ก็ให้เติมค่า Null ไปแทน
        2.5 write message ที่ได้ ไปที่ BigQuery table เป้าหมาย และ write parquet file ลงที่ s3 ( ตัวเดียวกับ short term )ด้วย 
            2.5.1 ในการ write parquet file ลงที่ s3 จะต้องเปิด windowing เพื่อสร้าง folder partitions hourly (yyyy/mm/dd/hh) ด้วย เหมือนของเดิมเลย แต่ไม่ต้อง สะสม message ไว้เป็น batch ต้องเขียนทีละ message เลย แค่ต้องการเปิด windowing ไว้เพื่อ create folder partition เท่านั้น 
    จากข้อ 3 ทั้งหมดนี้ ช่วย generate code scripts dataflow แบบ scripts เดียวจบให้หน่อย แบบไม่ต้องใช้ dataflow_common นะ ขอเป็น code ที่เขียนแบบ dataflow pipeline เลย จะได้เข้าใจง่ายๆ 
    
ขอบคุณมากครับ

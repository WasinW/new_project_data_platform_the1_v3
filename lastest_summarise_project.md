ต่อไปนี้คือ **สรุปสถานะโปรเจ็กต์ + โซลูชันที่คอนเฟิร์มแล้ว + ประเด็นที่ยังติด/ต้องทำต่อ** แบบครบถ้วนในฉบับเดียว (อัปเดตตามสิ่งที่คุยกันทั้งหมด และยึด baseline ปัจจุบัน)

---

## 1) เป้าหมายและขอบเขต (ยืนยัน)

* **Tech Hub = Source of Truth รายโดเมน**
  แบ่งเป็น **App / Infra / Data Pipeline** (แต่ละโดเมนมี pipeline ของตัวเอง ไม่ใช่แพลตฟอร์มกลางเดียว)
  ส่งต่อข้อมูลให้ **Data Hub** ผ่าน **Refined (BigQuery native)** และ **Authorized View (AV)**; ช่วงสั้น/กลาง **ส่ง Parquet ที่ S3** เพื่อ back‑compat เส้นเดิม
* **Data Hub = Analytics only**
  ใช้ **AV จาก Tech Hub** เป็นแหล่งเดียว แล้วทำ **Analytic Layer** ด้วย **Dataform** (ไม่ถือ raw/structured เอง)

**ไทม์ไลน์ตาม term (ตกลงแล้ว)**

* **Short‑term**: Source = BQ, **Dataflow (batch)** → **S3/Parquet = main** (ยังไม่เปิด AV หรือเปิดเฉพาะที่จำเป็น)
* **Mid‑term**: Source = Pub/Sub + Bigtable, **Dataflow (streaming)** → **Refined(BQ)** + **S3/Parquet = secondary** → เปิด **AV**
* **Long‑term**: เหมือน Mid แต่ **ตัด Parquet** → **Refined(BQ)** + **AV only**

---

## 2) สถานะปัจจุบัน (ของจริงที่ “ใช้ได้แล้ว”)

### 2.1 Tech Hub – โดเมน **Insight** (prepare outbound)

* **Baseline ที่วิ่งได้**:
  **`dag_ms_member_short_term2` + `ms_member_short.yaml` + `ms_member_short_pipeline.py` + `dataflow_common`**
  ลอจิกหลัก: อ่าน BQ mapping + source personas(‑2h) → parse `personas` → map → join origin → coalesce → normalize → **Write Parquet (S3)**
* **DAG ทดสอบ init** (ยืนยันโครง Airflow/Beam/Dataflow, DTS, sensor ฯลฯ):
  `ms_member_short_term_init` ใช้ **BeamRunPythonPipelineOperator** ส่ง **`ms_member_short_pipeline.py`** + ติดตั้ง wheel **dataflow_common** และตั้งค่า Dataflow ผ่าน `DataflowConfiguration` และ sensor เฝ้าจบงาน (เป็น flow ตัวอย่างสำหรับตรวจ VPC‑SC/การอ่าน‑เขียน) 
* **แนวปฏิบัติที่ “ล็อกแล้ว” ใน Tech Hub**

  * โครง **config‑first (YAML)** ให้ orchestrator สร้าง pipeline อัตโนมัติ
  * **ไม่พังของเดิม**: เพิ่มของใหม่ด้วยพารามิเตอร์ เช่น `term_type`, `audit`, `dq` (ค่า default ทำให้พฤติกรรมเดิมคงเดิม)
  * **Audit (batch)**: 1 แถว/รัน ลง BQ (เช่น `tech_governance.audit_job_log`)
  * **DQ (daily)**: Dataplex DQ Task (rules ใน GCS) → Airflow trigger → Save ลง BQ (เช่น `tech_governance.dq_results`)
  * **Lineage**: เปิด Data Lineage API; BQ lineage อัตโนมัติ; Dataflow lineage เปิดด้วย `--dataflow_service_options=enable_lineage=true` (สำหรับ mid/long)

### 2.2 Data Hub – Analytics (ภาพรวม)

* ใช้ **AV จาก Tech Hub** เป็นแหล่ง staging
* ใช้ **Dataform** ทำ staging/marts + assertions tests
* เก็บ **audit analytics run** แยกใน `hub_governance.job_audit` + **dq_results** ฝั่ง Hub

---

## 3) โซลูชันที่คอนเฟิร์มแล้ว (Decision Log)

### 3.1 การเขียนแบบ Realtime → **เลือก BigQuery native + Storage Write API** เป็น **sink หลัก**

* เหตุผล: latency ต่ำ, รองรับ at‑least‑once / exactly‑once, ง่ายต่อการสเกล/สังเกตการณ์
* ใน `dataflow_common` → **`WriteToBigQueryStep` รองรับ SWA** (เมื่อระบุ `write_method: STORAGE_WRITE_API` + `use_storage_write_api=true` ใน pipeline options) ⇒ **ไม่ต้องใช้ GCS temp** สำหรับสตรีม
* **รูปแบบข้อมูล**: แนะนำ **changelog append‑only** (เช่น `event_ts`, `op`, `ingest_id`) + **Authorized View: latest‑by‑key** เพื่อให้ snapshot ปัจจุบันแบบ realtime **โดยไม่ต้อง MERGE ถี่**

### 3.2 Iceberg (หากต้องการ multi‑engine)

* **ไม่เปิด Iceberg บน native table เดิม** (เป็นคนละชนิดตาราง)
* แนวทาง: **Native เป็น SoT** → ทำ **Managed Iceberg (BigLake Iceberg) เป็นสำเนา** สำหรับ Spark/Trino

  * **Bootstrap**: CTAS (ถ้า region/feature รองรับ) หรือ CREATE+INSERT
  * **Incremental sync**: `MERGE`/`INSERT … SELECT` เป็นรอบ **near‑realtime** (เช่น 5–15 นาที) → ไม่กระทบเส้น realtime หลัก
  * ทำ **DQ compare** ระหว่าง native vs iceberg (rowcount/hash/sanity)

### 3.3 Dataplex (DQ / Lineage / Catalog)

* **DQ**: ใช้ **DQ Task** (rules ที่ GCS) และ/หรือ auto‑scan รายตาราง; **Airflow trigger** เพื่อรวม SLA/alert/audit กลาง
* **Lineage**: เปิด API; BigQuery auto; Dataflow เปิด `enable_lineage`; Composer ส่ง OpenLineage ให้ Dataplex (เวอร์ชัน provider รองรับ)
* **Universal Catalog**: ใช้กับ **BQ native ได้ตรง**; ถ้าเป็น Iceberg → ทำ **BigLake external** ก่อนเพื่อให้เข้า Catalog ได้

### 3.4 AV policy

* **Data Hub ใช้เฉพาะ AV** (ไม่มี direct table access)
* วิวของ Tech ต้อง **เสถียร/สคีมาคงที่** และตีความได้เหมือนกันทุกโดเมน

---

## 4) โครงสร้างโปรเจ็กต์ (ที่ตกลงใช้)

* **ต่อโดเมน** แยกโปรเจ็กต์: `<domain>-app`, `<domain>-infra`, `<domain>-data` (Tech) และ `data-hub-analytics` (Hub)
* **Datasets (ตัวอย่าง Insight/Tech)**

  * `refined_insight` (BQ native refined)
  * `gov_insight` (audit_job_log, dq_results)
* **Datasets (Data Hub)**

  * `hub_staging_insight`, `hub_marts_insight`, `hub_gov_insight`

---

## 5) สถานะโมดูล/โค้ด (ยึด baseline + สิ่งที่เพิ่มแล้ว/ควรเพิ่ม)

### 5.1 `dataflow_common`

* **Steps ที่ใช้แล้ว** (short‑term): ReadBQQuery, ParseJson, MapRecord, CoGroupByKey/Coalesce, NormalizeToSchema, **WriteParquet**
* **Steps ที่คุย/เตรียมเพิ่ม** (ไม่ทำให้ของเดิมพัง):

  * **WriteToBigQuery** (รองรับ SWA) – ใช้ได้แล้วในเฟรมเวิร์ก
  * **AuditStep**: batch (single row) + streaming windowed (1h default) → BQ
  * **PubSub/BT connector** (mid/long): Consume Pub/Sub + DLQ → Extract ID (configurable key) → Bigtable lookup (per‑id)
  * **DQ hook** (Trigger Dataplex task) – รันจาก Airflow ฝั่ง Tech
* **Config** (เพิ่มแบบ additive):

  * `term_type: short|mid|long` (default=short)
  * `audit.enabled: true|false`, `audit.window_duration_seconds` (streaming)
  * `sink.primary: bq_native|parquet` + `bq.write_method`
  * `streaming.extract_key.field_path` (เช่น personas_id)

### 5.2 Airflow / Composer

* ใช้ **BeamRunPythonPipelineOperator** + wheel `dataflow_common` + container image ใน worker
* **ข้อแนะนำที่ปรับแล้ว/ต้องปรับ**

  * **อย่าใช้ Jinja ซ้อน**ใน `DataflowConfiguration.project_id` (เคยเกิด 403 `dataflow.jobs.get` เพราะ Jinja ไม่ render ตอน hook เรียก Dataflow API) ⇒ ใช้ `Variable.get("project_id")` ใส่เป็น string ที่ render แล้ว, ให้ operator รอเอง (ตัด sensor ซ้ำ)
  * Composer SA: มี `roles/dataflow.developer` และ `roles/iam.serviceAccountUser` บน Worker SA
  * หากเปิด lineage ใน Dataflow: เติม `dataflow_service_options=['enable_lineage=true']` + (ถ้า streaming→BQ) `use_storage_write_api=true`

*(ตัวอย่าง DAG init ที่ทดสอบอยู่ อ้างอิงไฟล์: BeamRunPythonPipelineOperator + DataflowConfiguration + Sensors) *

---

## 6) สิ่งที่ “ยังติด/ต้องทำต่อ” (พร้อมแนวทางแก้)

### 6.1 Realtime mid/long (Pub/Sub → BT → BQ)

* **สถานะ**: โครงคิดผ่านแล้ว, ยังไม่ได้ integrate ลง `dataflow_common` แบบสมบูรณ์
* **ทำต่อ**

  1. เพิ่ม **connectors**: Pub/Sub consumer (with DLQ), Bigtable point‑lookup (async)
  2. เพิ่ม **steps**: ExtractId (configurable), ReadBigTableById, (optional) per‑key backoff/retry
  3. เพิ่ม **audit (windowed)** ลง BQ
  4. เพิ่ม **sink.bq** (SWA) + Authorized View (latest‑by‑key)

### 6.2 Audit (มาตรฐานแพลตฟอร์ม)

* **สถานะ**: สเปกตกลงแล้ว; ตาราง BQ อาจต้องสร้างจริง + wiring ใน orchestrator
* **ทำต่อ**:

  * DDL: `tech_governance.audit_job_log`, `*_gov.dq_results`
  * Implementation:

    * **Batch**: open‑row at start → close‑row on finish
    * **Streaming**: per‑window (1h default) start→close รวม count source/target/error

### 6.3 Dataplex DQ

* **สถานะ**: วิธีทำตกลงแล้ว (Task + rules บน GCS + Airflow trigger)
* **ทำต่อ**: จัด repo/rules path มาตรฐานต่อโดเมน, ตั้ง Task รายโดเมน, Wiring DAG daily/after‑batch

### 6.4 Lineage (ให้ขึ้นจริงทุกงาน)

* **สถานะ**: เข้าใจผลต่างแล้ว; บาง env ยังไม่เห็นแท็บ Lineage
* **ทำต่อ**: เปิด Data Lineage API ทั้ง project ที่ “ส่ง” และ “ดู”; ใส่ `enable_lineage` ให้ Dataflow; ให้สิทธิ์ viewer: `datalineage.viewer` + `dataplex.catalogViewer` (+ BQ viewer) กับทีม

### 6.5 Iceberg (สำเนา multi‑engine)

* **สถานะ**: โครงตัดสินใจแล้ว (native → managed Iceberg copy)
* **ทำต่อ**:

  * **Bootstrap**: CTAS หรือ CREATE+INSERT
  * **Incremental**: MERGE/INSERT SELECT ตาม watermark (5–15 นาที)
  * **DQ compare** native vs iceberg

### 6.6 DAG issue ที่พบจาก log

* 403 `dataflow.jobs.get` ตอน sensor/monitor เพราะ `project_id` ใน `DataflowConfiguration` ไม่ render (ยังเป็น `{{ var.value.project_id }}`) → แก้เป็น `Variable.get("project_id")` และให้ Beam operator รอเอง (ไม่ต้องมี sensor ซ้ำ) 

---

## 7) แผนงานถัดไป (ลงมือได้ทันที)

**สัปดาห์นี้**

* [ ] เพิ่ม field ใหม่ใน YAML (**ไม่พังของเดิม**): `term_type`, `sink`, `audit`, `dq`, `streaming.extract_key`
* [ ] ทำ DDL ตาราง **audit** + **dq_results** (Tech/Hub) และสร้างวิวสรุปสำหรับ alert
* [ ] แก้ DAG ตามข้อ 6.6 (Jinja render/roles) และใส่ `use_storage_write_api=true`/`enable_lineage=true` ใน mid/long template
* [ ] จัด GCS path สำหรับ **DQ rules** และ **สร้าง Dataplex DQ Task** (หนึ่ง task ต่อโดเมน)

**2–3 สัปดาห์**

* [ ] เติม **Pub/Sub + Bigtable connectors** และ **steps** เข้าสู่ `dataflow_common` (mid/long)
* [ ] ทำ **Authorized View (latest‑by‑key)** บน refined (Tech) → เปิดสิทธิ์ให้ Data Hub
* [ ] ตั้ง **DAG sync Iceberg** (ถ้าต้องใช้) + DQ compare

---

## 8) สิ่งที่ยังต้องตัดสิน (เล็กน้อย/ไม่บล็อก)

* ชื่อมาตรฐาน datasets ต่อโดเมน (เช่น `refined_<domain>`, `gov_<domain>`, `hub_staging_<domain>`, `hub_marts_<domain>`)
* ช่วงเวลา default ของ **audit window** (แนะนำ 1 ชม.) และ **Iceberg sync** (แนะนำ 5–15 นาที)
* การวาง Composer แยกต่อโดเมน หรือแชร์ env เดียว (คุ้มค่า vs isolation)

---

## 9) TL;DR สั้นที่สุด

* **ของเดิม (short‑term)** ใช้ได้แล้ว (batch → S3 main), ยึดไว้เป็น baseline
* **Realtime ต่อไป**: Dataflow → **BQ native (Storage Write API)** เป็นหลัก, ใช้ **Authorized View: latest‑by‑key** เพื่อ realtime snapshot
* **Iceberg**: ทำเป็น **สำเนา** (managed) จาก native เพื่อ multi‑engine (near‑realtime sync)
* **Governance**: Dataplex DQ (Task + Airflow trigger), Audit (BQ), Lineage (API + enable ใน Dataflow/Composer/BQ)
* **อย่าแก้ของเดิมจนพัง**: เสริมทุกอย่างด้วย **config** และ **DAG แยก** สำหรับ mid/long

---

หากต้องการ ผมจัด **แพ็กไฟล์เทมเพลต** (YAML เสริม, DDL audit/dq, DAG mid/long ตัวอย่าง, สคริปต์สร้าง DQ Task) ให้คุณวางได้ทันทีตามโครงนี้ โดยยึด baseline เดิม 100% ครับ.

module "insight_data_pipeline_composer_bucket" {
  source  = "git::https://gitlab.com/The1central/platform/the1-terraform-gcp.git//modules/gcs-bucket?ref=main"
  project = "${var.domain}-${terraform.workspace}"
  name    = "the1-insight-${terraform.workspace}-data-pipeline-composer"
}

module "insight_data_pipeline_dataflow_bucket" {
  source  = "git::https://gitlab.com/The1central/platform/the1-terraform-gcp.git//modules/gcs-bucket?ref=main"
  project = "${var.domain}-${terraform.workspace}"
  name    = "the1-insight-${terraform.workspace}-data-pipeline-dataflow"
}

module "insight_dataflow_common_repo" {
  source        = "git::https://gitlab.com/The1central/platform/the1-terraform-gcp.git//modules/artifact-registry?ref=main"
  project_id    = "${var.domain}-${terraform.workspace}"
  location      = var.region
  repository_id = "insight-dataflow-common"
  description   = "Image repository for Insight Dataflow Common"
  format        = "DOCKER"
  immutable_tags = false
  # cleanup_policies = [...]  # omit to use sensible defaults above
  # labels = {}
}

ตรงนี้ผมจะได้ bucket : xxxx-data-pipeline-composer , xxxx-data-pipeline-dataflow
ซึ่้งผม คิดไว้ว่า path ข้างในจะเป็นงี้ 
1. gs://xxxx-data-pipeline-composer/
  - dags/
    - dag_ms_member_short_term_init.py
    - dag_ms_member_short_term.py
  - config/
    - ms_member_short_init.yaml
    - ms_member_short.yaml
  - packages/
    - dataflow_common/dataflow_common-1.0.0-py3-none-any.whl (เดี๋ยวอันนี้ deploy ตามมา)
2. gs://xxxx-data-pipeline-dataflow/
  - job/
    - ms_member_short_term.py

การที่ผมอยากจะทำต่อคือ หลังจาก provision bucket เสร็จแล้ว 
ผมอยาก
1. provide diractory โครงสร้างข้างบนในแต่ละ bucket
2. provide table in bq 
3. provide service bq data transfer service
4. upload scripts files to bucket ตามโครงสร้างข้างบน whl ยังไม่ต้อง upload ตอนนี้ ผมอยาก build จาก python dataflow_common/setup.py bdist_wheel แล้วเอามา upload ทีหลัง ด้วย tf 
5. build dataflow_common จาก python dataflow_common/setup.py bdist_wheel
6. upload dataflow_common-1.0.0-py3-none-any.whl to gs://xxxx-data-pipeline-composer/packages/dataflow_common/
7. build docker image from dataflow_common and push to artifact registry created from module "insight_dataflow_common_repo"
8. provide composer airflow and variables
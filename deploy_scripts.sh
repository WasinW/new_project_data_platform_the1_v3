cd packages/dataflow-common && python -m build
cd ../..
# common package
gsutil cp packages/dataflow-common/dist/dataflow_common-1.0.0-py3-none-any.whl gs://t1-dataflow-framework-bucket/framework/artifacts/
gsutil cp packages/dataflow-common/dist/dataflow_common-1.0.0.tar.gz gs://t1-dataflow-framework-bucket/common/packages/

# dags
gsutil cp composer/dags/dag_short_term_hourly.py gs://t1-airflow-composer-bucket/dags/composer/dags/
gsutil cp composer/dags/full_patch_pl.py gs://t1-airflow-composer-bucket/dags/composer/dags/

# dataflow framework
# gsutil cp dataflow/setup.py gs://t1-dataflow-framework-bucket/framework/
gsutil cp dataflow/FW/unified_dataflow_pipeline_bigtable.py gs://t1-dataflow-framework-bucket/framework/

# configs
gsutil cp composer/config/ms_member/batch/short_term_hourly.yaml gs://t1-airflow-composer-bucket/dags/composer/config/ms_member/batch/
gsutil cp composer/config/ms_member/common/defaults.yaml gs://t1-airflow-composer-bucket/dags/composer/config/ms_member/common/
gsutil cp composer/config/ms_member/init/pl_init_config.yaml gs://t1-airflow-composer-bucket/dags/composer/config/ms_member/init/
gsutil cp composer/config/ms_member/reconcile/full_patch_config.yaml gs://t1-airflow-composer-bucket/dags/composer/config/ms_member/reconcile/
gsutil cp composer/config/ms_member/streaming/streaming_realtime.yaml gs://t1-airflow-composer-bucket/dags/composer/config/ms_member/streaming/


# 
git add --all && git commit -a -m 'fix' && git push origin HEAD 


gsutil cp composer/dags/dag_short_term_hourly.py gs://t1-airflow-composer-bucket/dags/composer/dags/
gsutil cp dataflow/FW/unified_dataflow_pipeline_bigtable.py gs://t1-dataflow-framework-bucket/framework/
gsutil cp composer/config/ms_member/batch/short_term_hourly.yaml gs://t1-airflow-composer-bucket/dags/composer/config/ms_member/batch/
gsutil cp packages/dataflow-common/dist/dataflow_common-1.0.0-py3-none-any.whl gs://t1-dataflow-framework-bucket/framework/artifacts/

        modified:   composer/config/ms_member/batch/short_term_hourly.yaml
        modified:   composer/config/ms_member/common/defaults.yaml
        modified:   composer/dags/dag_short_term_hourly.py
        modified:   dataflow/FW/unified_dataflow_pipeline_bigtable.py
gsutil cp composer/config/ms_member/batch/short_term_hourly.yaml gs://t1-airflow-composer-bucket/dags/composer/config/ms_member/batch/
gsutil cp composer/config/ms_member/common/defaults.yaml gs://t1-airflow-composer-bucket/dags/composer/config/ms_member/common/
gsutil cp composer/dags/dag_short_term_hourly.py gs://t1-airflow-composer-bucket/dags/composer/dags/
gsutil cp dataflow/FW/unified_dataflow_pipeline_bigtable.py gs://t1-dataflow-framework-bucket/framework/


gsutil cp dataflow/FW/test_bq_read_simple.py gs://t1-airflow-composer-bucket/dags/composer/dags/
gsutil cp dataflow/FW/test_bq_read_simple2.py gs://t1-airflow-composer-bucket/dags/composer/dags/

gsutil cp composer/dags/test_run_bigquery_pipeline.py gs://t1-airflow-composer-bucket/dags/composer/dags/test_run_bigquery_pipeline.py
gsutil cp composer/dags/test_run_bigquery_pipeline2.py gs://t1-airflow-composer-bucket/dags/composer/dags/test_run_bigquery_pipeline.py
gsutil cp composer/dags/test_run_bigquery_pipeline3.py gs://t1-airflow-composer-bucket/dags/composer/dags/test_run_bigquery_pipeline.py
gsutil cp composer/dags/test_run_bigquery_pipeline4.py gs://t1-airflow-composer-bucket/dags/composer/dags/test_run_bigquery_pipeline.py
gsutil cp dataflow/FW/test_bq_read_simple4.py gs://t1-airflow-composer-bucket/dags/composer/dags/


export PROJECT_ID="the1-insight-dev"
export REGION="asia-southeast1"
export REPOSITORY="dataflow-images"
export IMAGE_NAME="beam-aws-pipeline"
export IMAGE_TAG="v1.0"

export IMAGE_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/${IMAGE_NAME}:${IMAGE_TAG}"

# Build ด้วย Cloud Build (ต้องมี Dockerfile อยู่ใน directory ปัจจุบัน)
gcloud builds submit \
    --tag="${IMAGE_URI}" \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --timeout=20m

# -------------------------------------------------------------------------------------------------------------------

# cd new_common/dataflow_common_container_fixed
cd dataflow_common

# Build wheel ใหม่
python setup.py bdist_wheel

# Upload
gsutil cp dist/dataflow_common-1.0.0-py3-none-any.whl \
    gs://t1-dataflow-framework-bucket/packages/

gsutil cp dist/dataflow_common-1.0.0-py3-none-any.whl \
    gs://t1-airflow-composer-bucket/dags/packages/

# Build Docker image ใหม่
docker build -t dataflow-test:v1.21 .

# Push
docker tag dataflow-test:v1.21 \
    asia-southeast1-docker.pkg.dev/the1-insight-dev/dataflow-images/dataflow-common:v1.21

docker push asia-southeast1-docker.pkg.dev/the1-insight-dev/dataflow-images/dataflow-common:v1.21

gsutil cp composer/dags/test_run_bigquery_pipeline2.py gs://t1-airflow-composer-bucket/dags/composer/dags/test_run_bigquery_pipeline.py
gsutil cp composer/dags/dag_ms_member_short_term.py gs://t1-airflow-composer-bucket/dags/composer/dags/
gsutil cp composer/dags/dag_ms_member_short_term2.py gs://t1-airflow-composer-bucket/dags/composer/dags/
gsutil cp dataflow_common/ms_member_short_pipeline.py gs://t1-dataflow-framework-bucket/jobs/

gsutil cp composer/config/ms_member/batch/ms_member_short_init.yaml gs://t1-airflow-composer-bucket/dags/composer/config/ms_member/batch/
gsutil cp composer/config/ms_member/batch/ms_member_short.yaml gs://t1-airflow-composer-bucket/dags/composer/config/ms_member/batch/
gsutil cp composer/dags/dag_ms_member_short_term_init.py gs://t1-airflow-composer-bucket/dags/composer/dags/
gsutil cp composer/dags/dag_ms_member_short_term2.py gs://t1-airflow-composer-bucket/dags/composer/dags/

gsutil cp composer/dags/dag_ms_member_short_term2.py gs://t1-airflow-composer-bucket/dags/composer/dags/
gsutil cp composer/config/ms_member/batch/ms_member_short.yaml gs://t1-airflow-composer-bucket/dags/composer/config/ms_member/batch/

gsutil cp dataflow/job/ms_member_short_bootstrap.py gs://t1-dataflow-framework-bucket/jobs/
gsutil cp composer/dags/dag_ms_member_short_term_bs.py gs://t1-airflow-composer-bucket/dags/composer/dags/

gsutil cp composer/dags/dag_ms_member_short_term_refactor.py gs://t1-airflow-composer-bucket/dags/composer/dags/
gsutil cp dataflow/job/ms_member_pipeline_v2.py gs://t1-dataflow-framework-bucket/jobs/

# ----------------------------------------------------------------------------------------------------------------
# 1. Build wheels
cd dataflow_framework_v2/dataflow_builder
python setup.py bdist_wheel
cd ../..

cd dataflow_framework_v2/dataflow_worker
python setup.py bdist_wheel
cd ../..

# 2. Upload wheels to GCS
gsutil cp dataflow_framework_v2/dataflow_builder/dist/dataflow_builder-2.0.0-py3-none-any.whl \
    gs://t1-airflow-composer-bucket/dags/packages/

gsutil cp dataflow_framework_v2/dataflow_worker/dist/dataflow_worker-2.0.0-py3-none-any.whl \
    gs://t1-airflow-composer-bucket/dags/packages/

# 3. Build Docker (ถ้า network OK)
docker build -t dataflow-worker:v2.0 .

# 4. Tag
docker tag dataflow-worker:v2.0 \
    asia-southeast1-docker.pkg.dev/the1-insight-dev/dataflow-images/dataflow-worker:v2.0

# 5. Push (ถ้า network OK)
docker push \
    asia-southeast1-docker.pkg.dev/the1-insight-dev/dataflow-images/dataflow-worker:v2.0


gsutil cp dist/dataflow-builder-2.0.0-py3-none-any.whl \
    gs://t1-airflow-composer-bucket/dags/packages/
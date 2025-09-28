cd packages/dataflow-common && python -m build
cd ../..
gsutil cp packages/dataflow-common/dist/dataflow_common-1.0.0-py3-none-any.whl gs://t1-dataflow-framework-bucket/common/packages/
gsutil cp packages/dataflow-common/dist/dataflow_common-1.0.0.tar.gz gs://t1-dataflow-framework-bucket/common/packages/

gsutil cp composer/dags/dag_short_term_hourly.py gs://t1-airflow-composer-bucket/dags/composer/dags/
gsutil cp dataflow/FW/unified_dataflow_pipeline_bigtable.py gs://t1-dataflow-framework-bucket/framework/

gsutil cp composer/config/ms_member/batch/short_term_hourly.yaml gs://t1-airflow-composer-bucket/dags/composer/config/ms_member/batch/
gsutil cp composer/config/ms_member/common/defaults.yaml gs://t1-airflow-composer-bucket/dags/composer/config/ms_member/common/
gsutil cp composer/config/ms_member/init/pl_init_config.yaml gs://t1-airflow-composer-bucket/dags/composer/config/ms_member/init/
gsutil cp composer/config/ms_member/reconcile/full_patch_config.yaml gs://t1-airflow-composer-bucket/dags/composer/config/ms_member/reconcile/
gsutil cp composer/config/ms_member/streaming/streaming_realtime.yaml gs://t1-airflow-composer-bucket/dags/composer/config/ms_member/streaming/

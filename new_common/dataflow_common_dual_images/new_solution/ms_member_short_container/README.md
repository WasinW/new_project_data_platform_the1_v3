# ms_member_short pipeline image

This directory contains a Dockerfile and supporting files for building a
custom container image that packages both the `ms_member_short` pipeline
script and the reusable `dataflow_common` library.  The image is built
using a two‑stage Docker build:

- **Stage 1 (common)**: copies the `dataflow_common` package and
  installs dependencies listed in `requirements.txt`.  This stage is
  primarily for convenience and to reduce duplication when building
  multiple pipeline images.
- **Stage 2 (final)**: copies the library and the pipeline script into
  the `/app` directory and installs dependencies again.  Setting
  `WORKDIR /app` ensures that Python can import `dataflow_common`
  directly (`from dataflow_common.config import load_config`) without
  modifying `sys.path`.

## Building the image

1. Ensure you have built the common base image separately (see
   `../dataflow_common_base`).  Alternatively, this Dockerfile will
   build the library stage itself since it copies the library and
   dependencies directly.

2. In this directory, run:

   ```sh
   docker build -t <REGION>-docker.pkg.dev/<PROJECT>/<REPOSITORY>/ms-member-short:<TAG> .
   docker push <REGION>-docker.pkg.dev/<PROJECT>/<REPOSITORY>/ms-member-short:<TAG>
   ```

   Replace `<REGION>`, `<PROJECT>`, `<REPOSITORY>`, and `<TAG>` with
   values appropriate for your environment.

## Using the image in Airflow

In your Airflow DAG, configure the `BeamRunPythonPipelineOperator` to
use this image and point `py_file` to the pipeline script inside the
container (`/app/ms_member_short_pipeline.py`).  The YAML config
remains external on GCS.  Example:

```python
run_pipeline = BeamRunPythonPipelineOperator(
    task_id='run_dataflow_pipeline',
    runner='DataflowRunner',
    py_file='/app/ms_member_short_pipeline.py',
    pipeline_options={
        'project': 'the1-insight-dev',
        'region': 'asia-southeast1',
        'temp_location': 'gs://t1-insight-audit-bucket/audit_log/dataflow/temp',
        'staging_location': 'gs://t1-insight-audit-bucket/audit_log/dataflow/staging',
        'service_account_email': 't1-ins-dev-sa-data@the1-insight-dev.iam.gserviceaccount.com',
        'sdk_container_image': '<REGION>-docker.pkg.dev/<PROJECT>/<REPOSITORY>/ms-member-short:<TAG>',
        'sdk_location': 'container',
        'config_path': 'gs://t1-airflow-composer-bucket/dags/composer/config/ms_member/batch/ms_member_short.yaml',
        # other options like S3 credentials
    },
    py_requirements=[
        'apache-beam[gcp]==2.59.0',
    ],
    py_system_site_packages=False,
    dag=dag,
)
```

Because the library and script are packaged together in `/app`, Beam will
stage them into its virtual environment on the worker.  There is no
need for `extra_packages` or modifying `sys.path`.
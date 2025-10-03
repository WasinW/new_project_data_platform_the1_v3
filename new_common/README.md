# dataflow_common – Containerized Version

This package contains the source code for `dataflow_common` and a `Dockerfile` that
can be used to build a custom container for Google Cloud Dataflow.  The goal
is to avoid relying on the `extra_packages` pipeline option and instead bake
all dependencies (including this library) into a Docker image.

## Project layout

```
dataflow_common_container/
├── src/dataflow_common/    # Source code of the library (generic, no job‑specific logic)
├── requirements.txt        # Python dependencies to install in the container
├── Dockerfile              # Defines how to build the custom container image
├── README.md               # This file
├── pyproject.toml          # Packaging metadata (optional)
├── setup.cfg               # Packaging configuration (optional)
├── setup.py                # Fallback setup script (optional)
└── ... (additional files as needed)
```

## Building the container

1. **Choose a base image**.  Dataflow custom containers must be built on top
   of an Apache Beam Python SDK image.  For example, to match Beam 2.59.0 and
   Python 3.11 you can use:

   ```dockerfile
   FROM apache/beam_python3.11_sdk:2.59.0
   ```

   You may need to adjust the tag to match the version of Beam used in your
   pipeline and your Dataflow worker configuration.

2. **Install dependencies and the library**.  The included `Dockerfile`
   illustrates how to copy the source code into the image, install the
   dependencies listed in `requirements.txt`, and install the library itself
   using `pip`.  Feel free to modify `requirements.txt` to include the two
   dependencies you mentioned (for example `google-cloud-bigquery` and
   `pyarrow`).

3. **Build and push the image**.  Run the provided script or use the
   following commands (replace `<PROJECT_ID>` and `<IMAGE_NAME>` with your
   own values):

   ```sh
   # Navigate into the project directory
   cd dataflow_common_container
   # Build the image locally
   docker build -t asia-southeast1-docker.pkg.dev/<PROJECT_ID>/<REPOSITORY>/<IMAGE_NAME>:v1 .
   # Push it to Artifact Registry (or Container Registry)
   docker push asia-southeast1-docker.pkg.dev/<PROJECT_ID>/<REPOSITORY>/<IMAGE_NAME>:v1
   ```

## Using the custom container in Dataflow

When submitting your pipeline via the Airflow DAG, specify the
`sdk_container_image` pipeline option instead of `extra_packages`.  For
example:

```python
BeamRunPythonPipelineOperator(
    task_id='run_dataflow_pipeline',
    runner='DataflowRunner',
    py_file='gs://your-bucket/path/to/your_pipeline.py',
    pipeline_options={
        'project': 'your-gcp-project',
        'region': 'asia-southeast1',
        # ... other options ...
        'sdk_container_image': 'asia-southeast1-docker.pkg.dev/your-gcp-project/your-repo/your-image:v1',
        # Do not specify extra_packages
    },
    # Optionally keep py_requirements for the driver side
    py_requirements=[ ... ],
    py_system_site_packages=False,
)
```

This instructs Dataflow to pull your custom container for the worker harness.

## Notes

- The base image already includes the Apache Beam SDK.  You should **not**
  install `apache-beam` in the container; instead install only the
  additional dependencies your pipeline needs.
- Your pipeline script (`py_file`) can remain on GCS; only the
  dependencies need to be packaged into the container.
- If your schema or other metadata is stored on GCS or BigQuery, the
  pipeline still reads those resources at runtime.  Ensure your Dataflow
  service account has the necessary IAM permissions.


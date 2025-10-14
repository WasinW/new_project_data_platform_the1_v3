@echo off
REM Build script for dataflow packages (Windows)

echo Building dataflow packages...

REM Build dataflow-builder wheel
echo Building dataflow-builder...
cd dataflow_framework_v2\dataflow_builder
python -m pip install build
python -m build
echo Built dataflow-builder

REM Build dataflow-worker
echo Preparing dataflow-worker...
cd ..\dataflow_worker
python -m build
echo Built dataflow-worker

cd ..\..
echo Build completed!
echo.
echo Next steps:
echo 1. Upload wheel: gsutil cp dataflow_framework_v2\dataflow_builder\dist\*.whl gs://t1-airflow-composer-bucket/dags/packages/
echo 2. Build Docker image with Dockerfile

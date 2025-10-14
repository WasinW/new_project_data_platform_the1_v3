#!/usr/bin/env python
"""
Complete build script for dataflow packages
"""

import os
import sys
import subprocess
from pathlib import Path
import shutil

def run_command(cmd, cwd=None):
    """Run command and show output"""
    print(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=True, cwd=cwd, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        return False
    return True

def build_packages():
    """Build both packages"""
    print("=" * 60)
    print("Building Dataflow Packages")
    print("=" * 60)
    
    # Check if directories exist
    builder_dir = Path("dataflow_framework_v2/dataflow_builder")
    worker_dir = Path("dataflow_framework_v2/dataflow_worker")
    
    if not builder_dir.exists():
        print("❌ dataflow_builder directory not found!")
        return False
    
    if not worker_dir.exists():
        print("❌ dataflow_worker directory not found!")
        return False
    
    # Install build tool
    print("\n📦 Installing build tools...")
    run_command("pip install --upgrade build")
    
    # Build dataflow-builder
    print("\n📦 Building dataflow-builder...")
    if not run_command("python -m build", cwd=str(builder_dir)):
        print("❌ Failed to build dataflow-builder")
        return False
    
    # Check if wheel was created
    builder_wheel = list(builder_dir.glob("dist/*.whl"))
    if builder_wheel:
        print(f"✅ Created: {builder_wheel[0].name}")
    else:
        print("❌ No wheel file created for dataflow-builder")
        return False
    
    # Build dataflow-worker
    print("\n📦 Building dataflow-worker...")
    if not run_command("python -m build", cwd=str(worker_dir)):
        print("❌ Failed to build dataflow-worker")
        return False
    
    # Check if wheel was created
    worker_wheel = list(worker_dir.glob("dist/*.whl"))
    if worker_wheel:
        print(f"✅ Created: {worker_wheel[0].name}")
    else:
        print("❌ No wheel file created for dataflow-worker")
        return False
    
    print("\n✅ Build completed successfully!")
    print(f"\n📁 Wheels created:")
    print(f"  - {builder_wheel[0]}")
    print(f"  - {worker_wheel[0]}")
    
    return True

def create_dockerfile():
    """Create Dockerfile for worker image"""
    print("\n📝 Creating Dockerfile...")
    
    dockerfile_content = '''FROM apache/beam_python3.11_sdk:2.59.0

ENV RUN_PYTHON_SDK_IN_DEFAULT_ENVIRONMENT=1

# Install build dependencies
RUN pip install --upgrade pip setuptools wheel

# Copy wheel files
COPY dataflow_framework_v2/dataflow_builder/dist/*.whl /tmp/
COPY dataflow_framework_v2/dataflow_worker/dist/*.whl /tmp/

# Install dataflow packages
RUN pip install /tmp/dataflow-builder-*.whl
RUN pip install /tmp/dataflow-worker-*.whl

# Install additional dependencies
RUN pip install --no-cache-dir \\
    boto3==1.34.106 \\
    pyarrow==14.0.2 \\
    google-cloud-bigquery==3.25.0 \\
    pyyaml==6.0.1

# Clean up
RUN rm -rf /tmp/*.whl

# Verify installation
RUN python -c "from dataflow_builder.config import PipelineConfig; print('dataflow-builder OK')"
RUN python -c "from dataflow_worker.steps import ReadBQQueryStep; print('dataflow-worker OK')"

WORKDIR /
'''
    
    with open("Dockerfile", "w", encoding="utf-8") as f:
        f.write(dockerfile_content)
    
    print("✅ Dockerfile created")

def build_docker():
    """Build Docker image"""
    print("\n🐳 Building Docker image...")
    
    # Check if Docker is available
    if not run_command("docker --version"):
        print("❌ Docker not found. Please install Docker.")
        return False
    
    # Build image
    print("Building dataflow-worker:v2.0...")
    if not run_command("docker build -t dataflow-worker:v2.0 ."):
        print("❌ Failed to build Docker image")
        return False
    
    print("✅ Docker image built successfully!")
    
    # Tag for GCP Artifact Registry
    print("\n🏷️ Tagging for GCP Artifact Registry...")
    if run_command("docker tag dataflow-worker:v2.0 asia-southeast1-docker.pkg.dev/the1-insight-dev/dataflow-images/dataflow-worker:v2.0"):
        print("✅ Tagged for GCP")
    
    return True

def create_upload_script():
    """Create script for uploading artifacts"""
    
    upload_script = '''#!/bin/bash
# Upload script for dataflow artifacts

echo "📤 Uploading dataflow-builder wheel to GCS..."
gsutil cp dataflow_framework_v2/dataflow_builder/dist/*.whl \\
    gs://t1-airflow-composer-bucket/dags/packages/

if [ $? -eq 0 ]; then
    echo "✅ Wheel uploaded to GCS"
else
    echo "❌ Failed to upload wheel"
fi

echo ""
echo "🐳 Pushing Docker image to Artifact Registry..."
echo "Make sure you're authenticated:"
echo "  gcloud auth configure-docker asia-southeast1-docker.pkg.dev"
echo ""

docker push asia-southeast1-docker.pkg.dev/the1-insight-dev/dataflow-images/dataflow-worker:v2.0

if [ $? -eq 0 ]; then
    echo "✅ Docker image pushed"
else
    echo "❌ Failed to push Docker image"
fi
'''
    
    with open("upload_artifacts.sh", "w", encoding="utf-8") as f:
        f.write(upload_script)
    
    # Make executable on Unix-like systems
    if os.name != 'nt':
        os.chmod("upload_artifacts.sh", 0o755)
    
    print("\n📝 Created upload_artifacts.sh")

def main():
    """Main build process"""
    
    # Step 1: Build packages
    if not build_packages():
        print("\n❌ Build failed!")
        sys.exit(1)
    
    # Step 2: Create Dockerfile
    create_dockerfile()
    
    # Step 3: Build Docker image
    if build_docker():
        print("\n" + "=" * 60)
        print("✅ BUILD SUCCESS!")
        print("=" * 60)
        
        # Create upload script
        create_upload_script()
        
        print("\n📋 Next steps:")
        print("1. Upload wheel to GCS:")
        print("   gsutil cp dataflow_framework_v2/dataflow_builder/dist/*.whl \\")
        print("     gs://t1-airflow-composer-bucket/dags/packages/")
        print("")
        print("2. Push Docker image:")
        print("   docker push asia-southeast1-docker.pkg.dev/the1-insight-dev/dataflow-images/dataflow-worker:v2.0")
        print("")
        print("Or run: ./upload_artifacts.sh")
    else:
        print("\n⚠️ Docker build failed, but wheels are ready")

if __name__ == "__main__":
    main()
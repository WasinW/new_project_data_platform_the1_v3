#!/usr/bin/env python
"""Bootstrap script that installs dependencies before importing"""

import subprocess
import sys
import os
import tempfile
import site

def install_dependencies():
    """Install required packages"""
    # Check if already installed
    try:
        import dataflow_common
        print("dataflow_common already installed")
        return
    except ImportError:
        print("Installing dataflow_common...")
    
    # Install from GCS
    wheel_gcs_path = "gs://t1-airflow-composer-bucket/dags/packages/dataflow_common-1.0.0-py3-none-any.whl"
    
    # Download and install
    # Create temp directory
    with tempfile.TemporaryDirectory() as tmpdir:
        local_wheel = os.path.join(tmpdir, "dataflow_common-1.0.0-py3-none-any.whl")
        
        # Method 1: Use gsutil to download
        try:
            print(f"Downloading {wheel_gcs_path} to {local_wheel}")
            subprocess.check_call([
                "gsutil", "cp", wheel_gcs_path, local_wheel
            ])
        except Exception as e:
            print(f"gsutil failed: {e}, trying alternative method...")
            
            # Method 2: Use Apache Beam's FileSystems (already available)
            from apache_beam.io.filesystems import FileSystems
            print(f"Downloading using Beam FileSystems...")
            
            with FileSystems.open(wheel_gcs_path) as src:
                with open(local_wheel, 'wb') as dst:
                    dst.write(src.read())
        
        # Install from local file
        print(f"Installing from {local_wheel}")
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", 
            local_wheel, 
            "--no-deps",  # Avoid dependency conflicts
            "--force-reinstall"  # Force reinstall if exists
        ])
    
    print("Installation complete!")
    
    # ✅ Add user site-packages to sys.path
    user_site = site.getusersitepackages()
    if user_site not in sys.path:
        print(f"Adding {user_site} to sys.path")
        sys.path.insert(0, user_site)
    
    # ✅ Force reload the module path
    import importlib
    import importlib.util
    importlib.invalidate_caches()
    
    # ✅ Verify installation
    try:
        import dataflow_common
        print(f"dataflow_common successfully imported from {dataflow_common.__file__}")
    except ImportError as e:
        print(f"Failed to import dataflow_common after installation: {e}")
        print(f"sys.path: {sys.path}")
        # Try to find where it was installed
        result = subprocess.run(
            [sys.executable, "-m", "pip", "show", "dataflow-common"],
            capture_output=True, text=True
        )
        print(f"pip show output: {result.stdout}")
        raise

def main():
    """Main entry point"""
    # Step 1: Install dependencies
    install_dependencies()
    
    # Step 2: Now we can import!
    print("Importing dataflow_common...")
    from dataflow_common.config import load_config
    from dataflow_common.orchestrator import Orchestrator
    from apache_beam.options.pipeline_options import PipelineOptions
    
    import argparse
    import logging
    from datetime import datetime
    
    logging.basicConfig(level=logging.INFO)
    
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--config_path", required=True)
    parser.add_argument("--run_dt", default=None)
    parser.add_argument("--cache_path", 
                       default="gs://t1-insight-audit-bucket/cache/ms_member_max_date.txt")
    known_args, pipeline_args = parser.parse_known_args()
    
    print(f"Loading config from {known_args.config_path}")
    # Load config
    cfg = load_config(known_args.config_path)
    
    # Read max_date from cache
    if known_args.cache_path:
        print(f"Reading max_date from {known_args.cache_path}")
        from apache_beam.io.filesystems import FileSystems
        try:
            with FileSystems.open(known_args.cache_path) as f:
                content = f.read()
                if isinstance(content, bytes):
                    content = content.decode('utf-8')
                cfg.params.max_date = content.strip()
                print(f"Max date set to: {cfg.params.max_date}")
        except Exception as e:
            print(f"Failed to read cache: {e}, using default")
            cfg.params.max_date = "2020-01-01 00:00:00"
    
    # Set run_dt and partitions
    if known_args.run_dt:
        cfg.params.run_dt = known_args.run_dt
    else:
        now = datetime.now()
        cfg.params.run_dt = now.strftime('%Y%m%d%H')
        cfg.params.run_par_month = now.strftime('%Y%m')
        cfg.params.run_par_day = now.strftime('%d')
        cfg.params.run_par_hour = now.strftime('%H')
    
    print(f"Pipeline params: run_dt={cfg.params.run_dt}, max_date={cfg.params.max_date}")
    
    # Create pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    
    # Run pipeline
    print("Starting pipeline orchestration...")
    orchestrator = Orchestrator(cfg)
    orchestrator.run(pipeline_options)
    
    print("Pipeline submitted successfully!")

if __name__ == "__main__":
    main()
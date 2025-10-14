#!/usr/bin/env python
"""
Fix all import issues in dataflow_worker package
"""

import re
from pathlib import Path

def fix_all_imports():
    """Fix all imports in the refactored packages"""
    
    print("🔧 Fixing imports in dataflow_worker...")
    
    worker_dir = Path("dataflow_framework_v2/dataflow_worker/src/dataflow_worker")
    
    # Map of files and their specific fixes
    fixes = {
        # Fix steps/__init__.py
        "steps/__init__.py": [
            (r'from \.\.config import PipelineConfig', 'from dataflow_builder.config import PipelineConfig'),
            (r'from \.\.core import BaseStep', 'from dataflow_worker.core import BaseStep'),
            (r'from \.\.connectors import', 'from dataflow_worker.connectors import'),
            (r'from \.\.transforms import', 'from dataflow_worker.transforms import'),
            (r'from \.streaming import', 'from .streaming import'),
        ],
        
        # Fix steps/streaming.py
        "steps/streaming.py": [
            (r'from \.\.core import BaseStep', 'from dataflow_worker.core import BaseStep'),
            (r'from \.\.connectors\.pubsub import', 'from dataflow_worker.connectors.pubsub import'),
            (r'from \.\.connectors\.bigtable import', 'from dataflow_worker.connectors.bigtable import'),
            (r'from \.\.config import PipelineConfig', 'from dataflow_builder.config import PipelineConfig'),
        ],
        
        # Fix steps/bigtable_batch_steps.py
        "steps/bigtable_batch_steps.py": [
            (r'from \.\.core import BaseStep', 'from dataflow_worker.core import BaseStep'),
            (r'from \.\.connectors\.bigtable_batch import', 'from dataflow_worker.connectors.bigtable_batch import'),
        ],
        
        # Fix steps/bigtable_realtime_steps.py
        "steps/bigtable_realtime_steps.py": [
            (r'from \.\.core import BaseStep', 'from dataflow_worker.core import BaseStep'),
            (r'from \.\.connectors\.bigtable_realtime import', 'from dataflow_worker.connectors.bigtable_realtime import'),
        ],
        
        # Fix core.py
        "core.py": [
            (r'from \.config import PipelineConfig', 'from dataflow_builder.config import PipelineConfig'),
        ],
        
        # Fix connectors/__init__.py
        "connectors/__init__.py": [
            (r'from \.\.config import PipelineConfig', 'from dataflow_builder.config import PipelineConfig'),
            (r'from \.\.transforms\.schema import', 'from dataflow_worker.transforms.schema import'),
            (r'from \.bigtable import', 'from .bigtable import'),
            (r'from \.pubsub import', 'from .pubsub import'),
        ],
        
        # Fix connectors/bigtable.py
        "connectors/bigtable.py": [
            (r'from \.\.config import PipelineConfig', 'from dataflow_builder.config import PipelineConfig'),
        ],
        
        # Fix transforms/schema.py
        "transforms/schema.py": [
            (r'from \.\.config import', 'from dataflow_builder.config import'),
        ],
    }
    
    # Apply fixes to specific files
    for file_path, replacements in fixes.items():
        full_path = worker_dir / file_path
        if full_path.exists():
            fix_file(full_path, replacements, file_path)
    
    # Generic fixes for all Python files
    print("\n🔍 Applying generic fixes to all Python files...")
    generic_replacements = [
        # Fix any remaining config imports
        (r'from dataflow_common\.config import', 'from dataflow_builder.config import'),
        (r'from dataflow_worker\.config import', 'from dataflow_builder.config import'),
        
        # Fix dataflow_common references
        (r'from dataflow_common\.core import', 'from dataflow_worker.core import'),
        (r'from dataflow_common\.transforms', 'from dataflow_worker.transforms'),
        (r'from dataflow_common\.connectors', 'from dataflow_worker.connectors'),
        (r'from dataflow_common\.steps', 'from dataflow_worker.steps'),
    ]
    
    for py_file in worker_dir.rglob("*.py"):
        fix_file(py_file, generic_replacements, py_file.relative_to(worker_dir))
    
    print("\n✅ Import fixing completed!")

def fix_file(file_path, replacements, display_name):
    """Fix imports in a single file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original = content
        changes = []
        
        for pattern, replacement in replacements:
            new_content = re.sub(pattern, replacement, content)
            if new_content != content:
                # Find what was replaced for logging
                matches = re.findall(pattern, content)
                if matches:
                    changes.append(f"  {pattern} → {replacement}")
                content = new_content
        
        if content != original:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"✓ Fixed {display_name}")
            for change in changes:
                print(f"  {change}")
                
    except Exception as e:
        print(f"❌ Error fixing {display_name}: {e}")

def verify_imports():
    """Verify that imports are correct"""
    print("\n🔍 Verifying imports...")
    
    worker_dir = Path("dataflow_framework_v2/dataflow_worker/src/dataflow_worker")
    
    errors = []
    
    # Check for wrong imports
    for py_file in worker_dir.rglob("*.py"):
        try:
            with open(py_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Check for problematic imports
            if 'from ..config import' in content:
                errors.append(f"{py_file.relative_to(worker_dir)}: Still has 'from ..config import'")
            if 'from dataflow_worker.config import' in content:
                errors.append(f"{py_file.relative_to(worker_dir)}: Still has 'from dataflow_worker.config import'")
            if 'from dataflow_common.' in content:
                errors.append(f"{py_file.relative_to(worker_dir)}: Still has 'from dataflow_common.'")
                
        except Exception as e:
            errors.append(f"{py_file}: Error reading: {e}")
    
    if errors:
        print("⚠️ Found issues:")
        for error in errors:
            print(f"  - {error}")
    else:
        print("✅ All imports look correct!")

if __name__ == "__main__":
    print("=" * 60)
    print("Import Fixer for Dataflow Refactoring")
    print("=" * 60)
    
    fix_all_imports()
    verify_imports()
    
    print("\n" + "=" * 60)
    print("Done! Now run: python test_refactor.py")
    print("=" * 60)
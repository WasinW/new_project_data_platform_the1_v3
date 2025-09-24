import os 
import sys 

print("Current PYTHONPATH:", os.path.abspath(__file__))
print((os.path.dirname(os.path.abspath(__file__))))
print(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
print(sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

print("==========================")
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
print(f"project_root: {project_root}")
dataflow_common_path = os.path.join(project_root, 'packages', 'dataflow-common')
print(sys.path.insert(0, os.path.join(project_root, 'packages')))
print(f"dataflow_common_path: {dataflow_common_path}")
print("==========================")

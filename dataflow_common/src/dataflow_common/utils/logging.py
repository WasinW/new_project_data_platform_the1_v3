# dataflow_common/src/dataflow_common/utils/logging.py
"""Simple logging utility ที่ work ใน Dataflow workers"""

import json
import sys
from datetime import datetime

class DataflowLogger:
    """Logger ที่ print JSON format - Dataflow จะ pick up แน่นอน"""
    
    @staticmethod
    def log(level: str, step: str, message: str, **kwargs):
        """
        Log as structured JSON to stdout - Dataflow จะจับเป็น jsonPayload
        
        Args:
            level: INFO, DEBUG, WARNING, ERROR
            step: ชื่อ step เช่น "ReadBQQuery_mapping_rows"
            message: ข้อความที่จะ log
            **kwargs: ข้อมูลเพิ่มเติมที่จะใส่ใน log
        """
        log_entry = {
            "severity": level,
            "timestamp": datetime.utcnow().isoformat(),
            "step": step,
            "message": message,
            **kwargs  # Extra fields
        }
        
        # Print as JSON to stdout - Dataflow จะจับเป็น structured log
        print(json.dumps(log_entry), file=sys.stdout, flush=True)
    
    @classmethod
    def info(cls, step: str, message: str, **kwargs):
        cls.log("INFO", step, message, **kwargs)
    
    @classmethod
    def debug(cls, step: str, message: str, **kwargs):
        cls.log("DEBUG", step, message, **kwargs)
    
    @classmethod
    def error(cls, step: str, message: str, **kwargs):
        cls.log("ERROR", step, message, **kwargs)

# Alias ให้ใช้ง่ายๆ
logger = DataflowLogger()
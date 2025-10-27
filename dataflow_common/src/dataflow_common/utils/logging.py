# dataflow_common/src/dataflow_common/utils/logging.py
"""Unified logging utility for Dataflow workers"""

import json
import sys
import inspect
from datetime import datetime

class DataflowLogger:
    """Enhanced logger ที่ work ทั้งใน steps และ modules อื่นๆ"""
    
    @staticmethod
    def log(level: str, message: str, step: str = None, **kwargs):
        """
        Log as structured JSON - รองรับทั้งมี/ไม่มี step
        
        Args:
            level: INFO, DEBUG, WARNING, ERROR
            message: ข้อความที่จะ log
            step: (Optional) ชื่อ step
            **kwargs: ข้อมูลเพิ่มเติม
        """
        # Auto-detect caller if step not provided
        if not step:
            frame = inspect.currentframe()
            if frame and frame.f_back:
                caller = frame.f_back
                module = caller.f_globals.get('__name__', 'unknown')
                function = caller.f_code.co_name
                step = f"{module}.{function}"
        
        log_entry = {
            "severity": level,
            "timestamp": datetime.utcnow().isoformat(),
            "message": message,
            **kwargs
        }
        
        # Add step only if available
        if step:
            log_entry["step"] = step
        
        # Print as JSON to stdout
        print(json.dumps(log_entry), file=sys.stdout, flush=True)
    
    @classmethod
    def info(cls, *args, **kwargs):
        """
        Flexible info logging
        Usage:
            logger.info("message")  # No step
            logger.info("step_name", "message")  # With step
            logger.info("message", extra_field="value")  # With kwargs
        """
        if len(args) == 1:
            # Only message
            cls.log("INFO", args[0], **kwargs)
        elif len(args) == 2:
            # Step and message
            cls.log("INFO", args[1], step=args[0], **kwargs)
        else:
            cls.log("INFO", str(args), **kwargs)
    
    @classmethod
    def debug(cls, *args, **kwargs):
        if len(args) == 1:
            cls.log("DEBUG", args[0], **kwargs)
        elif len(args) == 2:
            cls.log("DEBUG", args[1], step=args[0], **kwargs)
        else:
            cls.log("DEBUG", str(args), **kwargs)
    
    @classmethod
    def warning(cls, *args, **kwargs):
        if len(args) == 1:
            cls.log("WARNING", args[0], **kwargs)
        elif len(args) == 2:
            cls.log("WARNING", args[1], step=args[0], **kwargs)
        else:
            cls.log("WARNING", str(args), **kwargs)
    
    @classmethod
    def error(cls, *args, **kwargs):
        if len(args) == 1:
            cls.log("ERROR", args[0], **kwargs)
        elif len(args) == 2:
            cls.log("ERROR", args[1], step=args[0], **kwargs)
        else:
            cls.log("ERROR", str(args), **kwargs)

# Global logger instance
logger = DataflowLogger()

# Compatibility: สร้าง Python logger ที่ redirect ไป DataflowLogger
import logging

class DataflowHandler(logging.Handler):
    """Handler ที่ redirect Python logging ไป DataflowLogger"""
    
    def emit(self, record):
        level = record.levelname
        message = self.format(record)
        module = record.name
        
        DataflowLogger.log(
            level=level,
            message=message,
            step=f"{module}.{record.funcName}",
            line=record.lineno
        )

# Setup root logger ให้ใช้ DataflowHandler
def setup_logging():
    """Setup all loggers to use DataflowLogger"""
    root = logging.getLogger()
    root.handlers = []  # Clear existing handlers
    
    handler = DataflowHandler()
    handler.setFormatter(logging.Formatter('%(message)s'))
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    
    return logger

# Auto-setup when imported
logger = setup_logging()
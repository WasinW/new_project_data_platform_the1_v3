"""
Simple logging for Dataflow workers
"""
import logging
import os

def get_worker_logger(name):
    """
    Get logger for Dataflow workers
    Logs จะไปที่ Cloud Logging Explorer โดยอัตโนมัติ
    
    Args:
        name: Logger name (จะเป็น prefix ใน Cloud Logging)
    
    Returns:
        Logger instance
    """
    logger = logging.getLogger(name)
    
    # Set level from env or default
    level = os.environ.get('DATAFLOW_LOG_LEVEL', 'INFO')
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    
    # ไม่ต้อง add handler! Dataflow จัดการให้แล้ว
    # Handlers จะทำให้ log ซ้ำ
    
    return logger

# Helper function
def log_worker_info(logger, message, **kwargs):
    """Helper to add worker info to logs"""
    import socket
    worker_id = socket.gethostname()
    logger.info(f"[{worker_id}] {message}", **kwargs)
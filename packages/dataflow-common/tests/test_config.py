# tests/test_config.py
import pytest
from dataflow_common import CommonPipelineConfig

def test_common_pipeline_config():
    config = CommonPipelineConfig(
        project_id='test-project',
        custom_param='value'
    )
    assert config.project_id == 'test-project'
    assert config.get('custom_param') == 'value'
    assert config.get('missing', 'default') == 'default'
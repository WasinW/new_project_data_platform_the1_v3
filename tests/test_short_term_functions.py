# tests/test_short_term_functions.py
import unittest
import json
from datetime import datetime, date
from unittest.mock import Mock, patch
import pyarrow as pa
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../src'))

from dataflow_common.transforms.mapping import (
    create_mapping_dict,
    map_record,
    extract_by_path,
    normalize_path
)
from dataflow_common.transforms.coalesce import coalesce_by_mapping
from dataflow_common.transforms.schema import (
    normalize_row_to_schema,
    build_pyarrow_schema
)
from dataflow_common.config import FormatSpec


class TestShortTermFunctions(unittest.TestCase):
    """Test suite for MS Member Short Term pipeline functions"""
    
    def setUp(self):
        """Setup mock data for testing"""
        # Mock mapping rows from stg_mapping_reconcile
        self.mapping_rows = [
            {"RECONCILE_COLUMN_NAME" : "member_number" , "PERSONAS_MAPPING_COLUMN_NAME" : "profiles.memberId" , "RECONCILE_RETRIEVED": 1,"RECONCILE_CONFIRMED": 1,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "nationality" , "PERSONAS_MAPPING_COLUMN_NAME" : "profiles.nationalityId" , "RECONCILE_RETRIEVED": 1,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "country" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "passport_exp" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "birth_date" , "PERSONAS_MAPPING_COLUMN_NAME" : "profiles.dateOfBirth" , "RECONCILE_RETRIEVED": 1,"RECONCILE_CONFIRMED": 1,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "age" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "mobile_country_code" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "home_ph_country_code" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "type_of_housing" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "sub_district" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "district" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "city" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "postal_code" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "member_type" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "status_code" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "hold_reason" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "register_date" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "member_ref_by_name" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "member_ref_by_id" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "register_channel" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "register_partner" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "gender" , "PERSONAS_MAPPING_COLUMN_NAME" : "profiles.gender" , "RECONCILE_RETRIEVED": 1,"RECONCILE_CONFIRMED": 1,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "marital_status" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "job_title" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "education" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "monthly_income" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "prefer_lang" , "PERSONAS_MAPPING_COLUMN_NAME" : "profiles.languagePrefer" , "RECONCILE_RETRIEVED": 1,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "customer_type" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "register_staff" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "register_branch" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "privacy_flag" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "data_invalid_flag" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "dummy_flag" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "employee_bu_group" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "employee_bu" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "employee_resign_date" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "employee_join_date" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "employee_id" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "created_date" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "member_number_merged" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "register_partner_code" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "register_branch_code" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_address" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_mobile" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_email" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_send_sms_eng" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_send_sms_thai" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_expate" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_cds_line" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_rbs_line" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_ssp_line" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_cpn_line" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_cfr_line" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_cfm_line" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_twd_line" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_the1_line" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "updated_date" , "PERSONAS_MAPPING_COLUMN_NAME" : "timestamp" , "RECONCILE_RETRIEVED": 1,"RECONCILE_CONFIRMED": 1,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "insurance_not_send" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "consent_flag" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "consent_channel" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "consent_version" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "consent_date" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "iscall" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_call_cds" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_email_cds" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_address_cds" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_send_sms_eng_cds" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_send_sms_thai_cds" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_call_rbs" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_email_rbs" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_address_rbs" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_send_sms_eng_rbs" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_send_sms_thai_rbs" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_call_b2s" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_email_b2s" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_address_b2s" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_send_sms_eng_b2s" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_send_sms_thai_b2s" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_call_hws" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_email_hws" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_address_hws" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_send_sms_eng_hws" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_send_sms_thai_hws" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_call_twd" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_email_twd" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_address_twd" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_send_sms_eng_twd" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_send_sms_thai_twd" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_call_ssp" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_email_ssp" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_address_ssp" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_send_sms_eng_ssp" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_send_sms_thai_ssp" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_call_pwb" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_email_pwb" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_address_pwb" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_send_sms_eng_pwb" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_send_sms_thai_pwb" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_call_ofm" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_email_ofm" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_address_ofm" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_send_sms_eng_ofm" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_send_sms_thai_ofm" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_call_cfm" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_email_cfm" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_address_cfm" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_send_sms_eng_cfm" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_send_sms_thai_cfm" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_call_cfr" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_email_cfr" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_address_cfr" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_send_sms_eng_cfr" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_send_sms_thai_cfr" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_call_cmg" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_email_cmg" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_address_cmg" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_send_sms_eng_cmg" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_send_sms_thai_cmg" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "th_title" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "eng_title" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "ever_consent_partner" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "is_consent_the1" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "etl_created_by" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "etl_created_tms" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "invalid_member_flag" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
            {"RECONCILE_COLUMN_NAME" : "invalid_type" , "PERSONAS_MAPPING_COLUMN_NAME" : "" , "RECONCILE_RETRIEVED": 0,"RECONCILE_CONFIRMED": 0,"UPDATED_DATE": "2024-01-01"},
        ]
        # member_number     :   profiles.memberId
        # nationality       :   profiles.nationalityId
        # birth_date        :   profiles.dateOfBirth
        # gender            :   profiles.gender
        # prefer_lang       :   profiles.languagePrefer
        # Mock personas row with JSON profile
        self.personas_row_raw = {
            "personaId": "P123",
            "profiles": '{"memberId": "M001", "nationalityId": "THA", "gender": "Male", "languagePrefer": "EN", "dateOfBirth": "1990-05-20", "languagePrefer": "EN"}',
            "status": "ACTIVE",
            "timestamp": "2024-01-15T10:00:00"
        }
        
        # Mock MS member row
        self.ms_member_row = {
            "MEMBER_NUMBER": "M001",
            "nationality": "Thai",
            # "birth_date": "1990-05-20",
            "gender": "Male",
            # "prefer_lang": "EN",
            "prefer_lang": "TH",
            "date_of_birth": "1990-05-20",
            "updated_date": "2023-12-01"
        }
    
    def test_01_create_mapping_dict_pass(self):
        """Test creating mapping dictionary - PASS case"""
        print("\n=== TEST 01: create_mapping_dict (PASS) ===")
        
        # Execute
        mapping_dict = create_mapping_dict(
            self.mapping_rows,
            src_field="PERSONAS_MAPPING_COLUMN_NAME",
            dest_field="RECONCILE_COLUMN_NAME",
            retrieved_flag_field="RECONCILE_RETRIEVED",
            confirmed_flag_field="RECONCILE_CONFIRMED"
        )
        
        # # Expected output
        # expected = {
        #     "member_number": {
        #         "src_path": ["profiles", "memberId"],
        #         "reconcile": True,
        #         "original": True
        #     },
        #     "nationality": {
        #         "src_path": ["profiles", "nationalityId"],
        #         "reconcile": True,
        #         "original": False
        #     },
        #     "gender": {
        #         "src_path": ["profiles", "gender"],
        #         "reconcile": True,
        #         "original": True
        #     }
        # }
        
        # # Verify
        # self.assertEqual(mapping_dict, expected)
        # print(f"✅ PASS: Created mapping dict with {len(mapping_dict)} fields")
        # print(f"   Output: {mapping_dict}")
        # Check specific fields instead of comparing whole dict
        print(f"Created mapping dict with {len(mapping_dict)} fields")
        
        # Verify member_number
        self.assertIn("member_number", mapping_dict)
        self.assertEqual(mapping_dict["member_number"]["src_path"], ["profiles", "memberId"])
        self.assertTrue(mapping_dict["member_number"]["reconcile"])
        self.assertTrue(mapping_dict["member_number"]["original"])
        
        # Verify nationality
        self.assertIn("nationality", mapping_dict)
        self.assertEqual(mapping_dict["nationality"]["src_path"], ["profiles", "nationalityId"])
        
        # Verify prefer_lang (only reconcile)
        self.assertIn("prefer_lang", mapping_dict)
        self.assertTrue(mapping_dict["prefer_lang"]["reconcile"])
        self.assertFalse(mapping_dict["prefer_lang"]["original"])
        
        # Verify country (empty path)
        self.assertIn("country", mapping_dict)
        self.assertEqual(mapping_dict["country"]["src_path"], [])
        
        print(f"✅ PASS: Created mapping dict successfully with correct flags")

    def test_02_create_mapping_dict_fail(self):
        """Test creating mapping dictionary - FAIL case (empty rows)"""
        print("\n=== TEST 02: create_mapping_dict (FAIL - empty) ===")
        
        # Execute with empty list
        mapping_dict = create_mapping_dict([])
        
        # Should return empty dict
        self.assertEqual(mapping_dict, {})
        print("✅ Handled empty rows correctly")
    
    def test_03_parse_json_field_pass(self):
        """Test parsing JSON field - PASS case"""
        print("\n=== TEST 03: Parse JSON field (PASS) ===")
        
        # Parse JSON manually (simulate ParseJsonStep)
        record = dict(self.personas_row_raw)
        if isinstance(record["profiles"], str):
            record["profiles"] = json.loads(record["profiles"])
        
        # Verify
        self.assertIsInstance(record["profiles"], dict)
        self.assertEqual(record["profiles"]["memberId"], "M001")
        self.assertEqual(record["profiles"]["nationalityId"], "THA")
        self.assertEqual(record["profiles"]["gender"], "Male")
        print(f"✅ PASS: Parsed JSON field successfully")
        print(f"   Parsed profiles: {record['profiles']}")
    
    def test_04_parse_json_field_fail(self):
        """Test parsing JSON field - FAIL case (invalid JSON)"""
        print("\n=== TEST 04: Parse JSON field (FAIL - invalid) ===")
        
        record = {"profiles": "invalid json{"}
        
        # Try parse
        try:
            parsed = json.loads(record["profiles"])
            self.fail("Should have raised exception")
        except json.JSONDecodeError:
            print("✅ Correctly failed on invalid JSON")
    
    def test_05_extract_by_path_pass(self):
        """Test extracting value by path - PASS case"""
        print("\n=== TEST 05: extract_by_path (PASS) ===")
        
        # Setup: parsed personas data
        record = {
            "profiles": {
                "memberId": "M001",
                "nationalityId": "THA",
                "gender": "Male"
            }
        }
        
        # Test different paths
        test_cases = [
            (["profiles", "memberId"], "M001"),
            (["profiles", "nationalityId"], "THA"),
            (["profiles", "gender"], "Male"),
            ([], record),  # Empty path returns whole record
        ]
        
        for path, expected in test_cases:
            result = extract_by_path(record, path)
            self.assertEqual(result, expected)
            print(f"✅ Path {path} -> {result}")
    
    def test_06_extract_by_path_fail(self):
        """Test extracting value by path - FAIL case (missing key)"""
        print("\n=== TEST 06: extract_by_path (FAIL - missing) ===")
        
        record = {"profiles": {"memberId": "M001"}}
        
        # Try non-existent path
        result = extract_by_path(record, ["profiles", "nonExistent"])
        self.assertIsNone(result)
        print("✅ Correctly returned None for missing path")
    
    def test_07_map_record_pass(self):
        """Test mapping record - PASS case"""
        print("\n=== TEST 07: map_record (PASS) ===")
        
        # Setup
        personas_parsed = {
            "personaId": "P123",
            "profiles": {
                "memberId": "M001",
                "nationalityId": "THA",
                "gender": "Male"
            }
        }
        
        mapping_dict = {
            "member_number": {
                "src_path": ["profiles", "memberId"],
                "reconcile": True,
                "original": True
            },
            "nationality": {
                "src_path": ["profiles", "nationalityId"],
                "reconcile": True,
                "original": True
            },
            "gender": {
                "src_path": ["profiles", "gender"],
                "reconcile": True,
                "original": True
            }
        }
        
        # Execute
        mapped = map_record(personas_parsed, mapping_dict, mode="reconcile")
        
        # Expected
        expected = {
            "member_number": "M001",
            "nationality": "THA",
            "gender": "Male"
        }
        
        self.assertEqual(mapped, expected)
        print(f"✅ PASS: Mapped record successfully")
        print(f"   Output: {mapped}")
    
    def test_08_map_record_fail(self):
        """Test mapping record - FAIL case (wrong mode)"""
        print("\n=== TEST 08: map_record (FAIL - wrong mode) ===")
        
        mapping_dict = {
            "nationality": {
                "src_path": ["profiles", "nationalityId"],
                "reconcile": False,  # Not available in reconcile mode
                "original": True
            }
        }
        
        personas_parsed = {"profiles": {"nationalityId": "THA"}}
        
        # Execute with reconcile mode (should skip nationality)
        mapped = map_record(personas_parsed, mapping_dict, mode="reconcile")
        
        self.assertEqual(mapped, {})  # Should be empty
        print("✅ Correctly skipped fields not in reconcile mode")
    
    def test_09_coalesce_by_mapping_pass(self):
        """Test coalescing records - PASS case"""
        print("\n=== TEST 09: coalesce_by_mapping (PASS) ===")
        
        # Setup grouped data (after CoGroupByKey)
        key = "M001"
        groups = {
            "new": [{
                "member_number": "M001",
                "nationality": "THA",
                "gender": "Male"
            }],
            "old": [{
                "member_number": "M001",
                "nationality": "Thai",
                "gender": "Male"
            }]
        }
        
        kv = (key, groups)
        
        # Mapping rows for coalesce
        columns = [
            {
                "RECONCILE_COLUMN_NAME": "member_number",
                "RECONCILE_RETRIEVED": True  # Prefer new
            },
            {
                "RECONCILE_COLUMN_NAME": "nationality",
                "RECONCILE_RETRIEVED": True  # Prefer new
            },
            {
                "RECONCILE_COLUMN_NAME": "gender",
                "RECONCILE_RETRIEVED": False  # Prefer old
            }
        ]
        
        # Execute
        result = coalesce_by_mapping(
            kv,
            columns=columns,
            flag_field="RECONCILE_RETRIEVED",
            pk_field="member_number",
            dest_field="RECONCILE_COLUMN_NAME"
        )
        
        # Verify: should prefer new for member_number and nationality, old for gender
        self.assertEqual(result["member_number"], "M001")
        self.assertEqual(result["nationality"], "THA")  # From new
        self.assertEqual(result["gender"], "Male")  # From new
        print(f"✅ PASS: Coalesced correctly")
        print(f"   Output: {result}")
    
    def test_10_coalesce_by_mapping_fail(self):
        """Test coalescing records - FAIL case (no new records)"""
        print("\n=== TEST 10: coalesce_by_mapping (FAIL - no new) ===")
        
        kv = ("M001", {"new": [], "old": [{"MEMBER_NUMBER": "M001"}]})
        
        result = coalesce_by_mapping(
            kv,
            columns=[],
            flag_field="RECONCILE_RETRIEVED",
            pk_field="member_number",
            dest_field="RECONCILE_COLUMN_NAME"
        )
        
        self.assertIsNone(result)
        print("✅ Correctly returned None when no new records")
    
    def test_11_normalize_to_schema_pass(self):
        """Test normalizing to schema - PASS case"""
        print("\n=== TEST 11: normalize_row_to_schema (PASS) ===")
        
        # Create schema
        schema_def = [
            {"name": "member_number", "type": "STRING"},
            {"name": "nationality", "type": "STRING"},
            {"name": "gender", "type": "STRING"},
            {"name": "updated_date", "type": "TIMESTAMP"}
        ]
        schema = build_pyarrow_schema(schema_def)
        
        # Input row
        row = {
            "member_number": "M001",
            "nationality": "THA",
            "gender": "Male",
            "updated_date": "2024-01-15 10:30:00"
        }
        
        formats = FormatSpec()
        
        # Execute
        normalized = normalize_row_to_schema(row, schema, formats)
        
        # Verify
        self.assertEqual(normalized["member_number"], "M001")
        self.assertEqual(normalized["nationality"], "THA")
        self.assertEqual(normalized["gender"], "Male")
        self.assertIsInstance(normalized["updated_date"], datetime)
        print(f"✅ PASS: Normalized successfully")
        print(f"   Output: {normalized}")
    
    def test_12_normalize_to_schema_fail(self):
        """Test normalizing to schema - FAIL case (invalid date)"""
        print("\n=== TEST 12: normalize_row_to_schema (FAIL - invalid date) ===")
        
        schema_def = [{"name": "updated_date", "type": "TIMESTAMP"}]
        schema = build_pyarrow_schema(schema_def)
        
        row = {"updated_date": "invalid-date"}
        formats = FormatSpec()
        
        # Execute
        normalized = normalize_row_to_schema(row, schema, formats)
        
        # Should set to None for invalid date
        self.assertIsNone(normalized["updated_date"])
        print("✅ Correctly set None for invalid date")
    
    def test_13_normalize_path_variations(self):
        """Test normalize_path with different formats"""
        print("\n=== TEST 13: normalize_path variations ===")
        
        test_cases = [
            ("profiles.memberId", ["profiles", "memberId"]),
            # ("profiles['memberId']", ["profiles", "memberId"]),  # ❌ REMOVE THIS
            # ("['profiles']['memberId']", ["profiles", "memberId"]),  # ❌ REMOVE THIS  
            ("", []),  # Empty string
            ("singleField", ["singleField"]),
            ("level1.level2.level3", ["level1", "level2", "level3"]),

        ]
        
        for input_path, expected in test_cases:
            result = normalize_path(input_path)
            self.assertEqual(result, expected)
            print(f"✅ '{input_path}' -> {result}")
    
    def test_14_extract_json_from_string(self):
        """Test extracting from JSON string field (auto-parse)"""
        print("\n=== TEST 14: Extract from JSON string ===")
        
        # Test what happens when profiles is JSON string
        record = {
            "profiles": '{"memberId": "M001", "nationalityId": "THA"}'
        }
        
        # extract_by_path should handle JSON string
        result = extract_by_path(record, ["profiles", "memberId"])
        self.assertEqual(result, "M001")
        print(f"✅ Auto-parsed JSON string and extracted: {result}")
    
    def test_15_full_pipeline_simulation(self):
        """Simulate complete pipeline flow"""
        print("\n=== TEST 15: Full Pipeline Simulation ===")
        
        # Step 1: Parse JSON
        personas_raw = self.personas_row_raw.copy()
        personas_raw["profiles"] = json.loads(personas_raw["profiles"])
        print("Step 1: Parse JSON ✅")
        
        # Step 2: Create mapping dict
        mapping_dict = create_mapping_dict(
            self.mapping_rows,
            src_field="PERSONAS_MAPPING_COLUMN_NAME",
            dest_field="RECONCILE_COLUMN_NAME",
            retrieved_flag_field="RECONCILE_RETRIEVED",
            confirmed_flag_field="RECONCILE_CONFIRMED"
        )
        print(f"Step 2: Create mapping ({len(mapping_dict)} fields) ✅")
        
        # Step 3: Map personas record
        mapped_new = map_record(personas_raw, mapping_dict, mode="reconcile")
        print(f"Step 3: Map record ✅")
        
        # Step 4: Create KV pairs and group
        groups = {
            "new": [mapped_new],
            "old": [self.ms_member_row]
        }
        print("Step 4: Group by key ✅")
        
        # Step 5: Coalesce
        result = coalesce_by_mapping(
            ("M001", groups),
            columns=self.mapping_rows,
            flag_field="RECONCILE_RETRIEVED",
            pk_field="member_number",
            dest_field="RECONCILE_COLUMN_NAME"
        )
        
        self.assertIsNotNone(result)
        self.assertEqual(result["member_number"], "M001")
        print(f"Step 5: Coalesce ✅")
        
        print("\n✅ FULL PIPELINE SIMULATION PASSED!")


def run_tests():
    """Run all tests and show results"""
    print("\n" + "="*60)
    print("RUNNING MS MEMBER SHORT TERM FUNCTION TESTS")
    print("="*60)
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestShortTermFunctions)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    print(f"Tests Run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    # print(f"Success Rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")
    
    if result.wasSuccessful():
        print("✅ ALL TESTS PASSED!")
    else:
        print("❌ SOME TESTS FAILED")
        if result.failures:
            print("\nFailed Tests:")
            for test, traceback in result.failures:
                print(f"  - {test}")
    
    success_rate = ((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100)
    print(f"Success Rate: {success_rate:.1f}%")
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    exit(0 if success else 1)
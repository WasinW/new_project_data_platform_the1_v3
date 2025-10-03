1. get param from argument 
2. validate param
3. switch SML case
For Short term 
4. Build Pipeline DataFlow 
    4.1 Define Variable 
    4.2 Define Function 
    4.3 Gen query 
5. Run Pipeline 
    5.1 Read Mapping From BQ with query Mapping (4.3)
        - mapping_rows_pc > mapping_rows_list > mapping_dict_side
    5.2 Read Origin Data From BQ with query Origin (4.3)
        - origin_data
    5.3 Read Source Data From BQ with query Source (4.3)
        - source_data 
    5.4 Map prepare column from source to structure 
        - source_data + mapping_dict_side > mapped_new
    5.5 Join Data origin + new : check new or update records/message
        - mapped_new + origin_data > joined
    5.6 Mapping Column from joined with mapping_rows_list and condition flag_field="RECONCILE_RETRIEVED"
        - joined + mapping_rows_list  > reconcile_rows
    5.7 Mapping Column from joined with mapping_rows_list and condition flag_field="RECONCILE_CONFIRMED"
        - joined + mapping_rows_list  > patch_origin_rows
    5.8 clean column and datatype from reconcile_rows 
        - reconcile_rows > reconcile_rows_casted
    5.9 clean column and datatype from patch_origin_rows 
        - patch_origin_rows > member_rows_casted
    5.10 Write reconcile_rows_casted to S3 
        - reconcile_rows_casted > s3
    5.11 Write member_rows_casted to S3 
        - member_rows_casted > s3
STEP
    - read bq  --> move function to common 
        - query
    - gen mapping --> not move 
    - split new and updated message --> not move 
    - mapping schemas and full fill data --> not move 
    - prepare target data type -- move to common
    - write to target 
        - s3
        - bq 

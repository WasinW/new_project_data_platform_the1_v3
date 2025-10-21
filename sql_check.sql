with personas As(
    SELECT * EXCEPT(RN_PK)
    FROM (
      SELECT 
      personaId
      -- , profiles
      , JSON_VALUE(profiles, '$.gender') as psn_gender
      , JSON_VALUE(profiles, '$.dateOfBirth') as psn_birth_date
      , timestamp as psn_updated_date
      , JSON_VALUE(profiles, '$.memberId') as psn_member_number
      , JSON_VALUE(profiles, '$.nationalityId') as psn_nationality
      , JSON_VALUE(profiles, '$.languagePrefer') as psn_prefer_lang
      , status
      , timestamp
      , ROW_NUMBER() OVER(
        PARTITION BY JSON_VALUE(profiles, '$.memberId')
        ORDER BY TIMESTAMP DESC
      ) AS RN_PK
      FROM `the1-insight-prod.insight.personas`
      WHERE TIMESTAMP <= TIMESTAMP('2025-10-08 00:00:00')
    ) WHERE RN_PK = 1
)

, ms_member as (
    SELECT DISTINCT 
      origin.gender as org_gender	
    , origin.birth_date as org_birth_date	
    , origin.updated_date as org_updated_date	
    , origin.member_number as org_member_number	
    , origin.nationality as org_nationality	
    , origin.prefer_lang as org_prefer_lang	

    FROM `the1-insight-dev.insight_dev.stg_ms_member` origin
    INNER JOIN (
      SELECT JSON_VALUE(profiles, '$.memberId') as member_id
      FROM `the1-insight-prod.insight.personas`
      WHERE TIMESTAMP <= TIMESTAMP('2025-10-08 00:00:00')
      GROUP BY JSON_VALUE(profiles, '$.memberId')
    ) new_members
    ON origin.MEMBER_NUMBER = new_members.member_id
)

, compared as (
select 
psn.psn_member_number
, case 
    when org.org_member_number is null then 'new'
    when org.org_member_number is not null and psn.psn_member_number is not null then 'update'
    -- when org.org_member_number is not null and psn.psn_member_number is not null then 'update'
    else '???'
  end as checked
, psn.psn_gender
, psn.psn_birth_date
, psn.psn_updated_date
, psn.psn_nationality
, psn.psn_prefer_lang
, org.org_member_number	
, org.org_gender	
, org.org_birth_date	
, org.org_updated_date	
, org.org_nationality	
, org.org_prefer_lang	
from personas as  psn 
left join  ms_member as org 
on psn.psn_member_number = org.org_member_number 
) 

select *
-- select checked ,count(*)
from compared 
group by 1 

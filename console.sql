/* 153055 having respiratory failure(ICD-9:518.81, 518.83, 518.84) */
select count(d.patientunitstayid)
from diagnosis as d
where position('518.81' in d.icd9code) = 1
   or position('518.83' in d.icd9code) = 1
   or position('518.84' in d.icd9code) = 1;

/* 153005 having respiratory failure(ICD-9:518.81, 518.83, 518.84)
   and missing congestive heart failure(ICD-9:428.0) */
select count(d.patientunitstayid)
from diagnosis as d
where (position('518.81' in d.icd9code) = 1
    or position('518.83' in d.icd9code) = 1
    or position('518.84' in d.icd9code) = 1)
  and position('428.0' in d.icd9code) = 0;

/* 105827 having respiratory failure(ICD-9:518.81, 518.83, 518.84),
   missing congestive heart failure(ICD-9:428.0),
   and having mechanical ventilation during ICU stay */
select d.patientunitstayid
from diagnosis as d
where (position('518.81' in d.icd9code) = 1
    or position('518.83' in d.icd9code) = 1
    or position('518.84' in d.icd9code) = 1)
  and position('428.0' in d.icd9code) = 0
  and d.patientunitstayid in
      (select patientunitstayid from respiratorycare);

/*  49625 PEEP >= 5 at least once */
select patientunitstayid
from (select p.patientunitstayid,
             min(labresult) as lab_min_peep
      from patient as p
               inner join lab le on p.patientunitstayid = le.patientunitstayid
          and le.labname = 'PEEP'
          and le.labresult is not null and le.labresult >= 5 and labresult <= 60
      GROUP BY p.patientunitstayid
      order by lab_min_peep) as lab_peep --24287
UNION
select patientunitstayid
from (select p.patientunitstayid,
             min(respchartvalue) as res_min_peep
      from patient as p
               inner join respiratorycharting r on p.patientunitstayid = r.patientunitstayid
          and r.respchartvaluelabel = 'PEEP'
          and r.respchartvalue is not null
          and position('%' in respchartvalue) = 0
          and CAST(respchartvalue as DECIMAL) >= 5
          and CAST(respchartvalue as DECIMAL) <= 60
      GROUP BY p.patientunitstayid
      order by res_min_peep) as respiratorycharting_peep;
--42514

/* 14726 having respiratory failure(ICD-9:518.81, 518.83, 518.84),
   missing congestive heart failure(ICD-9:428.0),
   having mechanical ventilation during ICU stay,
   and PEEP >= 5 at least once  */
DROP TABLE IF EXISTS public.ards_data CASCADE;
CREATE TABLE public.ards_data as
select temp.patientunitstayid,
       0 as valid_tag
from (select d.patientunitstayid
      from diagnosis as d
      where (position('518.81' in d.icd9code) = 1
          or position('518.83' in d.icd9code) = 1
          or position('518.84' in d.icd9code) = 1)
        and position('428.0' in d.icd9code) = 0
        and d.patientunitstayid in
            (select patientunitstayid from respiratorycare)
      INTERSECT
      (select patientunitstayid
       from (select p.patientunitstayid,
                    min(labresult) as lab_min_peep
             from patient as p
                      inner join lab le on p.patientunitstayid = le.patientunitstayid
                 and le.labname = 'PEEP'
                 and le.labresult is not null and le.labresult >= 5 and labresult <= 60
             GROUP BY p.patientunitstayid
             order by lab_min_peep) as lab_peep --24287
       UNION
       select patientunitstayid
       from (select p.patientunitstayid,
                    min(respchartvalue) as res_min_peep
             from patient as p
                      inner join respiratorycharting r on p.patientunitstayid = r.patientunitstayid
                 and r.respchartvaluelabel = 'PEEP'
                 and r.respchartvalue is not null
                 and position('%' in respchartvalue) = 0
                 and CAST(respchartvalue as DECIMAL) >= 5
                 and CAST(respchartvalue as DECIMAL) <= 60
             GROUP BY p.patientunitstayid
             order by res_min_peep) as respiratorycharting_peep)) as temp;

/* get pao2, fio2 and  peep info */
select le.patientunitstayid,
       le.labresultoffset as resultoffset,
       le.labname         as name,
       le.labresult       as value
from lab as le
where le.patientunitstayid = 3181349
  and le.labname in
      ('paO2', 'FiO2', 'PEEP')
  and le.labresult is not null
  and le.labresult > 0
UNION
select res.patientunitstayid,
       res.respchartoffset                 as resultoffset,
       res.respchartvaluelabel             as name,
       CAST(res.respchartvalue as DECIMAL) as value
from respiratorycharting as res
where res.patientunitstayid = 3181349
  and res.respchartvaluelabel in
      ('FiO2', 'PEEP')
  and res.respchartvalue is not null
  and position('%' in res.respchartvalue) = 0;


------------------------------------------------------------------------------------------------------------------------
/* find columns */

-- from patient
select gender,
       age,
       apacheadmissiondx,
       admissionheight,
       admissionweight,
       hospitaladmitsource,
       unitdischargeoffset,
       unitdischargestatus,
       hospitaldischargeoffset,
       hospitaldischargestatus
from patient
where ;


-- from apachepatientresult
select patientunitstayid, apachescore
from apachepatientresult
limit 100;


-- indicator from drugname
select patientunitstayid, infusionoffset, drugname
from infusiondrug
where infusionoffset <= 1440
  and (
            drugname like 'vasopressin%'
        or drugname like '%dobutamine%'
        or drugname like '%dopamine%'
        or drugname like '%epinephrine%'
        or drugname like '%norepinephrine%'
        or drugname like '%phenylephrine%'
        or drugname like '%vasopressin%'
        or drugname like '%warfarin%'
        or drugname like '%heparin%'
        or drugname like '%milrinone%'
    )
limit 100;


-- from lab
select count(*)
from lab
where labname in (
                  'albumin', '-bands', 'WBC x 1000', 'bilirubin', 'platelets x 1000', 'BUN', 'bicarbonate',
                  'creatinine', 'potassium', 'PTT', 'sodium', 'PT - INR', 'glucose',
                  'paO2', 'paCO2', 'pH', 'FiO2', 'Base Excess', 'PEEP', 'FiO2', 'calcium',
                  'Total CO2', 'ALT (SGPT)', 'AST (SGOT)', 'lactate', 'Hct', 'Hgb',
    );
-- ALP
-- Basos
-- EOs
-- Magnesium
-- SaO2


-- from nursecharting
-- pivoted-gcs.sql
select patientunitstayid
        ,
       nursingchartoffset,
       nursingchartcelltypevallabel,
       nursingchartvalue
from nursecharting
where nursingchartcelltypecat in
      ('Scores', 'Other Vital Signs and Infusions')
  and nursingchartcelltypevallabel in
      ('Temperature', 'SpO2', 'Respiration rate', 'Glasgow coma score', 'Score (Glasgow Coma Scale)')
  and nursingchartvalue is not null
  and nursingchartvalue != ''
  and nursingchartoffset <= 1440
limit 100;

-- from respiratoryCharting
select patientunitstayid, respchartoffset, respchartvaluelabel, respchartvalue
from respiratorycharting
where respchartvaluelabel in (
                              'Plateau Pressure', 'Peak Insp. Pressure', 'Mean Airway Pressure', 'PEEP', 'TV/kg IBW'
    )
  and respchartoffset <= 1440
limit 100;

-- from vitalaperiodic
select patientunitstayid, observationoffset, nonInvasiveSystolic, noninvasivediastolic, noninvasivemean
from vitalaperiodic
where observationoffset <= 1440
limit 100;

-- from vitalperiodic
select patientunitstayid,
       observationoffset,
       heartrate,
       cvp,
       etco2,
       systemicsystolic,
       systemicdiastolic,
       systemicmean,
       pasystolic,
       padiastolic,
       pamean
from vitalperiodic
where observationoffset <= 1440
limit 10;/* 153055 having respiratory failure(ICD-9:518.81, 518.83, 518.84) */
select count(d.patientunitstayid)
from diagnosis as d
where position('518.81' in d.icd9code) = 1
   or position('518.83' in d.icd9code) = 1
   or position('518.84' in d.icd9code) = 1;

/* 153005 having respiratory failure(ICD-9:518.81, 518.83, 518.84)
   and missing congestive heart failure(ICD-9:428.0) */
select count(d.patientunitstayid)
from diagnosis as d
where (position('518.81' in d.icd9code) = 1
    or position('518.83' in d.icd9code) = 1
    or position('518.84' in d.icd9code) = 1)
  and position('428.0' in d.icd9code) = 0;

/* 105827 having respiratory failure(ICD-9:518.81, 518.83, 518.84),
   missing congestive heart failure(ICD-9:428.0),
   and having mechanical ventilation during ICU stay */
select d.patientunitstayid
from diagnosis as d
where (position('518.81' in d.icd9code) = 1
    or position('518.83' in d.icd9code) = 1
    or position('518.84' in d.icd9code) = 1)
  and position('428.0' in d.icd9code) = 0
  and d.patientunitstayid in
      (select patientunitstayid from respiratorycare);

/*  49625 PEEP >= 5 at least once */
select patientunitstayid
from (select p.patientunitstayid,
             min(labresult) as lab_min_peep
      from patient as p
               inner join lab le on p.patientunitstayid = le.patientunitstayid
          and le.labname = 'PEEP'
          and le.labresult is not null and le.labresult >= 5 and labresult <= 60
      GROUP BY p.patientunitstayid
      order by lab_min_peep) as lab_peep --24287
UNION
select patientunitstayid
from (select p.patientunitstayid,
             min(respchartvalue) as res_min_peep
      from patient as p
               inner join respiratorycharting r on p.patientunitstayid = r.patientunitstayid
          and r.respchartvaluelabel = 'PEEP'
          and r.respchartvalue is not null
          and position('%' in respchartvalue) = 0
          and CAST(respchartvalue as DECIMAL) >= 5
          and CAST(respchartvalue as DECIMAL) <= 60
      GROUP BY p.patientunitstayid
      order by res_min_peep) as respiratorycharting_peep;
--42514

/* 14726 having respiratory failure(ICD-9:518.81, 518.83, 518.84),
   missing congestive heart failure(ICD-9:428.0),
   having mechanical ventilation during ICU stay,
   and PEEP >= 5 at least once  */
DROP TABLE IF EXISTS public.ards_data CASCADE;
CREATE TABLE public.ards_data as
select temp.patientunitstayid,
       0 as valid_tag
from (select d.patientunitstayid
      from diagnosis as d
      where (position('518.81' in d.icd9code) = 1
          or position('518.83' in d.icd9code) = 1
          or position('518.84' in d.icd9code) = 1)
        and position('428.0' in d.icd9code) = 0
        and d.patientunitstayid in
            (select patientunitstayid from respiratorycare)
      INTERSECT
      (select patientunitstayid
       from (select p.patientunitstayid,
                    min(labresult) as lab_min_peep
             from patient as p
                      inner join lab le on p.patientunitstayid = le.patientunitstayid
                 and le.labname = 'PEEP'
                 and le.labresult is not null and le.labresult >= 5 and labresult <= 60
             GROUP BY p.patientunitstayid
             order by lab_min_peep) as lab_peep --24287
       UNION
       select patientunitstayid
       from (select p.patientunitstayid,
                    min(respchartvalue) as res_min_peep
             from patient as p
                      inner join respiratorycharting r on p.patientunitstayid = r.patientunitstayid
                 and r.respchartvaluelabel = 'PEEP'
                 and r.respchartvalue is not null
                 and position('%' in respchartvalue) = 0
                 and CAST(respchartvalue as DECIMAL) >= 5
                 and CAST(respchartvalue as DECIMAL) <= 60
             GROUP BY p.patientunitstayid
             order by res_min_peep) as respiratorycharting_peep)) as temp;

/* get pao2, fio2 and  peep info */
select le.patientunitstayid,
       le.labresultoffset as resultoffset,
       le.labname         as name,
       le.labresult       as value
from lab as le
where le.patientunitstayid = 3181349
  and le.labname in
      ('paO2', 'FiO2', 'PEEP')
  and le.labresult is not null
  and le.labresult > 0
UNION
select res.patientunitstayid,
       res.respchartoffset                 as resultoffset,
       res.respchartvaluelabel             as name,
       CAST(res.respchartvalue as DECIMAL) as value
from respiratorycharting as res
where res.patientunitstayid = 3181349
  and res.respchartvaluelabel in
      ('FiO2', 'PEEP')
  and res.respchartvalue is not null
  and position('%' in res.respchartvalue) = 0;


------------------------------------------------------------------------------------------------------------------------
/* find columns */

-- from patient
select patientunitstayid,
       gender,
       age,
       apacheadmissiondx,
       (case
            when admissionheight is not null and admissionweight is not null
                then admissionweight / (admissionheight * admissionheight * 0.0001)
            else NULL end) as bmi,
       hospitaladmitsource,
       hospitaldischargeoffset,
       unitdischargestatus,
       hospitaldischargeoffset,
       hospitaldischargestatus
from patient
where unitdischargestatus not in ('Alive', 'Expired')
limit 100;


-- from apachepatientresult
select apachescore
from apachepatientresult
limit 100;


-- indicator from drugname
select vasopressor_indicator,
       dobutamine_indicator,
       dopamine_indicator,
       epinephrine_indicator,
       norepinephrine_indicator,
       phenylephrine_indicator,
       vasopressin_indicator,
       warfarin_indicator,
       heparin_indicator,
       milrinone_indicator
from (select patientunitstayid,
             max(case when drugname like 'vasopressor%' then 1 else 0 end)    as vasopressor_indicator,
             max(case when drugname like 'dobutamine%' then 1 else 0 end)     as dobutamine_indicator,
             max(case when drugname like 'dopamine%' then 1 else 0 end)       as dopamine_indicator,
             max(case when drugname like 'epinephrine%' then 1 else 0 end)    as epinephrine_indicator,
             max(case when drugname like 'norepinephrine%' then 1 else 0 end) as norepinephrine_indicator,
             max(case when drugname like 'phenylephrine%' then 1 else 0 end)  as phenylephrine_indicator,
             max(case when drugname like 'vasopressin%' then 1 else 0 end)    as vasopressin_indicator,
             max(case when drugname like 'warfarin%' then 1 else 0 end)       as warfarin_indicator,
             max(case when drugname like 'heparin%' then 1 else 0 end)        as heparin_indicator,
             max(case when drugname like 'milrinone%' then 1 else 0 end)      as milrinone_indicator
      from infusiondrug
      where infusionoffset <= 1440
        and (
                  drugname like 'vasopressin%'
              or drugname like '%dobutamine%'
              or drugname like '%dopamine%'
              or drugname like '%epinephrine%'
              or drugname like '%norepinephrine%'
              or drugname like '%phenylephrine%'
              or drugname like '%vasopressin%'
              or drugname like '%warfarin%'
              or drugname like '%heparin%'
              or drugname like '%milrinone%'
          )
      group by patientunitstayid) as drug_indicator
where patientunitstayid = 1646201
order by heparin_indicator desc
limit 100;


select *
from infusiondrug
where infusionoffset <= 1440
  and drugname like '%heparin%';

-- from lab
select count(*)
from lab
where labname in (
                  'albumin', '-bands', 'WBC x 1000', 'bilirubin', 'platelets x 1000', 'BUN', 'bicarbonate',
                  'creatinine', 'potassium', 'PTT', 'sodium', 'PT - INR', 'glucose',
                  'paO2', 'paCO2', 'pH', 'FiO2', 'Base Excess', 'PEEP', 'FiO2', 'calcium',
                  'Total CO2', 'ALT (SGPT)', 'AST (SGOT)', 'lactate', 'Hct', 'Hgb'
    );
-- ALP
-- Basos
-- EOs
-- Magnesium
-- SaO2


-- from nursecharting
-- pivoted-gcs.sql
select nursingchartoffset as offset, nursingchartcelltypevallabel, nursingchartvalue
from nursecharting
where nursingchartcelltypecat in
      ('Scores', 'Other Vital Signs and Infusions')
  and nursingchartcelltypevallabel in
      ('Temperature', 'SpO2', 'Respiration Rate', 'Glasgow coma score', 'Score (Glasgow Coma Scale)')
  and nursingchartvalue is not null
  and nursingchartvalue != ''
  and nursingchartoffset <= 1440
limit 100;

-- from respiratoryCharting
select respchartoffset as offset, respchartvaluelabel, respchartvalue
from respiratorycharting
where respchartvaluelabel in (
                              'Plateau Pressure', 'Peak Insp. Pressure', 'Mean Airway Pressure', 'PEEP', 'TV/kg IBW'
    )
  and respchartoffset <= 1440
limit 100;

-- from vitalaperiodic
select observationoffset as offset, nonInvasiveSystolic, noninvasivediastolic, noninvasivemean
from vitalaperiodic
where observationoffset <= 1440
limit 100;

-- from vitalperiodic
select observationoffset as offset,
       heartrate,
       cvp,
       etco2,
       systemicsystolic,
       systemicdiastolic,
       systemicmean,
       pasystolic,
       padiastolic,
       pamean
from vitalperiodic
where observationoffset <= 1440
limit 1000
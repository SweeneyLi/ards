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
from patient;


-- from apachepatientresult
select patientunitstayid, apachescore
from apachepatientresult
limit 100;


-- indicator from drugname

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


-- from lab
select count(*)
from lab
where labname in (
                  'albumin', '-bands', 'WBC x 1000', 'bilirubin', 'platelets x 1000', 'BUN', 'bicarbonate',
                  'creatinine', 'potassium', 'PTT', 'sodium', 'PT - INR', 'glucose',
                  'paO2', 'paCO2', 'pH', 'FiO2', 'Base Excess', 'PEEP', 'FiO2', 'calcium',
                  'Total CO2', 'ALT (SGPT)', 'AST (SGOT)', 'lactate', 'Hct', 'Hgb', '-eos', '-basos', 'magnesium',
                  'SaO2', 'ALP'
    );



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
where observationoffset <= 1440;
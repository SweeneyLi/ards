SET search_path to public, eicu_crd;

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

select distinct uniquepid from
(select ards_data.patientunitstayid as icu_stay_id,
       uniquepid,
       unitdischargeoffset,
       unitdischargestatus,
       hospitaldischargeoffset,
       hospitaldischargestatus
from ards_data
         left join patient p on ards_data.patientunitstayid = p.patientunitstayid
where not(unitdischargestatus = '' or (unitdischargestatus = 'Alive' and hospitaldischargestatus = ''))) as temp;
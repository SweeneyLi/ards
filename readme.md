# Utilizing machine learning to improve clinical trial design for acute respiratory distress syndrome
This is my data scripts of "[Utilizing machine learning to improve clinical trial design for acute respiratory distress syndrome](https://www.nature.com/articles/s41746-021-00505-5)" .

## Development Environment 

python3.7

specific python packages see the requirments.txt

## Dataset Source

[The eICU Collaborative Research Database, a freely available multi-center database for critical care research](https://www.nature.com/articles/sdata2018178)

## File Structure

1. config--config of database and data structure
2. data_process--base script of data processing
3. dataset--result of data process
4. static--static source of this script
5. filter_data.py--the script to filter data by select criterion of paper
6. get_data.py--the script to get data feature by select criterion of paper

## Use Procedure

You should download the origin dataset and load in postgres database firstly.

1. install python packages in the requirment.txt
2. change the db config in foler of config
3. change the output path in filter.py and run "python filter_data.py"
4. change the relate config in get_data.py and run "python get_data.py"
5. change the output path in get_figure_data.py and run "python get_figure_data.py"

## Figure Data Structure

### Figure 3
![](https://github.com/SweeneyLi/ards/raw/master/static/img/fig3.png)

| figure name  | Characterization of ARDS cohort.                             |
| ------------ | ------------------------------------------------------------ |
| explanation  | Four statistical sub figure need one kinds of data.          |
| data time    | 24h after ARDS identification                                |
| data format  | icu_stay_id,ards_group,ards_severity,<br />hospital_dead_status,28d_death_status,icu_death_status,<br />age,apachescore,hospital_los,icu_los,admission_diagnosis |
| data example | 351515,long stay,Severe,0,0,0,72,111.0,40.04,31.76,Sepsis    |

### Figure 4
![](https://github.com/SweeneyLi/ards/raw/master/static/img/fig4.png)
| figure name  | Top univariate associations with patient status 48 h after identification. |
| ------------ | ------------------------------------------------------------ |
| explanation  | Three univariate sub figure need one kinds of data.(58 dynamic features) |
| data time    | 24h after ARDS identification                                |
| data format  | ards_group,gender,age,BMI,apachescore,vasopressor_indicator,dobutamine_indicator,dopamine_indicator,epinephrine_indicator,norepinephrine_indicator,phenylephrine_indicator,vasopressin_indicator,warfarin_indicator,heparin_indicator,milrinone_indicator,AST (SGOT)_median,AST (SGOT)_variance,AST (SGOT)_rate_change,albumin_median,albumin_variance,albumin_rate_change,ALP_median,ALP_variance,ALP_rate_change,ALT (SGPT)_median,ALT (SGPT)_variance,ALT (SGPT)_rate_change,bands_median,bands_variance,bands_rate_change,Base Excess_median,Base Excess_variance,Base Excess_rate_change,basos_median,basos_variance,basos_rate_change,bicarbonate_median,bicarbonate_variance,bicarbonate_rate_change,BUN_median,BUN_variance,BUN_rate_change,calcium_median,calcium_variance,calcium_rate_change,creatinine_median,creatinine_variance,creatinine_rate_change,cvp_median,cvp_variance,cvp_rate_change,eos_median,eos_variance,eos_rate_change,etco2_median,etco2_variance,etco2_rate_change,FiO2_median,FiO2_variance,FiO2_rate_change,GCS Eyes_median,GCS Eyes_variance,GCS Eyes_rate_change,GCS Intub_median,GCS Intub_variance,GCS Intub_rate_change,GCS Motor_median,GCS Motor_variance,GCS Motor_rate_change,GCS Total_median,GCS Total_variance,GCS Total_rate_change,GCS Unable_median,GCS Unable_variance,GCS Unable_rate_change,GCS Verbal_median,GCS Verbal_variance,GCS Verbal_rate_change,glucose_median,glucose_variance,glucose_rate_change,Hct_median,Hct_variance,Hct_rate_change,heartrate_median,heartrate_variance,heartrate_rate_change,Hgb_median,Hgb_variance,Hgb_rate_change,ionized calcium_median,ionized calcium_variance,ionized calcium_rate_change,lactate_median,lactate_variance,lactate_rate_change,magnesium_median,magnesium_variance,magnesium_rate_change,Mean Airway Pressure_median,Mean Airway Pressure_variance,Mean Airway Pressure_rate_change,noninvasivediastolic_median,noninvasivediastolic_variance,noninvasivediastolic_rate_change,noninvasivemean_median,noninvasivemean_variance,noninvasivemean_rate_change,noninvasivesystolic_median,noninvasivesystolic_variance,noninvasivesystolic_rate_change,P/F ratio_median,P/F ratio_variance,P/F ratio_rate_change,paCO2_median,paCO2_variance,paCO2_rate_change,padiastolic_median,padiastolic_variance,padiastolic_rate_change,pamean_median,pamean_variance,pamean_rate_change,paO2_median,paO2_variance,paO2_rate_change,pasystolic_median,pasystolic_variance,pasystolic_rate_change,Peak Insp. Pressure_median,Peak Insp. Pressure_variance,Peak Insp. Pressure_rate_change,PEEP_median,PEEP_variance,PEEP_rate_change,pH_median,pH_variance,pH_rate_change,Plateau Pressure_median,Plateau Pressure_variance,Plateau Pressure_rate_change,platelets x 1000_median,platelets x 1000_variance,platelets x 1000_rate_change,potassium_median,potassium_variance,potassium_rate_change,PT - INR_median,PT - INR_variance,PT - INR_rate_change,PTT_median,PTT_variance,PTT_rate_change,Respiratory Rate_median,Respiratory Rate_variance,Respiratory Rate_rate_change,SaO2_median,SaO2_variance,SaO2_rate_change,sodium_median,sodium_variance,sodium_rate_change,SpO2_median,SpO2_variance,SpO2_rate_change,systemicdiastolic_median,systemicdiastolic_variance,systemicdiastolic_rate_change,systemicmean_median,systemicmean_variance,systemicmean_rate_change,systemicsystolic_median,systemicsystolic_variance,systemicsystolic_rate_change,Temperature_median,Temperature_variance,Temperature_rate_change,total bilirubin_median,total bilirubin_variance,total bilirubin_rate_change,Total CO2_median,Total CO2_variance,Total CO2_rate_change,TV/kg IBW_median,TV/kg IBW_variance,TV/kg IBW_rate_change,WBC x 1000_median,WBC x 1000_variance,WBC x 1000_rate_change,diagnose_Cardiac Arrest,diagnose_Valve Disease,diagnose_Overdose,diagnose_Gastrointestinal Bleed,diagnose_Thoracotomy,diagnose_Arrhythmia,diagnose_Cardiogenic Shock,diagnose_Cardiovascular (Other),diagnose_Coma,diagnose_Cancer,diagnose_Cerebrovascular Accident/Stroke,diagnose_Acute Myocardial Infarction,diagnose_Cardiovascular (Medical),diagnose_Acute Coronary Syndrome,diagnose_Acute Renal Failure,diagnose_Diabetic Ketoacidosis,diagnose_Pneumonia,diagnose_Asthma or Emphysema,diagnose_Chest Pain Unknown Origin,diagnose_Coronary Artery Bypass Graft,diagnose_Trauma,diagnose_Neurologic,diagnose_Respiratory (Medical/Other),diagnose_Sepsis,diagnose_Gastrointestinal Obstruction,diagnose_Other |
| data example | long stay,Female,72,0.34,111.0,0,0,0,0,0,0,0,0,0,0,96.0,,0.0,1.5,,0.0,,,,31.0,,0.0,7.0,,0.0,,,,,,,8.0,,0.0,29.0,,0.0,6.3,,0.0,0.6,,0.0,19.0,6.14,16.0,,,,,,,100.0,,0.0,2.5,0.97,2.0,,,,4.0,1.5,3.0,7.0,1.07,3.0,,,,1.0,0.0,0.0,372.0,,0.0,31.1,,0.0,82.0,37.74,43.0,10.3,,0.0,,,,6.8,,0.0,,,,,,,54.0,126.88,56.0,68.0,137.9,69.0,111.5,115.64,40.0,,,,29.0,,0.0,,,,,,,77.2,,0.0,,,,,,,5.0,0.0,0.0,7.23,,0.0,22.0,26.24,16.0,150.0,,0.0,4.9,,0.0,,,,,,,28.0,9.04,11.0,,,,134.0,,0.0,,,,71.5,66.82,41.0,89.0,166.6,76.0,122.5,373.93,98.0,36.8,0.0,0.2,3.1,,0.0,,,,7.89,0.0,0.0,18.0,,0.0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0 |

### Figure 5
![](https://github.com/SweeneyLi/ards/raw/master/static/img/fig5.png)
| figure name  | Trends of acidotic parameters over time by patient status 48 h after identification time. |
| ------------ | ------------------------------------------------------------ |
| explanation  | Three sub figure need three kinds of lab data(Lactate,PH and Bicarbonate), each lab data need two types of data(point and box).So there are six files of this experiment. |
| data time    | 24h after ARDS identification                                |
| data format  | point: label,ards_group,offset_15m,value                     |
| data example | point: bicarbonate,long stay,0.0,22.0                        |
### Figure 6
![](https://github.com/SweeneyLi/ards/raw/master/static/img/fig6.png)
| figure name  | Predicting patients with a long ARDS course.                 |
| ------------ | ------------------------------------------------------------ |
| explanation  | There are four feature subsets of all features, top 15 features, bortua selection features and P/F ratio only, which used to train model. |
| data time    | 24h after ARDS identification                                |
| data format  | same as data of fig4                                         |
| data example | same as data of fig4                                         |
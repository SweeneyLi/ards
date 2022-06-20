# Utilizing machine learning to improve clinical trial design for acute respiratory distress syndrome
> data label: long_stay/rapid_death/spontaneous_recovery

## Figure
### Figure 3
![](https://github.com/SweeneyLi/ards/raw/master/static/img/fig3.png)

| figure name  | Characterization of ARDS cohort.                                                                                                                     |
| ------------ |------------------------------------------------------------------------------------------------------------------------------------------------------|
| explanation  | Four statistical sub figure need one kinds of data.                                                                                                  |
| data time    | 24h after ARDS identification                                                                                                                        |
| data format  | ards_group, pf_8h_min, hospital_dead_status, 28d_death_status, icu_death_status, age, apachescore, hospital_los, icu_los, admission_diagnosis |
| data example | long_stay, 9, true, true,  true, 66, 45, 8, 7, 'Sepsis'                                                                                              |

### Figure 4
![](https://github.com/SweeneyLi/ards/raw/master/static/img/fig4.png)
| figure name  | Top univariate associations with patient status 48 h after identification. |
| ------------ | ------------------------------------------------------------ |
| explanation  | Three univariate sub figure need one kinds of data.(58 dynamic features) |
| data time    | 24h after ARDS identification                                |
| data format  | ards_group, dynamic_feature_median, dynamic_feature_variance, dynamic_feature_rate_change |
| data example | Long_stay, fio2_median, fio2_variance, fio2_rate_change,.... |

### Figure 5
![](https://github.com/SweeneyLi/ards/raw/master/static/img/fig5.png)
| figure name  | Trends of acidotic parameters over time by patient status 48 h after identification time. |
| ------------ | ------------------------------------------------------------ |
| explanation  | Three sub figure need three kinds of lab data(Lactate,PH and Bicarbonate), each lab data need two types of data(point and box).So there are six files of this experiment. |
| data time    | 24h after ARDS identification                                |
| data format  | point: ards_group, lab_data_offset(each 15m), lab_data_value<br/>  box: ards_group, lab_data_section_start(each 2h),  lab_data_section_end， lab_data_section_25th, lab_data_section_75th |
| data example | point: long_stay, 45, 8<br/>  box: long_stay, 6, 8, 5.1, 8.2 |
### Figure 6
![](https://github.com/SweeneyLi/ards/raw/master/static/img/fig6.png)
| figure name  | Predicting patients with a long ARDS course.                 |
| ------------ | ------------------------------------------------------------ |
| explanation  | There are four feature subsets of all features, top 15 features, bortua selection features and P/F ratio only, which used to train model. |
| data time    | 24h after ARDS identification                                |
| data format  | ards_group, static_feature(4+5+26), indictator_feature(10) and dynamic_feature(58*3) |
| data example | long_staic, ....., true, false,..., fio2_median, fio2_variance, fio2_rate_change,.... |
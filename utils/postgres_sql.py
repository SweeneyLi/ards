import pandas as pd
import psycopg2
from utils.init_config import db_config
from utils.common import reformat_feature_from_column_to_line, log_time

__all__ = ['PostgresSqlConnector']


class PostgresSqlConnector:
    def __init__(self):
        self.con = psycopg2.connect(dbname=db_config['db_name'],
                                    user=db_config['sql_user'],
                                    password=db_config['db_password'],
                                    host=db_config['sql_host'],
                                    port=db_config['sql_port'])
        self.query_schema = 'SET search_path to public,' + db_config['schema_name'] + ';'

    def close(self):
        self.con.close()

    def get_data_by_query(self, query):
        query = self.query_schema + query
        return pd.read_sql_query(query, self.con)

    def get_ards_data_icu_stay_id(self):
        query = """
            select patientunitstayid from ards_data
            """
        return self.get_data_by_query(query)['patientunitstayid'].tolist()

    def get_pao2_fio2_peep_info_by_icu_stay_id(self, icu_stay_id):
        query = """
            select le.labresultoffset as time_offset,
                   le.labname         as label,
                   le.labresult       as value
            from lab as le
            where le.patientunitstayid = {icu_stay_id}
              and le.labname in
                  ('paO2', 'FiO2', 'PEEP')
              and le.labresult is not null
              and le.labresult > 0
            UNION
            select res.respchartoffset                 as time_offset,
                   res.respchartvaluelabel             as label,
                   CAST(res.respchartvalue as DECIMAL) as value
            from respiratorycharting as res
            where res.patientunitstayid = {icu_stay_id}
              and res.respchartvaluelabel in
                  ('FiO2', 'PEEP')
              and res.respchartvalue is not null
              and position('%' in res.respchartvalue) = 0;

            """.format(icu_stay_id=icu_stay_id)

        return self.get_data_by_query(query)

    def get_pao2_fio2_peep_info_by_icu_stay_id_and_offset(self, icu_stay_id, offset):
        query = """
            select le.labresultoffset as time_offset,
                   le.labname         as label,
                   le.labresult       as value
            from lab as le
            where le.patientunitstayid = {icu_stay_id} and
                    le.labresultoffset >= {offset}
              and le.labname in
                  ('paO2', 'FiO2', 'PEEP')
              and le.labresult is not null
              and le.labresult > 0
            UNION
            select res.respchartoffset                 as time_offset,
                   res.respchartvaluelabel             as name,
                   CAST(res.respchartvalue as DECIMAL) as value
            from respiratorycharting as res
            where res.patientunitstayid = {icu_stay_id}
              and res.respchartvaluelabel in
                  ('FiO2', 'PEEP')
              and res.respchartvalue is not null
              and position('%' in res.respchartvalue) = 0;

            """.format(icu_stay_id=icu_stay_id, offset=offset)

        return self.get_data_by_query(query)

    # todo::failed
    def set_ards_data_valid_tag(self, icu_stay_id, valid_tag=1):
        pass
        # con = psycopg2.connect(dbname=db_name, user=sql_user, password=db_password, host=sql_host, port=sql_port)
        # # query = """select * from pg_roles"""
        # query = """
        #     update ards_data
        #     set valid_tag = {valid_tag}
        #     where patientunitstayid = {icu_stay_id};
        #     """.format(icu_stay_id=icu_stay_id, valid_tag=valid_tag)
        # print(query)
        # cur = con.cursor()
        # cur.execute(query)
        # # data = cur.fetchall()
        # # print(data)
        #
        # con.commit()
        # cur.close()
        # con.close()

    def get_static_feature(self, icu_stay_id):
        return pd.concat(
            [self.get_patient_table_feature(icu_stay_id),
             self.get_apachePatientResult_table_feature(icu_stay_id),
             self.get_indicator_feature(icu_stay_id)],
            axis=1)

    def get_patient_table_feature(self, icu_stay_id):
        query = """
                    select gender,
                    age,
                    apacheadmissiondx,
                    admissionheight,
                    admissionweight,
                    hospitaladmitsource,
                    hospitalAdmitOffset,
                    unitdischargeoffset,
                    unitdischargestatus,
                    hospitaldischargeoffset,
                    hospitaldischargestatus
                    from patient
                    where patientunitstayid = {icu_stay_id};
            """.format(icu_stay_id=icu_stay_id)
        return self.get_data_by_query(query)

    def get_apachePatientResult_table_feature(self, icu_stay_id):
        query = """
                    select apachescore from apachepatientresult
                    where patientunitstayid = {icu_stay_id} limit 1;
            """.format(icu_stay_id=icu_stay_id)
        return self.get_data_by_query(query)

    def get_indicator_feature(self, icu_stay_id):
        query = """
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
                where patientunitstayid = {icu_stay_id};
                    """.format(icu_stay_id=icu_stay_id)
        data = self.get_data_by_query(query)
        if data.shape[0] == 0:
            data.loc[0] = [0 for i in range(10)]
        return data

    @log_time
    def get_dynamic_feature(self, icu_stay_id, start_offset, end_offset):
        data = {
            'lab': self.get_lab_feature(icu_stay_id, start_offset, end_offset),
            'nurseCharting': self.get_nurseCharting_feature(icu_stay_id, start_offset, end_offset),
            'respiratoryCharting': self.get_respiratoryCharting_feature(icu_stay_id, start_offset, end_offset),
            'vitalAperiodic': self.get_vitalAperiodic_feature(icu_stay_id, start_offset, end_offset),
            'vitalPeriodic': self.get_vitalPeriodic_feature(icu_stay_id, start_offset, end_offset),
        }

        return data

    def get_lab_feature(self, icu_stay_id, start_offset, end_offset):
        # ALP can not find
        query = """
                select labresultoffset as time_offset,
                labname         as label,
                labresult       as value
                from lab
                where 
                 labresultoffset >= {start_offset}
                and labresultoffset <= {end_offset}
                and labname in (
                '-eos',
                'magnesium',
                '-basos',
                'AST (SGOT)',
                '-bands',
                'total bilirubin',
                'calcium',
                'Total CO2',
                'creatinine',
                'paCO2',
                'potassium',
                'PTT',
                'SaO2',
                'sodium',
                'WBC x 1000',
                'glucose',
                'Hct',
                'Hgb',
                'lactate',
                'ionized calcium',
                'Magnesium',
                'paO2',
                'FiO2',
                'P/F ratio',
                'albumin',
                'platelets x 1000',
                'bicarbonate',
                'BUN',
                'Base Excess',
                'ALT (SGPT)',
                'ALP',
                'pH',
                'PT - INR',
                'Basos',
                'EOs'
                )
                
        """.format(icu_stay_id=icu_stay_id, start_offset=start_offset, end_offset=end_offset)
        data = self.get_data_by_query(query)
        data['label'].replace('-bands', 'bands', inplace=True)
        data['label'].replace('-basos', 'basos', inplace=True)
        data['label'].replace('-eos', 'eos', inplace=True)
        return data

    def get_nurseCharting_feature(self, icu_stay_id, start_offset, end_offset):
        # typecat, typevallabel, typevalname
        # Other Vital Signs and Infusions,Score (Glasgow Coma Scale),Value (GCS Total)
        # Scores, Glasgow coma score, GCS Total
        # Scores, Glasgow coma score, [Verbal|Motor|Eyes]
        # Other Vital Signs and Infusions,SpO2,Value
        # Vital Signs,Respiratory Rate,Respiratory Rate
        # Vital Signs,Temperature,Temperature (C)
        def get_nurseCharting_feature(typecat, typevallabel, typevalname):
            if typecat == 'Other Vital Signs and Infusions':
                if typevallabel == 'Score (Glasgow Coma Scale)':
                    return 'GCS Total'
                if typevallabel == 'SpO2':
                    return 'SpO2'
            if typecat == 'Scores':
                return {'GCS Total': 'GCS Total', 'Verbal': 'GCS Verbal', 'Motor': 'GCS Motor', 'Eyes': 'GCS Eyes'}.get(
                    typevalname, None)
            if typecat == 'Vital Signs':
                if typevallabel == 'Respiratory Rate':
                    return 'Respiratory Rate'
                if typevallabel == 'Temperature':
                    return 'Temperature'

            print('get_nurseCharting_feature: get wrong label\ntypecat:%s, typevallabel:%s, typevalname:%s' % (
                typecat, typevallabel, typevalname))

        query = """
            select 
            nursingchartoffset      as time_offset,
            nursingchartcelltypecat as typecat,
            nursingchartcelltypevallabel as typevallabel,
            nursingchartcelltypevalname as typevalname,
            nursingchartvalue           as value
            from nursecharting
            where nursingchartvalue != ''
            and nursingchartvalue != 'Unable to score due to medication'
            and nursingchartcelltypecat in
            ('Scores', 'Other Vital Signs and Infusions', 'Vital Signs') 
            and nursingchartcelltypevallabel in ('Glasgow coma score', 'Score (Glasgow Coma Scale)','SpO2', 'Temperature', 'Respiratory Rate') 
            and nursingchartcelltypevalname in ('Value', 'GCS Total', 'Motor', 'Verbal', 'Eyes', 'Respiratory Rate', 'Temperature (C)')
            and nursingchartoffset >= {start_offset}
            and nursingchartoffset <= {end_offset}
        """.format(icu_stay_id=icu_stay_id, start_offset=start_offset, end_offset=end_offset)
        data = self.get_data_by_query(query)

        data['label'] = data.apply(
            lambda x: get_nurseCharting_feature(x['typecat'], x['typevallabel'], x['typevalname']), axis=1)
        data = data.loc[:, ['time_offset', 'label', 'value']]

        return data

    def get_respiratoryCharting_feature(self, icu_stay_id, start_offset, end_offset):
        query = """
                select respchartoffset as time_offset,
                respchartvaluelabel as label,
                respchartvalue      as value
                from respiratorycharting
                where
                respchartvalue != ''
                and respchartoffset >= {start_offset}
                and respchartoffset <= {end_offset}
                and respchartvaluelabel in (
                                  'Plateau Pressure', 
                                  'Peak Insp. Pressure',
                                  'Mean Airway Pressure',
                                  'PEEP',
                                  'TV/kg IBW',
                                  'SaO2', 'EtCO2'
                )
        """.format(icu_stay_id=icu_stay_id, start_offset=start_offset, end_offset=end_offset)
        data = self.get_data_by_query(query)
        data['label'].replace('EtCO2', 'etco2', inplace=True)
        return data

    def get_vitalAperiodic_feature(self, icu_stay_id, start_offset, end_offset):
        query = """
                select observationoffset as time_offset, noninvasivesystolic, noninvasivediastolic, noninvasivemean
                from vitalaperiodic
                where observationoffset >= {start_offset}
                and observationoffset <= {end_offset}
                and patientunitstayid = {icu_stay_id};
            """.format(icu_stay_id=icu_stay_id, start_offset=start_offset, end_offset=end_offset)
        return reformat_feature_from_column_to_line(self.get_data_by_query(query))

    def get_vitalPeriodic_feature(self, icu_stay_id, start_offset, end_offset):
        query = """
                    select observationoffset as time_offset,
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
                    where observationoffset >= {start_offset}
                    and observationoffset <= {end_offset}
                    and patientunitstayid = {icu_stay_id};
            """.format(icu_stay_id=icu_stay_id, start_offset=start_offset, end_offset=end_offset)
        return reformat_feature_from_column_to_line(self.get_data_by_query(query))

    def get_pao2_fio2_in_first_8h_after_ards_identification(self, icu_stay_id, identification_offset):
        query = """
            select le.labresultoffset as time_offset,
                   le.labname         as label,
                   le.labresult       as value
            from lab as le
            where le.patientunitstayid = {icu_stay_id}
              and le.labname in
                  ('paO2', 'FiO2')
              and le.labresult is not null
              and le.labresult > 0
              and le.labresultoffset >= {identification_offset}
              and le.labresultoffset <= {identification_offset} + 8 * 60 
            UNION
            select res.respchartoffset                 as resultoffset,
                   res.respchartvaluelabel             as name,
                   CAST(res.respchartvalue as DECIMAL) as value
            from respiratorycharting as res
            where res.patientunitstayid = {icu_stay_id}
              and res.respchartvaluelabel = 'FiO2'
              and res.respchartvalue is not null
              and position('%' in res.respchartvalue) = 0
              and res.respchartoffset >= {identification_offset}
              and res.respchartoffset <= {identification_offset} + 8 * 60;
            """.format(icu_stay_id=icu_stay_id, identification_offset=identification_offset)

        return self.get_data_by_query(query)


if __name__ == '__main__':
    test_icu_stay_id = 554884
    test_identification_offset = 594
    sql = PostgresSqlConnector()
    # print(sql.get_patient_table_feature(test_icu_stay_id))
    # print(sql.get_apachePatientResult_table_feature(test_icu_stay_id))
    # print(sql.get_static_feature(test_icu_stay_id))
    # print(sql.get_indicator_feature(test_icu_stay_id))
    # print(sql.get_feature(test_icu_stay_id))
    # print(sql.get_pao2_fio2_in_first_8h_after_ards_identification(test_icu_stay_id, test_identification_offset))
    # print(sql.get_vitalAperiodic_feature(test_icu_stay_id))
    # print(sql.get_vitalPeriodic_feature(test_icu_stay_id))
    # sql.set_ards_data_valid_tag(351515)

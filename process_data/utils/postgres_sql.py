import pandas as pd
import psycopg2
import yaml
import os

__all__ = ['PostgresSqlConnector']

config_path = '/Users/sweeney/WorkSpace/WorkCode/aids/process_data/config.yaml'
if os.path.isfile(config_path):
    print('Detected the config file')
    with open(config_path, 'r') as f:
        config = yaml.load(f.read(), Loader=yaml.Loader)
    sql_user = config['database']['sql_user']
    db_name = config['database']['db_name']
    db_password = config['database']['db_password']
    schema_name = config['database']['schema_name']
    sql_host = config['database']['sql_host']
    sql_port = config['database']['sql_port']

    section_range = config['section_range']
else:
    sql_user = 'postgres'
    db_name = 'eicu'
    db_password = '123456'
    schema_name = 'eicu_crd'
    sql_host = '172.16.60.173'
    sql_port = 3307


class PostgresSqlConnector:
    def __init__(self):
        # Connect to the database
        self.con = psycopg2.connect(dbname=db_name, user=sql_user, password=db_password, host=sql_host, port=sql_port)
        self.query_schema = 'SET search_path to public,' + schema_name + ';'

    def get_data_by_query(self, query):
        query = self.query_schema + query
        return pd.read_sql_query(query, self.con)

    def get_ards_data_icu_stay_id(self):
        query = """
            select patientunitstayid from ards_data
            """
        return self.get_data_by_query(query)['patientunitstayid'].tolist()

    def get_pao2_fio2_peep_info_by_icu_stay_id(self, icu_stay_id):
        assert type(icu_stay_id) == int

        query = """
            select le.labresultoffset as offset,
                   le.labname         as label,
                   le.labresult       as value
            from lab as le
            where le.patientunitstayid = {icu_stay_id}
              and le.labname in
                  ('paO2', 'FiO2', 'PEEP')
              and le.labresult is not null
              and le.labresult > 0
            UNION
            select res.respchartoffset                 as resultoffset,
                   res.respchartvaluelabel             as name,
                   CAST(res.respchartvalue as DECIMAL) as value
            from respiratorycharting as res
            where res.patientunitstayid = {icu_stay_id}
              and res.respchartvaluelabel in
                  ('FiO2', 'PEEP')
              and res.respchartvalue is not null
              and position('%' in res.respchartvalue) = 0;

            """.format(icuid=icu_stay_id)

        return self.get_data_by_query(query)

    # todo::failed
    def set_ards_data_valid_tag(self, icu_stay_id, valid_tag=1):
        con = psycopg2.connect(dbname=db_name, user=sql_user, password=db_password, host=sql_host, port=sql_port)
        # query = """select * from pg_roles"""
        query = """
            update ards_data
            set valid_tag = {valid_tag}
            where patientunitstayid = {icu_stay_id};
            """.format(icu_stay_id=icu_stay_id, valid_tag=valid_tag)
        print(query)
        cur = con.cursor()
        cur.execute(query)
        # data = cur.fetchall()
        # print(data)

        con.commit()
        cur.close()
        con.close()

    def close(self):
        self.con.close()

    def get_feature(self, icu_stay_id):
        return pd.concat((self.get_static_feature(icu_stay_id), self.get_indicator_feature(icu_stay_id)),
                         axis=1)

    def get_static_feature(self, icu_stay_id):
        return pd.concat((self.get_patient_table_feature(icu_stay_id), self.get_apachePatientResult_table_feature(icu_stay_id)),
                         axis=1)

    def get_patient_table_feature(self, icu_stay_id):
        query = """
                    select 
                    gender,
                    age,
                    apacheadmissiondx,
                    (case
                    when admissionheight is not null
                    and admissionheight != 0
                    and admissionweight is not null
                    then admissionweight / (admissionheight * admissionheight * 0.0001)
                    else NULL end) as bmi,
                    hospitaladmitsource,
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

    def get_dynamic_result(self, icu_stay_id):
        return

    def get_vitalAperiodic_feature(self, icu_stay_id):
        query = """
                select observationoffset as offset, nonInvasiveSystolic, noninvasivediastolic, noninvasivemean
                from vitalaperiodic
                where observationoffset <= 1440
                and patientunitstayid = {icu_stay_id};
            """.format(icu_stay_id=icu_stay_id)
        return PostgresSqlConnector.reformat_feature(self.get_data_by_query(query))

    def get_vitalPeriodic_feature(self, icu_stay_id):
        query = """
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
                    and patientunitstayid = {icu_stay_id};
            """.format(icu_stay_id=icu_stay_id)
        return PostgresSqlConnector.reformat_feature(self.get_data_by_query(query))

    @staticmethod
    def reformat_feature(data):
        label_list = list(data.columns).copy()
        label_list.remove('offset')

        new_data = pd.DataFrame(columns=['offset', 'label', 'value'])
        for index, row in data.iterrows():
            offset = row['offset']
            for l in label_list:
                if row[l]:
                    new_data = new_data.append({'offset': offset, 'label': l, 'value': row[l]}, ignore_index=True)
        return new_data


if __name__ == '__main__':
    test_icu_stay_id = 554884
    sql = PostgresSqlConnector()
    # print(sql.get_patient_table_feature(test_icu_stay_id))
    # print(sql.get_apachePatientResult_table_feature(test_icu_stay_id))
    # print(sql.get_static_feature(test_icu_stay_id))
    # print(sql.get_indicator_feature(test_icu_stay_id))
    print(sql.get_feature(test_icu_stay_id))
    # print(sql.get_vitalAperiodic_feature(test_icu_stay_id))
    # print(sql.get_vitalPeriodic_feature(test_icu_stay_id))
    # sql.set_ards_data_valid_tag(351515)

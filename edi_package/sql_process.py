import pymssql
import sqlalchemy
import pandas as pd
from tqdm import tqdm
import logging
import datetime
from dateutil.relativedelta import relativedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)



class DBConnect:

    def __init__(self, host, user,port, password, database):
        self._hostname = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._conn = None # MSSQL 접속
        self._cursor = None

    def check_edi_table(self, database, table):
        create_table_sql = """
                                USE {db_name};
                                IF OBJECT_ID('{table_name}', 'U') IS NULL

                                CREATE TABLE {table_name}(
                                concept_code			  	VARCHAR(50)		NOT NULL ,
                                concept_name			  	VARCHAR(255)	NOT NULL ,  --Please note that we allowed lengthy concept name
                                concept_synonym       VARCHAR(255)	NULL,
                                domain_id				      VARCHAR(20)		NOT NULL ,
                                vocabulary_id			  	VARCHAR(20)		NOT NULL ,
                                concept_class_id			VARCHAR(20)		NOT NULL ,
                                valid_start_date			DATE			    NOT NULL ,
                                valid_end_date		  	DATE	    		NOT NULL ,
                                invalid_reason		  	VARCHAR(1)		NULL ,
                                ancestor_concept_code VARCHAR(20)		NULL ,
                                previous_concept_code VARCHAR(20)		NULL ,
                                material              VARCHAR(2000)  NULL ,
                                sanjung_name          VARCHAR(2000)		NULL ,
                                company_name          VARCHAR(255)  NULL,
                                value                VARCHAR(255)  NULL,
                                unit                 VARCHAR(255)   NULL
                                );
                                """.format(db_name=database, table_name= table)
        
        self._cursor.execute(create_table_sql)
        self._conn.commit()

    def check_drug_relation_table(self, database, table):
        create_table_sql = """
                                USE {db_name};
                                IF OBJECT_ID('{table_name}', 'U') IS NULL

                                CREATE TABLE {table_name}(
                                concept_code			  	VARCHAR(50)		NOT NULL ,
                                ancestor_concept_code VARCHAR(20)		NULL ,
                                ancestor_date   DATE    NOT NULL
                                );
                                """.format(db_name=database, table_name= table)
        
        self._cursor.execute(create_table_sql)
        self._conn.commit()
    
    def create_temp_table(self):
        delete_temp_table = """
                    IF OBJECT_ID('#temp_table') IS NOT NULL
                    DROP TABLE #temp_table;"""

        self._cursor.execute(delete_temp_table)
        self._conn.commit()
        
        create_temp_table = """
                    CREATE TABLE #temp_table (
                    concept_code			  	VARCHAR(50)		NOT NULL ,
                    concept_name			  	VARCHAR(255)	NOT NULL ,  --Please note that we allowed lengthy concept name
                    concept_synonym       VARCHAR(255)	NULL,
                    domain_id				      VARCHAR(20)		NOT NULL ,
                    vocabulary_id			  	VARCHAR(20)		NOT NULL ,
                    concept_class_id			VARCHAR(20)		NOT NULL ,
                    valid_start_date			DATE			    NOT NULL ,
                    valid_end_date		  	DATE	    		NOT NULL ,
                    invalid_reason		  	VARCHAR(1)		NULL ,
                    ancestor_concept_code VARCHAR(20)		NULL ,
                    previous_concept_code VARCHAR(20)		NULL ,
                    material              VARCHAR(1000)  NULL ,
                    sanjung_name          VARCHAR(1000)		NULL ,
                    company_name          VARCHAR(255)  NULL,
                    value                VARCHAR(255)  NULL,
                    unit                 VARCHAR(255)   NULL
                    );
                    """

        self._cursor.execute(create_temp_table)
        self._conn.commit()

    def create_temp_drug_relationship(self):
        delete_temp_table = """
                    IF OBJECT_ID('#temp_table') IS NOT NULL
                    DROP TABLE #temp_table;"""

        self._cursor.execute(delete_temp_table)
        self._conn.commit()
        
        create_temp_table = """
                    CREATE TABLE #temp_table (
                    concept_code			  	VARCHAR(50)		NOT NULL,
                    ancestor_concept_code       VARCHAR(20)		NULL ,
                    ancestor_date               DATE            NOT NULL
                    );
                    """

        self._cursor.execute(create_temp_table)
        self._conn.commit()      

    def check_null(self, data):

        check_null_col = ["concept_code", "concept_name", "domain_id", "vocabulary_id", "concept_class_id", "valid_start_date", "valid_end_date"]

        for col in check_null_col:
            if data[col].isnull().sum()>0:
            
                raise Exception(data[data[col].isna()], f"Fill {col}","count:",data[col].isnull().sum())
        
        return data

    def check_length(self, data):
        
        data["concept_name"] = data["concept_name"].apply(lambda x : x.encode('utf-8')[:255].decode("utf-8",'ignore') if len(x) >200 else x)
        data["concept_synonym"] = data["concept_synonym"].apply(lambda x : x.encode('utf-8')[:128].decode("utf-8",'ignore') if len(x) > 100 else x)

        return data

    def temp_update_data(self, data):
        
        engine = sqlalchemy.create_engine("mssql+pymssql://", creator=lambda: self._conn)

        def chunks(lst, n):
            """Yield successive n-sized chunks from lst."""
            for i in range(0, len(lst), n):
                yield lst[i:i + n]


        for data in tqdm(list(chunks(data.to_dict('records'), 500))):
            df_piece = pd.DataFrame(data)
            # Insert the DataFrame into the MSSQL database
            df_piece.to_sql('#temp_table', engine, if_exists='append', index=False, method='multi', chunksize=1000)
    
    def update_device(self, database, table, data):

        self._conn = pymssql.connect(host=self._hostname, user=self._user, password=self._password,port = self._port, database=self._database)
        self._cursor = self._conn.cursor()
        check_null_data = self.check_null(data)
        check_length_data = self.check_length(check_null_data)
        self.check_edi_table(database=database, table=table)

        self.create_temp_table()

        self.temp_update_data(data=check_length_data)

        del_db_device_list ="""DELETE FROM {db_name}.dbo.{table_name}
                                WHERE domain_id = 'device';
                            """.format(db_name=database, table_name= table)
        self._cursor.execute(del_db_device_list)

        insert_db_device_list ="""INSERT INTO {db_name}.dbo.{table_name}
        SELECT CONCEPT_CODE, CONCEPT_NAME, CONCEPT_SYNONYM, DOMAIN_ID, VOCABULARY_ID, CONCEPT_CLASS_ID,
        VALID_START_DATE, VALID_END_DATE, INVALID_REASON, ANCESTOR_CONCEPT_CODE, PREVIOUS_CONCEPT_CODE, MATERIAL, SANJUNG_NAME, COMPANY_NAME, VALUE, UNIT
        FROM #temp_table AS new_code""".format(db_name = database, table_name=table)
        self._cursor.execute(insert_db_device_list)

        self._conn.commit()

        self._conn.close()

    def update_suga(self, database, table, domain, data, date):

        self._conn = pymssql.connect(host=self._hostname, user=self._user, port= self._port, password=self._password, database=self._database)
        self._cursor = self._conn.cursor()
        check_null_data = self.check_null(data)
        check_length_data = self.check_length(check_null_data)

        # start_date = datetime.datetime.strptime(date, "%Y-%m-%d")

        end_date = datetime.datetime.strptime(date, "%Y.%m.%d") - datetime.timedelta(days=1)

        self.check_edi_table(database=database, table=table)

        self.create_temp_table()

        self.temp_update_data(data=check_length_data)
        # update_count_query = """SELECT COUNT(*) AS NEW_CODE_COUNT FROM #temp_table AS new_code
        # WHERE new_code.concept_code not in (SELECT concept_code from {db_name}.dbo.{table_name})""".format(db_name=database, table_name= table)

        # self._cursor.execute(update_count_query)


        # update_count =self._cursor.fetchone()

        # logging.info(update_count)

        sql_update_query =  """
        INSERT INTO {db_name}.dbo.{table_name}
        SELECT CONCEPT_CODE, CONCEPT_NAME, CONCEPT_SYNONYM, DOMAIN_ID, VOCABULARY_ID, CONCEPT_CLASS_ID,
        VALID_START_DATE,
        VALID_END_DATE, INVALID_REASON, ANCESTOR_CONCEPT_CODE, PREVIOUS_CONCEPT_CODE, MATERIAL, SANJUNG_NAME, COMPANY_NAME, VALUE, UNIT
        FROM #temp_table AS new_code
        WHERE new_code.concept_code not in (SELECT concept_code from {db_name}.dbo.{table_name})
        """.format(db_name = database, table_name=table)

        self._cursor.execute(sql_update_query)

        invalid_update_query = """UPDATE {db_name}.dbo.{table_name}
        SET VALID_END_DATE = CONVERT(DATE, '2099-12-31'), INVALID_REASON = NULL
        FROM {db_name}.dbo.{table_name} AS A
        LEFT JOIN #temp_table AS B
        ON A.concept_code = B.concept_code
        WHERE B.concept_code IS NOT NULL AND A.DOMAIN_ID IN ({domain_ids}) AND A.INVALID_REASON = 'D'
        """.format(db_name=database, table_name=table, domain_ids=domain, valid_start_date=date, valid_end_date=end_date)

        self._cursor.execute(invalid_update_query)

        sql_previous_update = """UPDATE {db_name}.dbo.{table_name}
        SET VALID_END_DATE = '{valid_end_date}', INVALID_REASON = 'U'
        FROM {db_name}.dbo.{table_name} AS A
        LEFT JOIN #temp_table AS B
        ON A.CONCEPT_CODE = B.PREVIOUS_CONCEPT_CODE
        WHERE B.PREVIOUS_CONCEPT_CODE IS NOT NULL AND A.DOMAIN_ID IN ({domain_ids}) AND (A.INVALID_REASON IS NULL OR A.INVALID_REASON = 'D')
        """.format(db_name=database, table_name=table, domain_ids=domain, valid_end_date=end_date)

        self._cursor.execute(sql_previous_update)

        invalid_update_query2 = """UPDATE {db_name}.dbo.{table_name}
        SET VALID_END_DATE = '{valid_end_date}', INVALID_REASON = 'D'
        FROM {db_name}.dbo.{table_name} as A 
        LEFT JOIN #temp_table as B
        on A.concept_code = B.concept_code
        WHERE B.concept_code IS NULL AND A.DOMAIN_ID IN ({domain_ids}) AND A.INVALID_REASON IS NULL

        """.format(db_name=database, table_name=table, domain_ids=domain, valid_end_date=end_date)

        self._cursor.execute(invalid_update_query2)

        self._conn.commit()
        self._conn.close()


    def update_drug(self, database, table, domain, data, date):

        # 코드 재사용을 update_suga를 사용 
        self.update_suga(self, database, table, domain, data, date)

    def update_drug_relationship(self, database, table, data, date):
        data = data[["concept_code","ancestor_concept_code","valid_start_date"]].rename(columns={'valid_start_date':'ancestor_date'})
        self._conn = pymssql.connect(host=self._hostname, user=self._user, port= self._port, password=self._password, database=self._database)
        self._cursor = self._conn.cursor()
        # check_null_data = self.check_null(data)
        # check_length_data = self.check_length(check_null_data)

        self.check_drug_relation_table(database=database, table=table)
        self.create_temp_drug_relationship()
        self.temp_update_data(data=data)

        sql_update_query =  """
        INSERT INTO {db_name}.dbo.{table_name}
        SELECT CONCEPT_CODE, ANCESTOR_CONCEPT_CODE, ancestor_date
        FROM #temp_table AS relationship_table
        WHERE relationship_table.CONCEPT_CODE NOT IN (SELECT CONCEPT_CODE FROM {db_name}.dbo.{table_name})
        """.format(db_name = database, table_name=table)
        
        self._cursor.execute(sql_update_query)

        sql_update_query2 = """
            INSERT INTO {table_name}
            SELECT relationship_table.concept_code, relationship_table.ancestor_concept_code, relationship_table.ancestor_date
            FROM #temp_table AS relationship_table
            LEFT JOIN {table_name}
                ON relationship_table.concept_code = {table_name}.concept_code 
                AND relationship_table.ancestor_concept_code = {table_name}.ancestor_concept_code
            WHERE {table_name}.concept_code IS NULL;
        """.format(db_name = database, table_name=table)
        self._cursor.execute(sql_update_query2)

        self._conn.commit()
        self._conn.close()


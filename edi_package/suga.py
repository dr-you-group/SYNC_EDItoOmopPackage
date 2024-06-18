import pandas as pd
import datetime
import os
import subprocess
import numpy as np
from google.cloud import translate
import re
from tqdm import tqdm
import multiprocessing
import time
from multiprocessing import Manager
import codecs



class SugaTransform:

    def __init__(self, suga_data, sheet_name, suga_code=None, korean_name=None,
                      english_name=None, start_date_name=None, sanjung_name=None):
        
        self._suga_data = suga_data
        self._sheet_name = sheet_name
        self._suga_code = suga_code
        self._korean_name = korean_name
        self._english_name = english_name
        self._start_date_name = start_date_name
        self._sanjung_name = sanjung_name
        self._merge_suga_df = None

    def data_transform(self):
        self.merge_suga()
        self.suga_process()
        return self._merge_suga_df

    def suga_process(self):

        self._merge_suga_df.rename(columns={self._suga_code : "concept_code",
                                        self._korean_name : "concept_synonym",
                                        self._english_name : "concept_name",
                                        self._start_date_name : "valid_start_date",
                                        self._sanjung_name : "sanjung_name"}, inplace=True)


        self._merge_suga_df = self._merge_suga_df.dropna(subset=["concept_code"],how='any')
        self._merge_suga_df["concept_code"] = self._merge_suga_df["concept_code"].astype(str)
        self._merge_suga_df["company_name"] = None
        self._merge_suga_df["domain_id"] = "Procedure"
        self._merge_suga_df["vocabulary_id"] = "EDI"
        self._merge_suga_df["valid_start_date"] = self._merge_suga_df["valid_start_date"].apply(lambda x: datetime.datetime.strptime("1970-01-01", "%Y-%m-%d") if pd.isna(x) else x)
        self._merge_suga_df["valid_end_date"] = datetime.datetime.strptime("2099-12-31", "%Y-%m-%d")
        self._merge_suga_df["invalid_reason"] = None
        self._merge_suga_df["previous_concept_code"] = None
        self._merge_suga_df["material"] = None
        try:
            self._merge_suga_df.loc[self._merge_suga_df['concept_name'].str.contains(r"[ㄱ-ㅣ가-힣]") == True, "concept_name"] = None
        except:
            pass

        # 문자열 양측 공백 제거
        self._merge_suga_df["concept_code"] = self._merge_suga_df["concept_code"].str.strip()
        self._merge_suga_df["concept_name"] = self._merge_suga_df["concept_name"].str.strip()
        self._merge_suga_df["concept_synonym"] = self._merge_suga_df["concept_synonym"].str.strip()
        self._merge_suga_df["sanjung_name"] = self._merge_suga_df["sanjung_name"].str.strip()

        self._merge_suga_df["company_name"] = None
        self._merge_suga_df["value"] = None
        self._merge_suga_df["unit"] = None

        # 문자열에 공백만 있는 데이터가 있음. 이럴땐 Nan으로 처리가 안되어서 Nan으로 바꿔줘야함.
        self._merge_suga_df["concept_code"] = self._merge_suga_df["concept_code"].replace(r'^\s*$', None, regex=True)
        self._merge_suga_df["concept_name"] = self._merge_suga_df["concept_name"].replace(r'^\s*$', None, regex=True)
        self._merge_suga_df["concept_synonym"] = self._merge_suga_df["concept_synonym"].replace(r'^\s*$', None, regex=True)
        self._merge_suga_df["sanjung_name"] = self._merge_suga_df["sanjung_name"].replace(r'^\s*$', None, regex=True)


        self._merge_suga_df["ancestor_concept_code"] = self._merge_suga_df["concept_code"].apply(lambda x:x[0:5])
        self._merge_suga_df["concept_class_id"] = "Proc Hierarchy"

        self._merge_suga_df.loc[self._merge_suga_df["ancestor_concept_code"] == self._merge_suga_df["concept_code"], "ancestor_concept_code"] = None

        self._merge_suga_df.loc[~self._merge_suga_df["ancestor_concept_code"].isin(self._merge_suga_df["concept_code"]), "ancestor_concept_code"] = None

        self._merge_suga_df.loc[self._merge_suga_df["concept_code"].str.match(r"^A[AH]|^[BCDEFG]|^H[AC]|^FA", na=False), "domain_id"] = "Measurement"

        self._merge_suga_df.loc[self._merge_suga_df["concept_code"].str.len() > self._merge_suga_df["ancestor_concept_code"].str.len(), "concept_class_id"] = "Procedure"

        self._merge_suga_df.loc[(self._merge_suga_df["domain_id"] == "Measurement") & (self._merge_suga_df["concept_class_id"] =="Proc Hierarchy"), "concept_class_id"] = "Meas Class"

        self._merge_suga_df.loc[(self._merge_suga_df["domain_id"] == "Measurement") & (self._merge_suga_df["concept_class_id"] =="Procedure"), "concept_class_id"] = "Measurement"  

        self._merge_suga_df.loc[(self._merge_suga_df["domain_id"] == "Measurement") & (self._merge_suga_df["concept_code"].str.match(r"^D", na=False)), "concept_class_id"] = "Proc Hierarchy"

        self._merge_suga_df.loc[self._merge_suga_df["concept_class_id"].str.contains("Measurement"), "domain_id"] = "Procedure"

        self._merge_suga_df = self._merge_suga_df[["concept_code", "concept_name", "concept_synonym", "domain_id", "vocabulary_id", "concept_class_id",
                 "valid_start_date", "valid_end_date", "invalid_reason","ancestor_concept_code","previous_concept_code",
                 "material","sanjung_name"]]
        
    def merge_suga(self):
        import openpyxl
        workbook = openpyxl.load_workbook(self._suga_data)

        sheet_names = workbook.sheetnames
        self._sheet_name = list(set(self._sheet_name).intersection(sheet_names))
        suga_excel = pd.read_excel(self._suga_data, sheet_name = self._sheet_name)

        merge_data = pd.DataFrame()

        for sheet in self._sheet_name:
            sheet_data = suga_excel[sheet]
            merge_data = pd.concat([merge_data, sheet_data], axis=0)
            
        merge_data = merge_data.dropna(subset=[self._korean_name],how='any')
        try:
            from datetime import datetime
            merge_data[self._start_date_name] = merge_data[self._start_date_name].apply(lambda x : datetime.strptime(str(x),"%Y%m%d") if type(x) == int else x)

        except:
            pass
        try:
            merge_data[self._english_name]
        except:
            merge_data[self._english_name] = None


        self._merge_suga_df = merge_data[[self._suga_code, self._korean_name, self._english_name, self._start_date_name, self._sanjung_name]]



    

class SugaTranslate:
    def __init__(self, suga_df, translation_path):
        self._suga_df = suga_df
        self._translation_path = translation_path
        self._translation_df = pd.read_csv(self._translation_path)
        self._translation_df_list = None
        self._translate_list = []
        self.result_translate = multiprocessing.Manager().list()

        
    def data_save(self, result_df):
        
        result = pd.concat([self._translation_df, result_df],axis=0).drop_duplicates(["concept_synonym"])
        save_path = "./result_translate_file"
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        result = result[["concept_name", "concept_synonym"]]

        result.to_csv("./result_translate_file/suga_translation.csv",index=False, encoding="utf-8")
    

    def multi_process_suga(self, text):

        location = "global"
        project_id = "edi-translation"

        parent = f"projects/{project_id}/locations/{location}"

        client = translate.TranslationServiceClient()

        response = client.translate_text(parent= parent, contents=[text],source_language_code="ko", target_language_code="en", mime_type ="text/plain")
        result_word = str(response.translations[0]).split('translated_text: "')[1].replace('"\n',"")
        decoded_text = codecs.escape_decode(result_word, 'unicode-escape')[0].decode('utf-8')
        self.result_translate.append([rf"{decoded_text}",rf'{text}'])
        

    def translate_text(self):

        len_translate_list = len(self._translate_list)


        for chunk in range(0, len_translate_list, 100):
            time.sleep(2)
            translate_list = list(set(self._translate_list))[chunk:chunk+100]
            # return translate_list
            pool = multiprocessing.Pool(10)
            
            pool.map(self.multi_process_suga, tqdm(translate_list))

            pool.close()
            pool.join()

            result_df = pd.DataFrame(list(self.result_translate))

            # return result_df

            result_df.columns = ["concept_name","concept_synonym"]

            self.data_save(result_df)

    def update_translation_csv(self):
        
        self._translation_df_list = self._translation_df["concept_synonym"].to_list()

        # concept_name start
        try:
            self._suga_df.loc[self._suga_df['concept_name'].str.contains(r"[ㄱ-ㅣ가-힣]") == True, "concept_name"] = None

        except:
            pass
        
        suga_df_english_name = self._suga_df[self._suga_df["concept_name"].isna()]

        suga_df_unique = suga_df_english_name.drop_duplicates(["concept_synonym"])["concept_synonym"].str.strip()

        for suga in suga_df_unique.to_list():
            
            if suga not in self._translation_df_list:
                self._translate_list.append(suga)

        # filter eng name in Korean        
        
        # sanjung start

        sanjung_df = self._suga_df.dropna(axis='index',how='any',subset=["sanjung_name"])

        sanjung_df_unique = sanjung_df.drop_duplicates(["sanjung_name"])["sanjung_name"].str.strip()
    
        for sanjung in sanjung_df_unique.to_list():
            
            if sanjung not in self._translation_df_list:
                self._translate_list.append(sanjung)
        
        if len(self._translate_list) != 0:
            self.translate_text()
            self._translation_df = pd.read_csv(self._translation_path)
        else:
            pass
        # return self._translate_list

    def data_translate(self):
        self.update_translation_csv()

        if (int(self._translation_df["concept_synonym"].duplicated().sum()) >0) == True:
            

            raise Exception("Korean names in the dictionary should be unique")

        self._translation_df.rename(columns={"concept_name":"concept_name_tr"}, inplace=True)

        merge_kr_suga = pd.merge(self._suga_df, self._translation_df, on="concept_synonym", how="left")

        merge_kr_suga["concept_name"] = merge_kr_suga.apply(lambda x : x["concept_name_tr"] if pd.isna(x["concept_name"]) else x["concept_name"], axis=1)

        # return merge_kr_suga
        self._translation_df.rename(columns={"concept_name_tr":"sanjung_tr","concept_synonym":"sanjung_name"}, inplace=True)

        merge_sanjung_kr_suga = pd.merge(merge_kr_suga, self._translation_df, on="sanjung_name", how="left")

        merge_sanjung_kr_suga["concept_name"] = merge_sanjung_kr_suga.apply(lambda x : str(x["concept_name"]) + "," + str(x["sanjung_tr"]) if pd.notna(x["sanjung_name"]) else x["concept_name"], axis=1)

        merge_sanjung_kr_suga["concept_synonym"] = merge_sanjung_kr_suga.apply(lambda x : str(x["concept_synonym"]) + "," + str(x["sanjung_name"]) if pd.notna(x["sanjung_name"]) else x["concept_synonym"], axis=1)

        result_suga = merge_sanjung_kr_suga[["concept_code", "concept_name", "concept_synonym", "domain_id", "vocabulary_id", "concept_class_id",
            "valid_start_date", "valid_end_date", "invalid_reason","ancestor_concept_code","previous_concept_code",
            "material","sanjung_name"]]
        
        return result_suga
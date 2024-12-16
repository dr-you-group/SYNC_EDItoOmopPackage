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


class DrugTransform:

    def __init__(self, drug_data, sheet_name, drug_code, drug_name, clinical_drug_code, previous_concept_code, company_name, value, unit, start_date):
        
        self._drug_data = drug_data
        self._sheet_name = sheet_name
        self._drug_code = drug_code
        self._drug_name = drug_name
        self._clinical_drug_code = clinical_drug_code
        self._previous_concept_code = previous_concept_code
        self._company_name = company_name
        self._value = value
        self._unit = unit
        self._start_date = start_date
        self._drug_df = None

    def data_transform(self):
            
        self._drug_df = pd.read_excel(self._drug_data, sheet_name=self._sheet_name)
        # return self._drug_df
        self._drug_df.rename(columns={self._drug_code: "concept_code",
                                self._drug_name : "concept_synonym",
                                self._clinical_drug_code: "ancestor_concept_code",
                                self._company_name : "company_name",
                                self._value : "value",
                                self._unit : "unit"
                                }, inplace=True)
        

        if self._previous_concept_code not in self._drug_df.columns:
            self._drug_df["previous_concept_code"] = None
        
        else:
            self._drug_df.rename(columns={self._previous_concept_code : "previous_concept_code"
                                    }, inplace=True)
        self._drug_df["concept_code"] = self._drug_df["concept_code"].astype(str)
        self._drug_df["concept_name"] = self._drug_df["concept_code"].str.strip()
        self._drug_df["company_name"] = self._drug_df["company_name"].astype(str)
        self._drug_df["value"] = self._drug_df["value"].astype(str)
        self._drug_df["unit"] = self._drug_df["unit"].astype(str)
        self._drug_df["concept_code"] = self._drug_df["concept_code"].apply(lambda x : "0" + x if len(x) == 8 else x)
        self._drug_df["domain_id"] = "Drug"
        self._drug_df["vocabulary_id"] = "EDI"
        self._drug_df["concept_class_id"] = "Drug Product"
        # drug_data["valid_start_date"] = drug_data["valid_start_date"].apply(lambda x: datetime.datetime.strptime("1970-01-01", "%Y-%m-%d") if pd.isna(x) else x)
        self._drug_df["valid_start_date"] = datetime.datetime.strptime(self._start_date, "%Y.%m.%d")
        self._drug_df["valid_end_date"] = datetime.datetime.strptime("2099-12-31", "%Y-%m-%d")
        self._drug_df["invalid_reason"] = None
        self._drug_df["material"] = None
        self._drug_df["sanjung_name"] = None
        # 문자열 양측 공백 제거
        self._drug_df["concept_code"] = self._drug_df["concept_code"].str.strip()
        self._drug_df["concept_synonym"] = self._drug_df["concept_synonym"].str.strip()

        # # 문자열에 공백만 있는 데이터가 있음. 이럴땐 Nan으로 처리가 안되어서 Nan으로 바꿔줘야함.
        self._drug_df["concept_code"] = self._drug_df["concept_code"].replace(r'^\s*$', None, regex=True)
        self._drug_df["concept_name"] = self._drug_df["concept_name"].replace(r'^\s*$', None, regex=True)
        self._drug_df["concept_synonym"] = self._drug_df["concept_synonym"].replace(r'^\s*$', np.nan, regex=True)

        self._drug_df.loc[pd.isna(self._drug_df["concept_synonym"]), "vocabulary_id"] = "KDC"
        self._drug_df.loc[self._drug_df["vocabulary_id"] == "KDC", "concept_class_id"] = "Clinical Drug"

        kdc_df = self._drug_df[self._drug_df["vocabulary_id"] =="KDC"]
        drug_name_df = kdc_df[["concept_name", "ancestor_concept_code"]]
        drug_name_df = drug_name_df.groupby('ancestor_concept_code')['concept_name'].agg(lambda x: ', '.join(x)).reset_index()
        bdg_df = self._drug_df[self._drug_df["vocabulary_id"] =="EDI"]
        bdg_df = bdg_df.drop(columns=["concept_name"])
        drug_data_df = pd.merge(bdg_df, drug_name_df, on ="ancestor_concept_code", how="left")
        drug_data_df = drug_data_df[~pd.isna(drug_data_df["concept_name"])]
        drug_data_df["concept_name"] = drug_data_df["concept_name"].apply(lambda x : x.encode('utf-8')[:255].decode("utf-8",'ignore') if len(x) >200 else x)
        drug_data_df["concept_synonym"] = drug_data_df["concept_synonym"].apply(lambda x : x.encode('utf-8')[:128].decode("utf-8",'ignore') if len(x) > 100 else x)
        drug_data_df["concept_name"] = drug_data_df["concept_name"].apply(lambda x : x.replace("   ", " "))
        drug_data_df["concept_name"] = drug_data_df["concept_name"].apply(lambda x : x.replace("  ", " "))

        # previous_concept_code에 소수점 데이터가 있어서 처리 (12341.0 -> 12341)
        # drug_data_df["previous_concept_code"] = drug_data_df["previous_concept_code"].apply(lambda x : None if pd.isna(x) == float else x)
        drug_data_df["previous_concept_code"] = drug_data_df["previous_concept_code"].apply(lambda x : str(x) if pd.notna(x) == True else x)
        
        drug_data_df = drug_data_df[["concept_code", "concept_name","concept_synonym","domain_id", "vocabulary_id", "concept_class_id",
                    "valid_start_date", "valid_end_date", "invalid_reason", "ancestor_concept_code" ,"previous_concept_code", "material", "sanjung_name", "company_name", "value", "unit"]]
        
        return drug_data_df
    
        
class DrugTranslate:

    def __init__(self, drug_df, translation_path, num_cores):
        self._drug_df = drug_df
        self._translation_path = translation_path
        self._translation_df = pd.read_csv(self._translation_path)
        self._translation_df_list = None
        self._translate_list = []
        self._num_cores = num_cores
        self.result_translate = multiprocessing.Manager().list()

    def data_save(self, result_df):

        result = pd.concat([self._translation_df, result_df],axis=0).drop_duplicates(["concept_name"], keep="first")
        save_path = "./result_translate_file"
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        result = result[["concept_name", "concept_synonym"]]

        result.to_csv("./result_translate_file/drug_translation.csv",index=False, encoding="utf-8")
        

    def multi_process(self, text):
        kor_check = re.sub(r"[^ㄱ-ㅣ가-힣\s]", "", text).strip()

        time.sleep(0.5)

        location = "global"
        project_id = "edi-translation"

        parent = f"projects/{project_id}/locations/{location}"

        client = translate.TranslationServiceClient()

        if len(kor_check) >0:

            response = client.translate_text(parent= parent, contents=[text],source_language_code="ko", target_language_code="en", mime_type ="text/plain")
            result_word = str(response.translations[0]).split('translated_text: "')[1].replace('"\n',"")
            decoded_text = codecs.escape_decode(result_word, 'unicode-escape')[0].decode('utf-8')
            self.result_translate.append([rf"{text}",rf'{decoded_text}'])


    def translate_text(self):

        len_translate_list = len(self._translate_list)

        # translate_list = list(set(self._translate_list))[:]

        for chunk in range(0, len_translate_list, 100):
            time.sleep(2)
            translate_list = list(set(self._translate_list))[chunk:chunk+100]
            # return translate_list
            pool = multiprocessing.Pool(self._num_cores)
            
            pool.map(self.multi_process, tqdm(translate_list))

            pool.close()
            pool.join()


            # return self._result_translate
            result_df = pd.DataFrame(list(self.result_translate))
            result_df.columns = ["concept_name","concept_synonym"]

            self.data_save(result_df)

    def update_translation_csv(self):

        self._translation_df_list = self._translation_df["concept_name"].to_list()

        drug_find_korean = self._drug_df.loc[self._drug_df['concept_name'].str.contains(r"[ㄱ-ㅣ가-힣]") == True, "concept_name"]

        drug_df_unique = drug_find_korean.drop_duplicates()

        for drug in drug_df_unique.to_list():
            if drug not in self._translation_df_list:
                self._translate_list.append(drug)

        # return self._translate_list
        if len(self._translate_list) != 0:
            self.translate_text()
            self._translation_df = pd.read_csv(self._translation_path)
        else:
            pass

    def data_translate(self):
        self.update_translation_csv()

        if (int(self._translation_df["concept_name"].duplicated().sum()) >0) == True:
            raise Exception("Korean names in the dictionary should be unique")

        self._translation_df.rename(columns={"concept_synonym":"concept_name_tr"}, inplace=True)

        merge_kr_drug = pd.merge(self._drug_df, self._translation_df, on="concept_name", how="left")
        merge_kr_drug["concept_name"] = merge_kr_drug.apply(lambda x : x["concept_name"] if pd.isna(x["concept_name_tr"]) else x["concept_name_tr"], axis=1)


        if merge_kr_drug.loc[merge_kr_drug['concept_name'].str.contains(r"[ㄱ-ㅣ가-힣]") == True, "concept_name"].count() > 0:
            raise Exception("concept name only allows English")

        result_drug = merge_kr_drug[["concept_code", "concept_name","concept_synonym","domain_id", "vocabulary_id", "concept_class_id",
                    "valid_start_date", "valid_end_date", "invalid_reason", "ancestor_concept_code" ,"previous_concept_code", "material", "sanjung_name", "company_name", "value", "unit"]]

        return result_drug
import pandas as pd
from google.cloud import translate
import re
import argparse
import logging
from tqdm import tqdm
import os
import numpy as np
import multiprocessing
import time
from multiprocessing import Manager


logger = logging.getLogger()
logger.setLevel(logging.INFO)



class SugaTrUpdate:
    def __init__(self):
        self._suga_df = None
        self._translation_df = None
        self._translation_df_list = None
        self._translate_list = []
        self.result_translate = multiprocessing.Manager().list()

        
    def data_save(self, result_df):
        
        result = pd.concat([self._translation_df, result_df],axis=0).drop_duplicates(["concept_synonym"])

        save_path = "./result_translate_file"
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        result = result[["concept_name", "concept_synonym"]]

        result.to_csv("./result_translate_file/suga_translate.csv",index=False, encoding="utf-8")
    

    def multi_process_suga(self, text):

        location = "global"
        project_id = "edi-translation"

        parent = f"projects/{project_id}/locations/{location}"

        client = translate.TranslationServiceClient()

        response = client.translate_text(parent= parent, contents=[text],source_language_code="ko", target_language_code="en", mime_type ="text/plain")
        result_word = str(response.translations[0]).split('translated_text: "')[1].replace('"\n',"")
        self.result_translate.append([rf"{result_word}",rf'{text}'])
        

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

    def update_translation_csv(self, suga_excel_path, sheet_name, suga_translation_path):
        
        self._suga_df = pd.read_excel(suga_excel_path ,sheet_name=sheet_name)
        self._translation_df = pd.read_csv(suga_translation_path)
        self._translation_df_list = self._translation_df["concept_synonym"].to_list()
        self._translate_list = []

        # concept_name start
        try:
            self._suga_df.loc[self._suga_df['영문명'].str.contains(r"[ㄱ-ㅣ가-힣]") == True, "영문명"] = np.nan

        except:
            pass
        
        suga_df_english_name = self._suga_df[self._suga_df["영문명"].isna()]

        suga_df_unique = suga_df_english_name.drop_duplicates(["한글명"])["한글명"].str.strip()

        for suga in suga_df_unique.to_list():
            
            if suga not in self._translation_df_list:
                self._translate_list.append(suga)

        # filter eng name in Korean        
        
        # sanjeong start

        sanjeong_df = self._suga_df.dropna(axis='index',how='any',subset=["산정명칭"])

        sanjeong_df_unique = sanjeong_df.drop_duplicates(["산정명칭"])["산정명칭"].str.strip()
    
        for sangeong in sanjeong_df_unique.to_list():
            
            if sangeong not in self._translation_df_list:
                self._translate_list.append(sangeong)
        
        # return self._translate_list
        return self.translate_text()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--file_path",type=str)          
    args = parser.parse_args()

    SugaTrUpdate().update_translation_csv(suga_excel_path=f"{args.file_path}", sheet_name="result", suga_translation_path="./result_translate_file/suga_translate.csv")




import pandas as pd
# from google.cloud import translate_v2 as translate
from google.cloud import translate
import re
import argparse
import logging
from tqdm import tqdm
import os
import multiprocessing
import time
from multiprocessing import Manager

logger = logging.getLogger()
logger.setLevel(logging.INFO)



class DeviceTrUpdate:
    def __init__(self):
        self._device_df = None
        self._translation_df = None
        self._translation_df_list = None
        self._translate_list = []
        self.result_translate = multiprocessing.Manager().list()
    def data_save(self, result_df):

        
        result = pd.concat([self._translation_df, result_df],axis=0).drop_duplicates(["concept_name"], keep="first")
        save_path = "./result_translate_file"
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        result = result[["concept_name", "concept_synonym"]]

        result.to_csv("./result_translate_file/device_translation.csv",index=False, encoding="utf-8")
        

    def multi_process_test(self, text):
        kor_check = re.sub(r"[^ㄱ-ㅣ가-힣\s]", "", text).strip()

        time.sleep(0.5)

        location = "global"
        project_id = "edi-translation"

        parent = f"projects/{project_id}/locations/{location}"

        client = translate.TranslationServiceClient()

        if len(kor_check) >0:

            response = client.translate_text(parent= parent, contents=[text],source_language_code="ko", target_language_code="en", mime_type ="text/plain")
            result_word = str(response.translations[0]).split('translated_text: "')[1].replace('"\n',"")
            self.result_translate.append([rf"{text}",rf'{result_word}'])


        else:

            response = client.translate_text(parent= parent, contents=[text],source_language_code="en", target_language_code="ko", mime_type ="text/plain")
            result_word = str(response.translations[0]).split('translated_text: "')[1].replace('"\n',"")
            self.result_translate.append([rf"{text}",rf'{result_word}'])


    def translate_text(self):

            len_translate_list = len(self._translate_list)

            # translate_list = list(set(self._translate_list))[:]

            for chunk in range(0, len_translate_list, 100):
                time.sleep(2)
                translate_list = list(set(self._translate_list))[chunk:chunk+100]
                # return translate_list
                pool = multiprocessing.Pool(10)
                
                pool.map(self.multi_process_test, tqdm(translate_list))

                pool.close()
                pool.join()


                # return self._result_translate
                result_df = pd.DataFrame(list(self.result_translate))
                result_df.columns = ["concept_name","concept_synonym"]

                self.data_save(result_df)

    def update_translation_csv(self, device_excel_path, sheet_name, device_translation_path):

        self._device_df = pd.read_excel(device_excel_path, sheet_name=sheet_name)
        self._translation_df = pd.read_csv(device_translation_path)
        self._translation_df_list = self._translation_df["concept_name"].to_list()


        device_df_unique = self._device_df.drop_duplicates(["품 명"])["품 명"].str.strip()


        for device in device_df_unique.to_list():
            if device not in self._translation_df_list:
                self._translate_list.append(device)

        logger.info(self._translate_list,"traslate 개수")

        self.translate_text()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--file_path",type=str)          
    args = parser.parse_args()

    DeviceTrUpdate().update_translation_csv(device_excel_path = f"{args.file_path}", sheet_name = "result", device_translation_path= "./result_translate_file/device_translation.csv")



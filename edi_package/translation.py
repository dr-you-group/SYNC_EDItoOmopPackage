import pandas as pd
from google.cloud import translate_v2 as translate
import re
import logging
from tqdm import tqdm
import os
import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class SugaTrUpdate:
    def __init__(self):
        self._suga_df = None
        self._translation_df = None
        self._translation_df_list = None
        self._translate_list = []

    def data_save(self, result_df):
        
        result = pd.concat([self._translation_df, result_df],axis=0).drop_duplicates(["concept_synonym"])

        save_path = "./result_translate_file"
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        result = result[["concept_name", "concept_synonym"]]

        result.to_csv("./result_translate_file/suga_translate.csv",index=False, encoding="utf-8")
        
    
    def translate_text(self):
        result_translate = []
        
        translate_list = list(set(self._translate_list))

        location = "global"
        project_id = "edi-translation"

        parent = f"projects/{project_id}/locations/{location}"

        client = translate.TranslationServiceClient()

        for text in tqdm(translate_list):

            response = client.translate_text(parent= parent, contents=[text],source_language_code="ko", target_language_code="en", mime_type ="text/plain")
            result_word = str(response.translations[0]).split('translated_text: "')[1].replace('"\n',"")
            self.result_translate.append([rf"{text}",rf'{result_word}'])

        result_df = pd.DataFrame(result_translate)
        result_df.columns = self._translation_df.columns
        self.data_save(result_df)

    def update_translation_csv(self, suga_excel_path, sheet_name, suga_translation_path):
        
        self._suga_df = pd.read_excel(suga_excel_path ,sheet_name=sheet_name)
        self._translation_df = pd.read_csv(suga_translation_path)
        self._translation_df_list = self._translation_df["concept_synonym"].to_list()
        self._translate_list = []

        # concept_name start

        suga_df_english_name = self._suga_df[self._suga_df["영문명"].isna()]

        suga_df_unique = suga_df_english_name.drop_duplicates(["한글명"])["한글명"].str.strip()


        for suga in suga_df_unique.to_list():
            
            if suga not in self._translation_df_list:
                self._translate_list.append(suga)

        # sanjeong start
        
        sanjeong_df = self._suga_df.dropna(axis='index',how='any',subset=["산정명칭"])

        sanjeong_df_unique = sanjeong_df.drop_duplicates(["산정명칭"])["산정명칭"].str.strip()
    
        for sangeong in sanjeong_df_unique.to_list():
            
            if sangeong not in self._translation_df_list:
                self._translate_list.append(sangeong)
        
        self.translate_text()



class DeviceTrUpdate:
    def __init__(self):
        self._device_df = None
        self._translation_df = None
        self._translation_df_list = None
        self._translate_list = []

    def data_save(self, result_df):

        
        result = pd.concat([self._translation_df, result_df],axis=0).drop_duplicates(["concept_name"])
        save_path = "./result_translate_file"
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        result = result[["concept_name", "concept_synonym"]]

        result.to_csv("./result_translate_file/device_translation.csv",index=False, encoding="utf-8")
        
    def translate_text(self):
        result_translate = []
        translate_list = list(set(self._translate_list))
        for text in tqdm(translate_list):

            kor_check = re.sub(r"[^ㄱ-ㅣ가-힣\s]", "", text).strip()

            if len(kor_check) >0:

                if isinstance(text, bytes):
                    text = text.decode("utf-8")
                translate_client = translate.Client() 

                result = translate_client.translate(text, target_language="en")
                result_translate.append([rf"{text}",rf'{result["translatedText"]}'])

            else:
                
                if isinstance(text, bytes):
                    text = text.decode("utf-8")
                translate_client = translate.Client()

                result = translate_client.translate(text, target_language="ko")
                
                result_translate.append([text,result["translatedText"]])

        result_df = pd.DataFrame(result_translate)
        result_df.columns = ["concept_name","concept_synonym"]


        self.data_save(result_df)



    def update_translation_csv(self, device_excel_path, sheet_name, device_translation_path):

        self._device_df = pd.read_excel(device_excel_path, sheet_name=sheet_name)
        self._translation_df = pd.read_csv(device_translation_path)
        self._translation_df_list = self._translation_df["concept_name"].to_list()


        logger.info("프로그램을 시작합니다")

        device_df_unique = self._device_df.drop_duplicates(["품 명"])["품 명"].str.strip()


        for device in device_df_unique.to_list():
            if device not in self._translation_df_list:
                self._translate_list.append(device)

        self.translate_text()


class DeviceSplit:

    def __init__(self, device_data, del_device_data, date):
        self._device_data = pd.read_excel(f"{device_data}", None)
        self._del_device_data = pd.read_excel(f"{del_device_data}", None)
        self._result_no_delite_list = []
        self._result_delite_list = []
        self._date = date

    def start(self):
        self.device_process()
        self.del_device_process()
        self.save_data()

    def device_process(self):
        data_columns = self._device_data.keys()

        for col in data_columns:
            data_list = self._device_data[col].values.tolist()
            for append_data in data_list:
                if append_data[0] !="코드" and pd.notna(append_data[0]) and pd.notna(append_data[3]) and pd.notna(append_data[-3]) and pd.notna(append_data[-2]):
                    

                    if isinstance(append_data[-3] , datetime.datetime) == True:
                        append_data[-3] = datetime.datetime.strftime(append_data[-3], "%Y-%m-%d")
                    
                    if isinstance(append_data[-2] , datetime.datetime) == True:
                        append_data[-2] = datetime.datetime.strftime(append_data[-2], "%Y-%m-%d")

                    try:

                        self._result_no_delite_list.append({"코 드":append_data[0],
                                            "품 명":append_data[3],
                                            "재 질":append_data[7],
                                            "최초등재일자": datetime.datetime.strptime(str(append_data[-3]), "%Y-%m-%d"),
                                            "적용일자": datetime.datetime.strptime(str(append_data[-2]), "%Y-%m-%d")
                                            })             
                    except:
                        pass

    def del_device_process(self):
            
        data_columns = self._del_device_data.keys()

        for col in data_columns:

            del_data_list = self._del_device_data[col]

            del_col = del_data_list.columns[0]

            del_numpy = del_data_list.values.tolist()

            len_del = len(del_data_list[del_col])

            end_count = 0
            target_domain = None

            while end_count < len_del:

                if del_numpy[end_count][0] == "급여중지":
                    target_domain = "급여중지"
                
                if del_numpy[end_count][0] == "삭제":
                    target_domain = "삭제"

                if target_domain =="급여중지" and del_numpy[end_count][0] !="코드" and pd.notna(del_numpy[end_count][0]) \
                    and pd.notna(del_numpy[end_count][3]) and pd.notna(del_numpy[end_count][-3]) and pd.notna(del_numpy[end_count][-2]):


                    if isinstance(del_numpy[end_count][-3] , datetime.datetime) == True:
                        del_numpy[end_count][-3] = datetime.datetime.strftime(del_numpy[end_count][-3], "%Y-%m-%d")
                    
                    if isinstance(del_numpy[end_count][-2] , datetime.datetime) == True:
                        del_numpy[end_count][-2] = datetime.datetime.strftime(del_numpy[end_count][-2], "%Y-%m-%d")

                    try:

                        self._result_no_delite_list.append({"코 드":del_numpy[end_count][0],
                                            "품 명":del_numpy[end_count][3],
                                            "재 질":del_numpy[end_count][7],
                                            "최초등재일자": datetime.datetime.strptime(str(del_numpy[end_count][-3]), "%Y-%m-%d"),
                                            "적용일자": datetime.datetime.strptime(str(del_numpy[end_count][-2]), "%Y-%m-%d")
                                            })             
                    except:
                        pass

                if target_domain =="삭제" and del_numpy[end_count][0] !="코드" and pd.notna(del_numpy[end_count][0]) \
                    and pd.notna(del_numpy[end_count][3]) and pd.notna(del_numpy[end_count][-3]) and pd.notna(del_numpy[end_count][-2]):


                    if isinstance(del_numpy[end_count][-3] , datetime.datetime) == True:
                        del_numpy[end_count][-3] = datetime.datetime.strftime(del_numpy[end_count][-3], "%Y-%m-%d")
                    
                    if isinstance(del_numpy[end_count][-2] , datetime.datetime) == True:
                        del_numpy[end_count][-2] = datetime.datetime.strftime(del_numpy[end_count][-2], "%Y-%m-%d")

                    try:

                        self._result_delite_list.append({"코 드":del_numpy[end_count][0],
                                            "품 명":del_numpy[end_count][3],
                                            "재 질":del_numpy[end_count][7],
                                            "최초등재일자": datetime.datetime.strptime(str(del_numpy[end_count][-3]), "%Y-%m-%d"),
                                            "적용일자": datetime.datetime.strptime(str(del_numpy[end_count][-2]), "%Y-%m-%d")
                                            })             
                    except:
                        pass

                end_count +=1

    def save_data(self):

        save_path = "./device_split"
        if not os.path.exists(save_path):
            os.makedirs(save_path)

        result_no_delite = pd.DataFrame(self._result_no_delite_list)
        result_no_delite.to_excel(f"./device_split/result_no_delite_device_{self._date}.xlsx",index=False,sheet_name="result", header=True)

        result_delite = pd.DataFrame(self._result_delite_list)
        result_delite.to_excel(f"./device_split/result_delite_device_{self._date}.xlsx", index=False,sheet_name="result", header=True)



    
















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



    
class DeviceTransform:

    def __init__(self, device_data, del_device_data, device_code, device_name, start_date_name,
                end_date_name, material_name, date):
        self._device_data = pd.read_excel(f"{device_data}", None)
        self._del_device_data = pd.read_excel(f"{del_device_data}", None)
        self._delite_device_list = []
        self._device_list = []
        self._delete_device_df = None
        self._device_df =None
        self._device_code = device_code
        self._device_name = device_name
        self._start_date_name = start_date_name
        self._end_date_name = end_date_name
        self._material_name = material_name
        self._date = date

    def data_transform(self):
        self.device_process()
        self.del_device_process()
        self.make_device_df()
        self.device_transform()
        self.del_device_transform()
        return self.merge_device()
    
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

                        self._device_list.append({"코 드":append_data[0],
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

                        self._device_list.append({"코 드":del_numpy[end_count][0],
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

                        self._delite_device_list.append({"코 드":del_numpy[end_count][0],
                                            "품 명":del_numpy[end_count][3],
                                            "재 질":del_numpy[end_count][7],
                                            "최초등재일자": datetime.datetime.strptime(str(del_numpy[end_count][-3]), "%Y-%m-%d"),
                                            "적용일자": datetime.datetime.strptime(str(del_numpy[end_count][-2]), "%Y-%m-%d")
                                            })             
                    except:
                        pass

                end_count +=1


    def del_device_transform(self):
        
        self._delete_device_df.rename(columns={self._device_code : "concept_code",
                                        self._device_name : "concept_name",
                                        self._start_date_name : "valid_start_date",
                                        self._end_date_name : "valid_end_date",
                                        self._material_name : "material"}, inplace=True)

        self._delete_device_df["domain_id"] = "Device"
        self._delete_device_df["vocabulary_id"] = "EDI"
        self._delete_device_df["concept_class_id"] = "Device"
        self._delete_device_df["valid_start_date"] = self._delete_device_df["valid_start_date"].apply(lambda x: datetime.datetime.strptime("1970-01-01", "%Y-%m-%d") if pd.isna(x) else x)
        self._delete_device_df["invalid_reason"] = "D"
        self._delete_device_df["ancestor_concept_code"] = np.nan
        self._delete_device_df["previous_concept_code"] = np.nan
        self._delete_device_df["sanjung_name"] = np.nan

        # 문자열 양측 공백 제거
        self._delete_device_df["concept_code"] = self._delete_device_df["concept_code"].astype('str').str.strip()
        self._delete_device_df["concept_name"] = self._delete_device_df["concept_name"].astype('str').str.strip()


        # 문자열에 공백만 있는 데이터가 있음. 이럴땐 Nan으로 처리가 안되어서 Nan으로 바꿔줘야함.
        self._delete_device_df["concept_code"] = self._delete_device_df["concept_code"].replace(r'^\s*$', np.nan, regex=True)
        self._delete_device_df["concept_name"] = self._delete_device_df["concept_name"].replace(r'^\s*$', np.nan, regex=True)


        self._delete_device_df = self._delete_device_df[["concept_code", "concept_name", "domain_id", "vocabulary_id", "concept_class_id",
                    "valid_start_date", "valid_end_date", "invalid_reason","ancestor_concept_code","previous_concept_code",
                    "material","sanjung_name"]]


    def device_transform(self):

        self._device_df.rename(columns={self._device_code : "concept_code",
                                        self._device_name : "concept_name",
                                        self._start_date_name : "valid_start_date",
                                        self._material_name : "material"}, inplace=True)
        
        self._device_df["domain_id"] = "Device"
        self._device_df["vocabulary_id"] = "EDI"
        self._device_df["concept_class_id"] = "Device"
        self._device_df["valid_start_date"] = self._device_df["valid_start_date"].apply(lambda x: datetime.datetime.strptime("1970-01-01", "%Y-%m-%d") if pd.isna(x) else x)
        self._device_df["valid_end_date"] = datetime.datetime.strptime("2099-12-31", "%Y-%m-%d")
        self._device_df["invalid_reason"] = np.nan
        self._device_df["ancestor_concept_code"] = np.nan
        self._device_df["previous_concept_code"] = np.nan
        self._device_df["sanjung_name"] = np.nan

        # 문자열 양측 공백 제거
        self._device_df["concept_code"] = self._device_df["concept_code"].str.strip()
        self._device_df["concept_name"] = self._device_df["concept_name"].str.strip()

        # 문자열에 공백만 있는 데이터가 있음. 이럴땐 Nan으로 처리가 안되어서 Nan으로 바꿔줘야함.
        self._device_df["concept_code"] = self._device_df["concept_code"].replace(r'^\s*$', np.nan, regex=True)
        self._device_df["concept_name"] = self._device_df["concept_name"].replace(r'^\s*$', np.nan, regex=True)


        self._device_df = self._device_df[["concept_code", "concept_name", "domain_id", "vocabulary_id", "concept_class_id",
                    "valid_start_date", "valid_end_date", "invalid_reason","ancestor_concept_code","previous_concept_code",
                    "material","sanjung_name"]]

    def make_device_df(self):

        self._delete_device_df = pd.DataFrame(self._delite_device_list)
        self._device_df = pd.DataFrame(self._device_list)

    def merge_device(self):
        
        data = np.concatenate([self._device_df, self._delete_device_df],axis=0)

        col = ["concept_code", "concept_name", "domain_id", "vocabulary_id", "concept_class_id",
                            "valid_start_date", "valid_end_date", "invalid_reason","ancestor_concept_code","previous_concept_code",
                            "material","sanjung_name"]
        result_device = pd.DataFrame(data, columns=col)

        return result_device

class DeviceTranslate:

    def __init__(self, device_df, translation_path):
        self._device_df = device_df
        self._translation_df = pd.read_csv(translation_path)
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


        else:

            response = client.translate_text(parent= parent, contents=[text],source_language_code="en", target_language_code="ko", mime_type ="text/plain")
            result_word = str(response.translations[0]).split('translated_text: "')[1].replace('"\n',"")
            decoded_text = codecs.escape_decode(result_word, 'unicode-escape')[0].decode('utf-8')
            self.result_translate.append([rf"{text}",rf'{decoded_text}'])


    def translate_text(self):

            len_translate_list = len(self._translate_list)


            for chunk in range(0, len_translate_list, 100):
                time.sleep(2)
                translate_list = list(set(self._translate_list))[chunk:chunk+100]
                # return translate_list
                pool = multiprocessing.Pool(10)
                
                pool.map(self.multi_process, tqdm(translate_list))

                pool.close()
                pool.join()


                # return self._result_translate
                result_df = pd.DataFrame(list(self.result_translate))
                result_df.columns = ["concept_name","concept_synonym"]

                self.data_save(result_df)

    def update_translation_csv(self):

        self._translation_df_list = self._translation_df["concept_name"].to_list()


        device_df_unique = self._device_df.drop_duplicates(["concept_name"])["concept_name"].str.strip()

        for device in device_df_unique.to_list():
            if device not in self._translation_df_list:
                self._translate_list.append(device)

        self.translate_text()


    def data_translate(self):

        self.update_translation_csv()
        translate = self._translation_df
        if (int(translate["concept_name"].duplicated().sum()) >0) == True:
            raise Exception("Korean names in the dictionary should be unique")


        result_df = pd.merge(self._device_df, translate, on="concept_name", how= "left")
        
        find_korean = result_df['concept_name'].str.contains(r"[ㄱ-ㅣ가-힣]") == True

        result_df.loc[find_korean, ['concept_name', 'concept_synonym']] = result_df.loc[find_korean, ['concept_synonym', 'concept_name']].values

        result_df = result_df[["concept_code", "concept_name","concept_synonym","domain_id", "vocabulary_id", "concept_class_id",
                    "valid_start_date", "valid_end_date", "invalid_reason","ancestor_concept_code","previous_concept_code",
                    "material","sanjung_name"]]
        
        result_df["concept_name"].replace('\n', '').replace('\r', '')
        result_df["concept_synonym"].replace('\n', '').replace('\r', '')

        return result_df
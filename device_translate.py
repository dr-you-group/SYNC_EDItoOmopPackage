# import pandas as pd
# from google.cloud import translate_v2 as translate
# import re
# import argparse
# import logging
# from tqdm import tqdm
# import multiprocessing

# logger = logging.getLogger()
# logger.setLevel(logging.INFO)

# parser = argparse.ArgumentParser()


# parser.add_argument("-d","--date",type=str,help="add date")

# args = parser.parse_args()


# class EdiTranslation:
#     def __init__(self):
#         self._device_df = pd.read_excel(f"./Device2023.{args.date}(merge).xlsx",sheet_name="result")
#         self._translation_df = pd.read_csv("./device_translation.csv")
#         self._translation_df_list = self._translation_df["concept_name"].to_list()
#         self._translate_list = []

#     def start_program(self):

#         self.synonym_translate()
#         self.translate_text()


#     def data_save(self, result_df):

        
#         result = pd.concat([self._translation_df, result_df],axis=0).drop_duplicates(["concept_name"])
#         result.to_csv("./device_translation.csv",index=False, encoding="utf-8")
        
#     def translate_text(self):
#         result_translate = []
        
#         translate_list = list(set(self._translate_list))
#         result_list = multiprocessing.Manager().list()

#         for text in tqdm(translate_list):


#             kor_check = re.sub(r"[^ㄱ-ㅣ가-힣\s]", "", text).strip()

#             if len(kor_check) >= 1:

#                 if isinstance(text, bytes):
#                     text = text.decode("utf-8")
#                 translate_client = translate.Client()

#                 result = translate_client.translate(text, target_language="en")
                
#                 result_translate.append([text,result["translatedText"]])

#             else:
                
#                 if isinstance(text, bytes):
#                     text = text.decode("utf-8")
#                 translate_client = translate.Client()

#                 result = translate_client.translate(text, target_language="ko")
                
#                 result_translate.append([text,result["translatedText"]])

#         result_df = pd.DataFrame(result_translate)
#         result_df.columns = self._translation_df.columns
#         self.data_save(result_df)

#     def synonym_translate(self):
#         logger.info("프로그램을 시작합니다")

#         device_df_unique = self._device_df.drop_duplicates(["품 명"])["품 명"]


#         for device in device_df_unique.to_list():
#             if device not in self._translation_df_list:
#                 self._translate_list.append(device)

#         logger.info("한글 데이터 파싱이 끝났습니다.")
#         logger.info("한글 데이터의 개수는 ",len(self._translate_list),"입니다")
# start = EdiTranslation()

# print(start.start_program())

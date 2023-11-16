import pandas as pd
import numpy as np
import subprocess
import os
from edi_package.translation import SugaTrUpdate
from edi_package.edi_process import Process
from edi_package.sql_process import DBConnect
import datetime
from dateutil.relativedelta import relativedelta
import openpyxl


data = os.listdir("./dataset/Suga/")
connect = DBConnect(host="",port='', user="", password="",database="EDI_OMOP")

sheet = ["의치과_급여_전체", "의치과_100대100_전체","의치과_비급여_전체"]

start =0
for file_name in data:
    if "Suga2022.06.01.xlsx" == file_name:
        start =1


    if start != 1:
        continue
    date_split = file_name.split(".")
    year = date_split[0].replace("Suga","")
    month = date_split[1]
    result_date = f"{year}.{month}.01"
    print(result_date)
    end_date = datetime.datetime.strptime(result_date, "%Y.%m.%d") + relativedelta(months=1)
    end_date = end_date.strftime("%Y.%m.%d")
  
    Process().merge_suga(file_path = f"./dataset/Suga/{file_name}", sheet_name = sheet, date = result_date)


    subprocess.run(["python", "./multiprocess_translate_suga.py","--date",result_date], shell=True)

    suga = Process().suga_process(file_path=f"./dataset/merge/merge_suga_{result_date}.xlsx",
                                sheet_name= "result",
                                suga_code="수가코드",
                                korean_name="한글명",
                                english_name="영문명",
                                start_date_name="적용일자",
                                sanjung_name="산정명칭",
                                translate_path="./result_translate_file/suga_translate.csv")

    connect.update_data(database="EDI_OMOP", table="edi_voca_table",domain = "'Procedure','Measurement'", data = suga, date= end_date)

from edi_package.translation import SugaTrUpdate
from edi_package.edi_process import Process
from edi_package.sql_process import DBConnect

import os
import datetime
from dateutil.relativedelta import relativedelta


data = os.listdir("./dataset/Drug/")


connect = DBConnect(host="", user="", password="",database="EDI_OMOP")


start = 0
for file_name in data:

    if "Drug2021.02.01.xlsx" == file_name:
        start =1


    if start != 1:
        continue
    
    date_split = file_name.split(".")

    year = date_split[0].replace("Drug","")

    month = date_split[1]

    result_date = f"{year}-{month}-01"
    print(result_date)
    drug = Process().drug_process(file_path=f"./dataset/Drug/{file_name}", sheet_name=0, drug_code="제품코드", drug_name="제품명", clinical_drug_code = "주성분코드",previous_concept_code = "목록정비전코드")


    connect.update_data(database="EDI_OMOP", table="edi_voca_table",domain = "'Drug'", data = drug, date = result_date)


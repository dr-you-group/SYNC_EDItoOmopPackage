from edi_package.translation import SugaTrUpdate
from edi_package.edi_process import Process
from edi_package.edi_process import DeviceSplit
from edi_package.sql_process import DBConnect
import subprocess


connect = DBConnect(host="",port='', user="", password="",database="EDI_OMOP")


# device


# Device는 2개의 CSV 파일이 있습니다. (급여 진행 중인 것 / 급여 삭제 된 것)
# 두개의 파일을 넣으면 ./dataset/device_split 경로에 result로 결과물이 나옵니다.


DeviceSplit(device_data="./dataset/Device/Device2023.10.01.xlsx", del_device_data="./dataset/Device/Device2023.10.01_급여중지삭제.xlsx", date="20231001").start()



# Device CSV 내 한글명을 영문명으로 translate 해주는게 필요합니다. (Google Translate를 사용 하였음. 사용하려면 API 권한이 필요합니다.)
# 번역된 결과물은 ./result_translate_file 내에 도메인 별 CSV로 저장되어 있습니다.
# 빠른 API 사용을 위해 Multiprocess를 활용하였습니다. 
# file_path에 translate 하고자 하는 device 파일 경로명을 입력하세요. (DeviceSplit 클래스를 통해 나온 결과물 파일 경로를 넣으세요.)

no_del_file_path = ""
subprocess.run(["python", "./multiprocess_translate_device.py","--file_path", no_del_file_path], shell=True)

del_file_path = ""
subprocess.run(["python", "./multiprocess_translate_device.py","--file_path", del_file_path], shell=True)


device = Process().device_process(file_path=del_file_path,
                        sheet_name="result",
                        device_code="코 드",
                        device_name="품 명",
                        start_date_name="최초등재일자",
                        material_name="재 질",
                        translate_path = "./result_translate_file/device_translation.csv"
                        )

del_device = Process().del_device_process(file_path=no_del_file_path,
                        sheet_name="result",
                        device_code="코 드",
                        device_name="품 명",
                        start_date_name="최초등재일자",
                        end_date_name="적용일자",
                        material_name="재 질",
                        translate_path = "./result_translate_file/device_translation.csv"
                        )


device_result = Process().merge_device(device,del_device)

connect.update_device(database="EDI_OMOP", table="edi_voca_table", result_device=device_result)


# suga

suga_file_path = "./dataset/Suga/Suga2023.10.01.xlsx"
suga_date = "2023.10.01"
merge_suga_file_path = f"./dataset/Suga_merge/merge_suga_{suga_date}.xlsx"

# Suga는 sheet name이 일정하지 않아서 사용자가 확인 후 추가를 하시면 됩니다.
sheet = ["의치과_급여_전체", "의치과_100대100_전체", "의치과_비급여_전체"]


Process().merge_suga(file_path = suga_file_path, sheet_name = sheet, date = suga_date)

subprocess.run(["python", "./multiprocess_translate_suga.py","--file_path",merge_suga_file_path], shell=True)

suga = Process().suga_process(file_path=merge_suga_file_path,
                            sheet_name= "result",
                            suga_code="수가코드",
                            korean_name="한글명",
                            english_name="영문명",
                            start_date_name="적용일자",
                            sanjung_name="산정명칭",
                            translate_path="./result_translate_file/suga_translate.csv")

connect.update_data(database="EDI_OMOP", table="edi_voca_table",domain = "'Procedure','Measurement'", data = suga, date= suga_date)

# drug

drug_file_name = "./dataset/Drug/Drug2023.10.01.xlsx"
drug_date = "2023.10.01"

drug = Process().drug_process(file_path=drug_file_name,
                            sheet_name=0,
                            drug_code="제품코드",
                            drug_name="제품명",
                            clinical_drug_code="주성분코드",
                            previous_concept_code = "목록정비전코드")


connect.update_data(database="EDI_OMOP", table="edi_voca_table",domain = "'Drug'", data = drug, date = drug_date)

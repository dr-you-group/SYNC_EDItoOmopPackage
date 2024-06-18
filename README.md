# EDItoOmopPackage
  
jupyter를 이용해서 run_edi_package.ipynb 를 사용하는것을 권장 드립니다.  

## Prepare  
``` python
PIP LIST
tqdm==4.66.1  
sqlalchemy==2.0.20  
pymssql==2.2.11  
pandas==2.1.0  
openpyxl==3.1.2  
```  
**GOOGLE TRANSLATION**  
EDI concept name에 대해서 한글을 영문으로 바꾸어야 합니다. 이에 패키지는 Google Translation을 사용 합니다.  
Google Translation을 사용 할 수 있는 API KEY를 준비 해주세요. 

**GOOGLE TRANSLATION URL**  
  
https://cloud.google.com/sdk/docs/install?authuser=2&hl=ko  

**GOOGLE TRANSLATION Install**  
  
`pip install google-cloud-translate==2.0.1`  
  
`gcloud init`  
  
`gcloud auth application-default login`  

## How to Use 

``` python

host : 서버 IP
port = 서버DB port
user = DB ID
password = DB PASSWORD
database = EDI DB name

connect = DBConnect(host="",port="", user="", password="",database="")


Transform() -> 고시파일을 OMOP TABLE 형태로 변환
Translate() -> 영문 변환이 필요한 데이터를 Google Translate를 사용하여 변환시킴
update() -> DataBase에 적재

update_drug_relationship() -> Drug 주성분코드 추적을 위한 Table
```

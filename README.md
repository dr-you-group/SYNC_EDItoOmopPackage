# EDItoOmopPackage
  
jupyter를 이용해서 run_edi_package.ipynb 를 사용하는것을 권장 드립니다.  

### Prepare

EDI concept name에 대해서 한글을 영문으로 바꾸어야 합니다. 이에 패키지는 Google Translation을 사용 합니다.  
Google Translation을 사용 할 수 있는 API KEY를 준비 해주세요. 

**GOOGLE TRANSLATION URL**  
  
https://cloud.google.com/sdk/docs/install?authuser=2&hl=ko  

**Install**  
  
`pip install google-cloud-translate==2.0.1`  
  
`gcloud init`  
  
`gcloud auth application-default login`  

### How to Use 

``` python

host : 서버 IP
port = 서버DB port
user = DB ID
password = DB PASSWORD
database = EDI DB name

connect = DBConnect(host="",port="", user="", password="",database="")


DeviceTransform().data_transform() -> 고시 CSV를 OMOP 형태로 변환해줍니다.
DeviceTranslate().data_translate() -> concept_name 열에서 한글을 영어로 변환해줍니다. (translation csv 저장 후 concept_synonym에 변환)
connect.update_device() -> 변환이 완료 된 omop을 DB에 업데이트 합니다.

```

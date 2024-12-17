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
- num_cores 옵션은 Google Translate를 멀티프로세싱으로 처리하기 위함 (프로그램을 실행하는 컴퓨터 Core 개수에 따라 적절하게 사용)
update() -> DataBase에 적재
update_drug_relationship() -> Drug 주성분코드 추적을 위한 Table
```

## 고시 다운로드

https://www.hira.or.kr/rc/insu/insuadtcrtr/InsuAdtCrtrList.do?pgmid=HIRAA030069000400&WT.gnb=%EB%B3%B4%ED%97%98%EC%9D%B8%EC%A0%95%EA%B8%B0%EC%A4%80

-  Device(키워드 : 치료재료), Suga(키워드 : 수가)는 여기서 다운로드 받으시면 됩니다

https://www.hira.or.kr/bbsDummy.do?pgmid=HIRAA030014050000&brdScnBltNo=4&brdBltNo=1676&pageIndex=1&pageIndex2=1

- Drug는 여기서 다운로드 받으시면 됩니다.

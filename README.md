# SYNC_EDItoOmopPackage

We recommend using run_edi_package.ipynb with Jupyter for convenience.
Please use Python version 3.12 or lower.

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

For EDI concept names, you need to translate Korean to English. This package uses Google Translation.
Please prepare an ID that can use Google Translation.

**GOOGLE TRANSLATION URL**  
  
https://cloud.google.com/sdk/docs/install?authuser=2&hl=ko  

**GOOGLE TRANSLATION Install**  
  
`pip install google-cloud-translate==2.0.1`  
  
`gcloud init`  
  
`gcloud auth application-default login`  

## How to Use 

``` python

host : Server IP
port = Server DB port
user = DB ID
password = DB PASSWORD
database = EDI DB name

connect = DBConnect(host="",port="", user="", password="",database="")


Transform() -> Converts notification files to OMOP TABLE format
Translate() -> Translates data requiring English conversion using Google Translate
- The num_cores option enables multiprocessing for Google Translate (use appropriately according to the number of cores on the computer running the program)
update() -> Loads into the Database
update_drug_relationship() -> Table for tracking Drug main ingredient codes
```

## 고시 다운로드

https://www.hira.or.kr/rc/insu/insuadtcrtr/InsuAdtCrtrList.do?pgmid=HIRAA030069000400&WT.gnb=%EB%B3%B4%ED%97%98%EC%9D%B8%EC%A0%95%EA%B8%B0%EC%A4%80

-  Device (keyword: 치료재료/medical materials) and Suga (keyword: 수가/medical fee) can be downloaded from here

https://www.hira.or.kr/bbsDummy.do?pgmid=HIRAA030014050000&brdScnBltNo=4&brdBltNo=1676&pageIndex=1&pageIndex2=1

- Drug can be downloaded from here

from edi_package.device import DeviceTransform,DeviceTranslate
from edi_package.suga import SugaTransform,SugaTranslate
from edi_package.drug import DrugTransform,DrugTranslate
from edi_package.sql_process import DBConnect

connect = DBConnect(host="",port="", user="", password="",database="")
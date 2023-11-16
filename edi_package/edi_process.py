import pandas as pd
import datetime
import logging
import os

import numpy as np
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Process:

    def del_device_process(self, file_path, translate_path, sheet_name, start_date_name, end_date_name, material_date = None , device_code = None,
                    device_name = None, material_name = None):
        
        device_data = pd.read_excel(file_path,sheet_name=sheet_name)
        device_data.rename(columns={device_code : "concept_code",
                                        device_name : "concept_name",
                                        start_date_name : "valid_start_date",
                                        end_date_name : "valid_end_date",
                                        material_name : "material"}, inplace=True)

        device_data["domain_id"] = "Device"
        device_data["vocabulary_id"] = "EDI"
        device_data["concept_class_id"] = "Device"
        device_data["concept_name"] = device_data["concept_name"].str.strip()
        device_data["valid_start_date"] = device_data["valid_start_date"].apply(lambda x: datetime.datetime.strptime("1970-01-01", "%Y-%m-%d") if pd.isna(x) else x)
        device_data["invalid_reason"] = "D"
        device_data["ancestor_concept_code"] = np.nan
        device_data["previous_concept_code"] = np.nan
        device_data["sanjung_name"] = np.nan

        device_data = device_data[["concept_code", "concept_name", "domain_id", "vocabulary_id", "concept_class_id",
                    "valid_start_date", "valid_end_date", "invalid_reason","ancestor_concept_code","previous_concept_code",
                    "material","sanjung_name"]]

        return self.device_korean_translate(translate_path, device_data)


    def device_process(self, file_path, translate_path, sheet_name, material_date = None , device_code = None,
                    device_name = None, start_date_name = None, material_name = None):

        device_data = pd.read_excel(file_path,sheet_name=sheet_name)
        device_data.rename(columns={device_code : "concept_code",
                                        device_name : "concept_name",
                                        start_date_name : "valid_start_date",
                                        material_name : "material"}, inplace=True)
        
        device_data["domain_id"] = "Device"
        device_data["vocabulary_id"] = "EDI"
        device_data["concept_class_id"] = "Device"
        device_data["concept_name"] = device_data["concept_name"].str.strip()
        device_data["valid_start_date"] = device_data["valid_start_date"].apply(lambda x: datetime.datetime.strptime("1970-01-01", "%Y-%m-%d") if pd.isna(x) else x)
        device_data["valid_end_date"] = datetime.datetime.strptime("2099-12-31", "%Y-%m-%d")
        device_data["invalid_reason"] = np.nan
        device_data["ancestor_concept_code"] = np.nan
        device_data["previous_concept_code"] = np.nan
        device_data["sanjung_name"] = np.nan

        device_data = device_data[["concept_code", "concept_name", "domain_id", "vocabulary_id", "concept_class_id",
                    "valid_start_date", "valid_end_date", "invalid_reason","ancestor_concept_code","previous_concept_code",
                    "material","sanjung_name"]]

        return self.device_korean_translate(translate_path, device_data)


    def device_korean_translate(self, translate_path, device_data):

        translate = pd.read_csv(translate_path)
        if (int(translate["concept_name"].duplicated().sum()) >0) == True:
            raise Exception("Korean names in the dictionary should be unique")


        result_df = pd.merge(device_data, translate, on="concept_name", how= "left")
        
        find_korean = result_df['concept_name'].str.contains(r"[ㄱ-ㅣ가-힣]") == True

        result_df.loc[find_korean, ['concept_name', 'concept_synonym']] = result_df.loc[find_korean, ['concept_synonym', 'concept_name']].values

        result_df = result_df[["concept_code", "concept_name","concept_synonym","domain_id", "vocabulary_id", "concept_class_id",
                    "valid_start_date", "valid_end_date", "invalid_reason","ancestor_concept_code","previous_concept_code",
                    "material","sanjung_name"]]
        
        result_df["concept_name"].replace('\n', '').replace('\r', '')
        result_df["concept_synonym"].replace('\n', '').replace('\r', '')

        return result_df

    def merge_device(self, device, del_device):
        
        data = np.concatenate([device, del_device],axis=0)

        col = ["concept_code", "concept_name","concept_synonym","domain_id", "vocabulary_id", "concept_class_id",
                            "valid_start_date", "valid_end_date", "invalid_reason","ancestor_concept_code","previous_concept_code",
                            "material","sanjung_name"]
        result_device = pd.DataFrame(data, columns=col)


        return result_device
    

    def drug_process(self, file_path, sheet_name,drug_code, drug_name, clinical_drug_code, previous_concept_code):
        
        drug_data = pd.read_excel(file_path, sheet_name=sheet_name)
        drug_data.rename(columns={drug_name : "concept_synonym",
                                  drug_code: "concept_code",
                                  clinical_drug_code: "ancestor_concept_code",
                                  }, inplace=True)
        

        if previous_concept_code not in drug_data.columns:
            drug_data["previous_concept_code"] = np.nan
        
        else:
            drug_data.rename(columns={previous_concept_code : "previous_concept_code"
                                    }, inplace=True)
            
        drug_data["concept_code"] = drug_data["concept_code"].astype('str')
        drug_data["concept_code"] = drug_data["concept_code"].apply(lambda x : "0" + x if len(x) == 8 else x)
        drug_data["domain_id"] = "Drug"
        drug_data["vocabulary_id"] = "EDI"
        drug_data["concept_class_id"] = "Drug Product"
        drug_data["concept_name"] = drug_data["concept_code"].str.strip()
        # drug_data["valid_start_date"] = drug_data["valid_start_date"].apply(lambda x: datetime.datetime.strptime("1970-01-01", "%Y-%m-%d") if pd.isna(x) else x)
        drug_data["valid_start_date"] = datetime.datetime.strptime("1970-01-01", "%Y-%m-%d")
        drug_data["valid_end_date"] = datetime.datetime.strptime("2099-12-31", "%Y-%m-%d")
        drug_data["invalid_reason"] = np.nan
        drug_data["material"] = np.nan
        drug_data["sanjung_name"] = np.nan
        # drug_data["update_date"] = update_date

        drug_data.loc[pd.isna(drug_data["concept_synonym"]), "vocabulary_id"] = "KDC"
        drug_data.loc[drug_data["vocabulary_id"] == "KDC", "concept_class_id"] = "Clinical Drug"

        kdc_df = drug_data[drug_data["vocabulary_id"] =="KDC"]
        drug_name_df = kdc_df[["concept_name", "ancestor_concept_code"]]
        drug_name_df = drug_name_df.groupby('ancestor_concept_code')['concept_name'].agg(lambda x: ', '.join(x)).reset_index()

        bdg_df = drug_data[drug_data["vocabulary_id"] =="EDI"]
        
        bdg_df = bdg_df.drop(columns=["concept_name"])

        result_df = pd.merge(bdg_df, drug_name_df, on ="ancestor_concept_code", how="left")
        result_df = result_df[~pd.isna(result_df["concept_name"])]
        result_df = result_df[["concept_code", "concept_name","concept_synonym","domain_id", "vocabulary_id", "concept_class_id",
                    "valid_start_date", "valid_end_date", "invalid_reason","ancestor_concept_code","previous_concept_code",
                    "material","sanjung_name"]]
        return result_df


    def suga_process(self, file_path, sheet_name, translate_path =None, suga_code=None, korean_name=None,
                      english_name=None, start_date_name=None, sanjung_name=None):

        suga_data = pd.read_excel(file_path, sheet_name=sheet_name)

        
        suga_data.rename(columns={suga_code : "concept_code",
                                        korean_name : "concept_synonym",
                                        english_name : "concept_name",
                                        start_date_name : "valid_start_date",
                                        sanjung_name : "sanjung_name"}, inplace=True)
        # except:
        #     suga_data.rename(columns={suga_code : "concept_code",
        #                         korean_name : "concept_synonym",
        #                         start_date_name : "valid_start_date",
        #                         sanjung_name : "sanjung_name"}, inplace=True)
        #     suga_data["concept_name"] = np.nan


        suga_data["concept_code"] = suga_data["concept_code"].astype('str')
        suga_data["domain_id"] = "Procedure"
        suga_data["vocabulary_id"] = "EDI"
        suga_data["valid_start_date"] = suga_data["valid_start_date"].apply(lambda x: datetime.datetime.strptime("1970-01-01", "%Y-%m-%d") if pd.isna(x) else x)
        suga_data["valid_end_date"] = datetime.datetime.strptime("2099-12-31", "%Y-%m-%d")
        suga_data["invalid_reason"] = np.nan
        suga_data["previous_concept_code"] = np.nan
        suga_data["material"] = np.nan
        try:
            suga_data.loc[suga_data['concept_name'].str.contains(r"[ㄱ-ㅣ가-힣]") == True, "concept_name"] = np.nan
        except:
            pass
        suga_data["concept_synonym"] = suga_data["concept_synonym"].str.strip()
        try:
            suga_data["concept_name"] = suga_data["concept_name"].str.strip()
        except:
            pass
        suga_data["sanjung_name"] = suga_data["sanjung_name"].str.strip()
        suga_data["ancestor_concept_code"] = suga_data["concept_code"].apply(lambda x:x[0:5])
        suga_data["concept_class_id"] = "Proc Hierarchy"
        
        suga_data.loc[suga_data["ancestor_concept_code"] == suga_data["concept_code"], "ancestor_concept_code"] = np.nan

        suga_data.loc[~suga_data["ancestor_concept_code"].isin(suga_data["concept_code"]), "ancestor_concept_code"] = np.nan

        suga_data.loc[suga_data["concept_code"].str.match(r"^A[AH]|^[BCDEFG]|^H[AC]|^FA", na=False), "domain_id"] = "Measurement"

        suga_data.loc[suga_data["concept_code"].str.len() > suga_data["ancestor_concept_code"].str.len(), "concept_class_id"] = "Procedure"

        suga_data.loc[(suga_data["domain_id"] == "Measurement") & (suga_data["concept_class_id"] =="Proc Hierarchy"), "concept_class_id"] = "Meas Class"

        suga_data.loc[(suga_data["domain_id"] == "Measurement") & (suga_data["concept_class_id"] =="Procedure"), "concept_class_id"] = "Measurement"  

        suga_data.loc[(suga_data["domain_id"] == "Measurement") & (suga_data["concept_code"].str.match(r"^D", na=False)), "concept_class_id"] = "Proc Hierarchy"

        suga_data.loc[suga_data["concept_class_id"].str.contains("Measurement"), "domain_id"] = "Procedure"


        suga_data = suga_data[["concept_code", "concept_name", "concept_synonym", "domain_id", "vocabulary_id", "concept_class_id",
                 "valid_start_date", "valid_end_date", "invalid_reason","ancestor_concept_code","previous_concept_code",
                 "material","sanjung_name"]]
        
        return self.suga_korean_translate(translate_path, suga_data)

    def suga_korean_translate(self, translate_path, suga_data):

        translate = pd.read_csv(translate_path)

        if (int(translate["concept_synonym"].duplicated().sum()) >0) == True:
            raise Exception("Korean names in the dictionary should be unique")

        translate.rename(columns={"concept_name":"concept_name_tr"}, inplace=True)

        merge_kr_suga = pd.merge(suga_data, translate, on="concept_synonym", how="left")

        merge_kr_suga["concept_name"] = merge_kr_suga.apply(lambda x : x["concept_name_tr"] if pd.isna(x["concept_name"]) else x["concept_name"], axis=1)

        translate.rename(columns={"concept_name_tr":"sanjung_tr","concept_synonym":"sanjung_name"}, inplace=True)

        merge_sanjung_kr_suga = pd.merge(merge_kr_suga, translate, on="sanjung_name", how="left")

        merge_sanjung_kr_suga["concept_name"] = merge_sanjung_kr_suga.apply(lambda x : str(x["concept_name"]) + "," + str(x["sanjung_tr"]) if pd.notna(x["sanjung_name"]) else x["concept_name"], axis=1)

        merge_sanjung_kr_suga["concept_synonym"] = merge_sanjung_kr_suga.apply(lambda x : str(x["concept_synonym"]) + "," + str(x["sanjung_name"]) if pd.notna(x["sanjung_name"]) else x["concept_synonym"], axis=1)

        result_suga = merge_sanjung_kr_suga[["concept_code", "concept_name", "concept_synonym", "domain_id", "vocabulary_id", "concept_class_id",
            "valid_start_date", "valid_end_date", "invalid_reason","ancestor_concept_code","previous_concept_code",
            "material","sanjung_name"]]
        
        return result_suga

    def merge_suga(self, file_path,sheet_name, date):
        import openpyxl

        # col_name = ["수가코드","한글명","영문명","적용일자","산정명칭"]
        # sheet_name = ["의치과_급여_전체", "의치과_100대100_전체", "의치과_비급여_전체"]

        date = file_path.split("")

        workbook = openpyxl.load_workbook(file_path)

        sheet_names = workbook.sheetnames
        result_sheet = list(set(sheet_name).intersection(sheet_names))

        # return result_sheet
        suga_excel = pd.read_excel(file_path, sheet_name = result_sheet)

        merge_data = pd.DataFrame()

        for sheet in result_sheet:
            sheet_data = suga_excel[sheet]
            merge_data = pd.concat([merge_data, sheet_data], axis=0)
            
        merge_data = merge_data.dropna(subset=["한글명"],how='any')
        try:
            from datetime import datetime
            merge_data["적용일자"] = merge_data["적용일자"].apply(lambda x : datetime.strptime(str(x),"%Y%m%d") if type(x) == int else x)

        except:
            pass
        try:
            merge_data["영문명"]
        except:
            merge_data["영문명"] = np.nan


        # merge_data = merge_data[[col_name]]
        merge_data.to_excel(f"./dataset/merge/merge_suga_{date}.xlsx", index=False,sheet_name="result", header=True)


    
class DeviceSplit:

    def __init__(self, device_data, del_device_data, date):
        self._device_data = pd.read_excel(f"{device_data}", None)
        self._del_device_data = pd.read_excel(f"{del_device_data}", None)
        self._result_no_delite_list = []
        self._result_delite_list = []
        self._date = date

    def start(self):
        self.device_process()
        self.del_device_process()
        self.save_data()

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

                        self._result_no_delite_list.append({"코 드":append_data[0],
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

                        self._result_no_delite_list.append({"코 드":del_numpy[end_count][0],
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

                        self._result_delite_list.append({"코 드":del_numpy[end_count][0],
                                            "품 명":del_numpy[end_count][3],
                                            "재 질":del_numpy[end_count][7],
                                            "최초등재일자": datetime.datetime.strptime(str(del_numpy[end_count][-3]), "%Y-%m-%d"),
                                            "적용일자": datetime.datetime.strptime(str(del_numpy[end_count][-2]), "%Y-%m-%d")
                                            })             
                    except:
                        pass

                end_count +=1

    def save_data(self):

        save_path = "./dataset/device_split"
        if not os.path.exists(save_path):
            os.makedirs(save_path)

        result_no_delite = pd.DataFrame(self._result_no_delite_list)
        result_no_delite.to_excel(f"./dataset/device_split/result_no_delite_device_{self._date}.xlsx",index=False,sheet_name="result", header=True)

        result_delite = pd.DataFrame(self._result_delite_list)
        result_delite.to_excel(f"./dataset/device_split/result_delite_device_{self._date}.xlsx", index=False,sheet_name="result", header=True)
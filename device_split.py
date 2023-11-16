import pandas as pd
import os
import datetime

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

        save_path = "./device_split"
        if not os.path.exists(save_path):
            os.makedirs(save_path)

        result_no_delite = pd.DataFrame(self._result_no_delite_list)
        result_no_delite.to_excel(f"./dataset/device_split/result_no_delite_device_{self._date}.xlsx",index=False,sheet_name="result", header=True)

        result_delite = pd.DataFrame(self._result_delite_list)
        result_delite.to_excel(f"./dataset/device_split/result_delite_device_{self._date}.xlsx", index=False,sheet_name="result", header=True)




data = DeviceSplit(device_data="./dataset/Device/Device2023.10.01.xlsx", del_device_data="./dataset/Device/Device2023.10.01_급여중지삭제.xlsx", date="20231001").start()
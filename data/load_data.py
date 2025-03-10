import os
import pandas as pd
import json

class DataManager:
    def __init__(self, config_path="config.json"):
        """
        初始化 DataManager 類別，讀取 JSON 配置檔案中的 base_path_load 和 base_path_get。
        """
        if not os.path.exists(config_path):
            parent_path = os.path.join(os.getcwd(), "TQuantLab_Data", "config.json")
            if os.path.exists(parent_path):
                config_path = parent_path
            else:
                raise FileNotFoundError(f"Config file '{config_path}' not found.")
        
        self.config_path = config_path
        self.base_path_load, self.base_path_get = self.load_base_paths()
        
        self.data_structure = {
            "證券屬性資料表": "證券屬性資料/證券屬性資料表",
            "月營收": "公司營運資料/月營收",
            "股利政策": "公司營運資料/股利政策",
            "資本形成": "公司營運資料/資本形成",
            "集保庫存": "交易屬性資料/集保庫存",
            "三大法人_融資券_當沖": "交易屬性資料/三大法人_融資券_當沖",
            "股價交易資訊": "交易屬性資料/股價交易資訊",
            "股票日交易註記資訊": "交易屬性資料/股票日交易註記資訊",
            "交易日期表": "交易屬性資料/交易日期表",
            "會計師簽證財務資料": "財務屬性資料/會計師簽證財務資料",
            "公司自結數": "財務屬性資料/公司自結數",
            "合併收購": "法人機構專屬資料/合併收購",
            "董事長與高階主管變動事件": "法人機構專屬資料/董事長與高階主管變動事件",
            "全面改選統計": "法人機構專屬資料/全面改選統計",
            "董監全體持股狀況": "法人機構專屬資料/董監全體持股狀況",
            "庫藏股實施事件簿": "法人機構專屬資料/庫藏股實施事件簿",
            "董監申報轉讓_未轉讓": "法人機構專屬資料/董監申報轉讓_未轉讓",
            "董監申報轉讓_轉讓": "法人機構專屬資料/董監申報轉讓_轉讓",
            "私募應募人與公司的關係": "法人機構專屬資料/私募應募人與公司的關係",
            "月營收_法人機構專屬版本": "法人機構專屬資料/月營收_法人機構專屬版本",
        }

    def load_base_paths(self):
        """
        從配置檔案中載入 base_path_load 和 base_path_get。
        """
        with open(self.config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
        base_path_load = config.get("base_path_load", "")
        base_path_get = config.get("base_path_get", "")
        return base_path_load, base_path_get
    
    def list_datasets(self):
        """
        列出所有可用的資料集名稱。
        """
        print("可用的資料集:")
        for dataset in self.data_structure.keys():
            print(f"- {dataset}")

    def list_subfolders(self):
        """
        列出 base_path_load 路徑下的所有子資料夾，並統計每個資料夾內 .parquet 文件的獨特資料類別數量及類別名稱（排除 'mdate' 和 'coid'）。
        """
        target_path = self.base_path_load  # 使用 base_path_load
        subfolders = [folder for folder in os.listdir(target_path) 
                    if os.path.isdir(os.path.join(target_path, folder))]
        subfolder_info = []

        for folder in subfolders:
            folder_path = os.path.join(target_path, folder)
            unique_columns = set()

            for file in os.listdir(folder_path):
                if file.endswith(".parquet"):
                    file_path = os.path.join(folder_path, file)
                    try:
                        df = pd.read_parquet(file_path)
                        # 排除 'mdate' 和 'coid'
                        if isinstance(df.columns, pd.MultiIndex):
                            unique_columns.update(
                                col for col in df.columns.get_level_values(0) 
                                if col not in ["mdate", "coid"]
                            )
                        else:
                            unique_columns.update(col for col in df.columns if col not in ["mdate", "coid"])
                    except Exception as e:
                        print(f"Error reading {file_path}: {e}")

            subfolder_info.append({
                "folder": folder,
                "data_type_count": len(unique_columns),
                "data_types": list(unique_columns)
            })

        # 列印結果
        for info in subfolder_info:
            print(f"資料夾: {info['folder']}, 資料類型數量: {info['data_type_count']}, 資料類型: {info['data_types']}")
            
    
    def list_parquet_files(self, directory):
        """
        列出目錄中的所有 .parquet 文件。
        """
        return [f for f in os.listdir(directory) if f.endswith('.parquet')]


    def tool_api_data(self, *dataset_names):
        """
        載入一個或多個資料集（使用 base_path_load）。
        如果資料只有 'mdate' 和 'coid' 兩個欄位，則直接返回原始表格。
        否則，將進行 pivot 操作來整理資料，並直接顯示每個資料集的結果。

        Args:
            *dataset_names (str): 多個資料集名稱。

        Returns:
            pd.DataFrame: 合併多個資料集結果的 DataFrame，使用欄位名稱區分各資料集。
        """
        combined_data = []

        for dataset_name in dataset_names:
            folder_path = os.path.join(self.base_path_load, dataset_name, dataset_name + ".parquet")
            if not os.path.exists(folder_path):
                print(f"資料檔不存在: {folder_path}")
                continue

            try:
                # 讀取 parquet 資料
                data = pd.read_parquet(folder_path)

                if "mdate" in data.columns:
                    data["mdate"] = pd.to_datetime(data["mdate"], errors="coerce")
                else:
                    raise ValueError(f"資料集 '{dataset_name}' 缺少 'mdate' 欄位，無法處理。")

                non_key_columns = [col for col in data.columns if col not in {"mdate", "coid"}]
                if not non_key_columns:
                    print(f"資料集 '{dataset_name}' 只有 'mdate' 和 'coid' 欄位，返回原始資料。")
                    data["dataset_name"] = dataset_name  
                    combined_data.append(data)
                    continue
                
                print(f"資料集 '{dataset_name}' 包含其他欄位（{non_key_columns}），進行 pivot 操作。")
                data = data.drop_duplicates(subset=["mdate", "coid"], keep="last")
                pivot_data = data.pivot(index="mdate", columns="coid")
                combined_data.append(pivot_data)

            except Exception as e:
                print(f"處理資料集 '{dataset_name}' 時發生錯誤: {e}")

        if combined_data:
            final_result = pd.concat(combined_data, axis=1)
            return final_result
        else:
            print("未能成功載入任何資料集。")
            return pd.DataFrame()


    def get_data(self, dataset_name, merge_files=True, common_stock=False):
        """
        從多個 parquet 檔案中讀取資料（使用 base_path_get），並可選擇篩選普通股。

        Args:
            dataset_name (str): 資料集名稱。
            merge_files (bool): 是否合併多個文件，默認為 True。
            common_stock (bool): 是否篩選普通股，默認為 False。

        Returns:
            pd.DataFrame: 返回處理後的資料。
        """
        if dataset_name not in self.data_structure:
            raise ValueError(f"無效的資料集名稱: {dataset_name}")
        folder_path = os.path.join(self.base_path_get, "TQuantLab_Data", self.data_structure[dataset_name])
        if not os.path.exists(folder_path):
            raise FileNotFoundError(f"資料夾不存在: {folder_path}")
        
        files = self.list_parquet_files(folder_path)
        if not files:
            raise FileNotFoundError(f"資料夾 {folder_path} 中沒有找到 parquet 文件")

        dataframes = []
        for file in files:
            file_path = os.path.join(folder_path, file)
            try:
                df = pd.read_parquet(file_path)
                dataframes.append(df)
            except Exception as e:
                print(f"無法讀取文件 {file_path}: {e}")

        if not dataframes:
            raise ValueError(f"所有 .parquet 文件都無法讀取，資料夾: {folder_path}")

        if merge_files:
            result = pd.concat(dataframes, ignore_index=True)
        else:
            result = dataframes[0]

        if common_stock:
            common_stock_path = os.path.join(self.base_path_load, "證券種類_中文", "證券種類_中文.parquet")
            if not os.path.exists(common_stock_path):
                raise FileNotFoundError(f"普通股表不存在: {common_stock_path}")
            
            common_stock_df = pd.read_parquet(common_stock_path)
            common_stock_codes = common_stock_df.loc[common_stock_df["Security_Type_Chinese"] == "普通股", "coid"].unique()

            # 篩選出普通股的資料
            if "證券名稱" in result.columns:
                result = result[result["證券名稱"].isin(common_stock_codes)]
            elif "公司" in result.columns:
                result = result[result["公司"].isin(common_stock_codes)]
            else:
                raise ValueError("資料集中缺少 '證券名稱' 或 '公司' 欄位，無法篩選普通股。")
        
        if dataset_name == '三大法人_融資券_當沖':
            if "市場別" in result.columns and "證券名稱" in result.columns and "資料日" in result.columns:
                result = result.sort_values(by=["市場別", "證券名稱", "資料日"], ascending=[True, True, True]).reset_index(drop=True)
                result = result.drop_duplicates(subset=["資料日", "證券名稱"], keep='first')
            else:
                raise ValueError(f"表 {dataset_name} 缺少排序所需的欄位 '市場別'、'證券名稱' 或 '資料日'。")
        elif dataset_name in ['股價交易資訊', '股票日交易註記資訊']:
            if "證券名稱" in result.columns and "資料日" in result.columns:
                result = result.sort_values(by=["證券名稱", "資料日"], ascending=[True, True]).reset_index(drop=True)
                result = result.drop_duplicates(subset=["資料日", "證券名稱"], keep='first')
            else:
                raise ValueError(f"表 {dataset_name} 缺少排序所需的欄位 '證券名稱' 或 '資料日'。")
        
        return result



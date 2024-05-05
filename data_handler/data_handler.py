import os
import pandas as pd
import pyarrow.parquet as pq
from file_holder import CFileHolder

current_path = os.path.dirname( __file__ )
dict_parquet_folder = {
    'second_hand': os.path.join(current_path, 'total_second_hand'), 
    'pre_sale': os.path.join(current_path, 'total_pre_sale'),
    'rent': os.path.join(current_path, 'total_rent')
}

class CDataHandler:
    def __init__(self):
        self.df_second_hand = None
        self.df_pre_sale = None
        self.df_rent = None

    def find_unique_in_df2(self, df_1, df_2):
        if df_1 is None:
            return df_2

        # 使用 merge() 函數進行合併，使用 "編號" 這個欄位作為合併的 key
        merged_df = pd.merge(df_2, df_1, on='編號', suffixes=('', '_y'), how='outer', indicator=True)
        
        # 選擇 df_2 中在 df_1 中不存在的資料
        df_3 = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge'])
        df_3.drop(df_3.filter(regex='_y$').columns, axis=1, inplace=True)
        
        return df_3

    def merge_df1_df3(self, df_1, df_3):
        if df_1 is None:
            return df_3
        
        # 使用 concat() 函數將 df_3 與 df_1 合併
        merged_df = pd.concat([df_1, df_3], ignore_index=True)
        return merged_df

    # trade_mode: second_hand, pre_sale, rent
    def update_year_season_data(self, year, season, trade_mode='second_hand'):
        # get year_season data from file_holder
        fileHolder = CFileHolder()
        df_official_data = fileHolder.get_year_season_data_by_mode(year, season, 'pre_sale')
        
        # get current saved data
        max_trade_date = df_official_data['交易年'].max()
        min_trade_date = df_official_data['交易年'].min()
        filters = [('交易年', '>=', min_trade_date),('交易年', '<=', max_trade_date)]
        try:
            dataset = pq.ParquetDataset(dict_parquet_folder[trade_mode], filters=filters)
            setattr(self, 'df_' + trade_mode, dataset.read().to_pandas())
        except:
            print('Read Parquet by PyArrow with exception.')

        # find the data need to be add to saved data
        df_trade_mode = getattr(self, 'df_' + trade_mode, None)
        df_new_data = self.find_unique_in_df2(df_trade_mode, df_official_data)
        df_final_data = self.merge_df1_df3(df_trade_mode, df_new_data)

        # save to parquet
        df_final_data.to_parquet(dict_parquet_folder[trade_mode], engine='pyarrow', partition_cols=['交易年', '交易月'], existing_data_behavior='delete_matching')

    def get_data_by_condition(self, begin_year=110, begin_month=1, end_year=110, end_month=1, city='臺北市', dist='中正區', trade_mode='pre_sale'):
        filters = [
            ('交易年月日', '>', begin_year * 10000 + begin_month * 100),
            ('交易年月日', '<', end_year * 10000 + (end_month + 1) * 100),
            ('縣市', '==', city),
            ('鄉鎮市區', '==', dist)
            ]
        print(f'::get_data_by_condition, {filters=:}')
        try:
            dataset = pq.ParquetDataset(dict_parquet_folder[trade_mode], filters=filters)
            setattr(self, 'df_' + trade_mode, dataset.read().to_pandas())
        except Exception as e:
            print('::get_data_by_condition, Read Parquet by PyArrow with exception: ' + str(e))
        return getattr(self, 'df_' + trade_mode, None)

if __name__ == '__main__':
    dataHandler = CDataHandler()

    #dataHandler.update_year_season_data(110, 1, 'pre_sale')
    #dataHandler.update_year_season_data(110, 2, 'pre_sale')
    #dataHandler.update_year_season_data(110, 3, 'pre_sale')    
    #dataHandler.update_year_season_data(110, 4, 'pre_sale')
    #dataHandler.update_year_season_data(111, 1, 'pre_sale')
    #dataHandler.update_year_season_data(111, 2, 'pre_sale')
    #dataHandler.update_year_season_data(111, 3, 'pre_sale')
    #dataHandler.update_year_season_data(111, 4, 'pre_sale')
    #dataHandler.update_year_season_data(112, 1, 'pre_sale')
    #dataHandler.update_year_season_data(112, 2, 'pre_sale')
    #dataHandler.update_year_season_data(112, 3, 'pre_sale')
    #dataHandler.update_year_season_data(112, 4, 'pre_sale')
    dataHandler.update_year_season_data(113, 1, 'pre_sale')
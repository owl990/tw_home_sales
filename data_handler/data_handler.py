import os
import pandas as pd
import pyarrow.parquet as pq

current_path = os.getcwd()
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

    # trade_mode: second_hand, pre_sale, rent
    def update_year_season_data(self, year, season, trade_mode='second_hand'):
        # get year_season data from file_holder

        # get current saved data
        # TODO: need to filter date to decrease size
        #filters = [('date', '>', '2024-04-01'),('date', '<', '2024-04-30'), ('city', '==', 'Taipei')]
        #table = dataset.read(filters=filters)
        try:
            dataset = pq.ParquetDataset(dict_parquet_folder[trade_mode])
            setattr(self, 'df_' + trade_mode, dataset.read().to_pandas())
        except:
            print('Read Parquet by PyArrow with exception.')

        # find the data need to be add to saved data
        df_trade_mode = getattr(self, 'df_' + trade_mode, None)

        # union current data and new data

        # save to parquet

if __name__ == '__main__':
    pass
import os
import requests
import zipfile
import shutil
import pandas as pd
import numpy as np
import glob
import csv

# EXAMPLE: https://plvr.land.moi.gov.tw//DownloadSeason?season=110S4&type=zip&fileName=lvr_landcsv.zip
TEMPLATE_DOWNLOAD_URL = "https://plvr.land.moi.gov.tw//DownloadSeason?season={year_season}&type=zip&fileName=lvr_landcsv.zip"

dict_file_format = {
    'second_hand': {
        'total': '*_lvr_land_a.csv',
        'build': '*_lvr_land_a_build.csv',
        'land': '*_lvr_land_a_land.csv',
        'park': '*_lvr_land_a_park.csv'
    },
    'pre_sale': {
        'total': '*_lvr_land_b.csv',
        #'build': '*_lvr_land_b_build.csv', # pre_sale doesn't have building
        'land': '*_lvr_land_b_land.csv',
        'park': '*_lvr_land_b_park.csv'
    },
    'rent': {
        'total': '*_lvr_land_c.csv',
        'build': '*_lvr_land_c_build.csv',
        'land': '*_lvr_land_c_land.csv',
        'park': '*_lvr_land_c_park.csv'
    },
}

"""
Help to hold the downloaded file from Web
"""
class CFileHolder():
    def __init__(self):
        self.file_path_map = dict()
        self.list_bad_line = []

        current_path = os.getcwd()
        self.download_folder = os.path.join(current_path, "download_folder")
        if not os.path.exists(self.download_folder):
            os.makedirs(self.download_folder)

    def __del__(self):
        for folder_path in self.file_path_map.values():
            try:
                shutil.rmtree(folder_path)
                print(f"remove '{folder_path}' success")
            except Exception as e:
                print(f"remove '{folder_path}' failed：{e}")

    def download_unzip_file(self, year, season):
        year_season = f'{year}S{season}'
        url = TEMPLATE_DOWNLOAD_URL.format(year_season=year_season)
        download_file_path = os.path.join(self.download_folder, "downloaded_file.zip")
        
        response = requests.get(url)
        if response.status_code == 200:
            with open(download_file_path, 'wb') as f:
                f.write(response.content)

            # unzip file
            unzip_folder_path = os.path.join(self.download_folder, year_season)
            with zipfile.ZipFile(download_file_path, 'r') as zip_ref:
                zip_ref.extractall(unzip_folder_path)

            # remove downloaded zip file
            os.remove(download_file_path)

            self.file_path_map[year_season] = unzip_folder_path

            return unzip_folder_path
        
        else:
            return None

    def keep_bad_line(self, bad_line):
        self.list_bad_line.append(bad_line)

    def fix_bad_line_to_dataframe(self, expected_columns, expected_dtypes):
        expected_field_number = len(expected_columns)
        list_fixed_line = []
        for bad_line in self.list_bad_line:
            fixed_line = []
            for idx in range(expected_field_number - 1):
                objectFixed = np.nan if '' == bad_line[idx] else bad_line[idx]
                fixed_line.append(objectFixed)
            fixed_line.append(','.join(bad_line[expected_field_number - 1:]))
            list_fixed_line.append(fixed_line)
        return pd.DataFrame(list_fixed_line, columns=expected_columns).astype(expected_dtypes)
    
    def get_year_season_data_by_mode(self, year, season, mode='pre_sale'):
        # mode = second_hand, pre_sale, rent
        year_season = f'{year}S{season}'
        csv_data_folder = None
        if year_season not in self.file_path_map:
            csv_data_folder = self.download_unzip_file(year, season) # TODO: recover me
            #csv_data_folder = os.path.join(self.download_folder, year_season)

        if None == csv_data_folder:
            raise FileNotFoundError(f'Get data for {year_season} failed!')
        
        self.list_bad_line = []
        file_paths = glob.glob(os.path.join(csv_data_folder, dict_file_format[mode]['total']))
        combined_df = pd.DataFrame()
        for file_path in file_paths:
            df = pd.read_csv(file_path, quoting=csv.QUOTE_NONE, header=0, skiprows=[1], engine='python', on_bad_lines=self.keep_bad_line)
            combined_df = pd.concat([combined_df, df], ignore_index=True)

        combined_df = pd.concat([combined_df, self.fix_bad_line_to_dataframe(combined_df.columns, combined_df.dtypes)], ignore_index=True)
        combined_df['縣市'] = combined_df['土地位置建物門牌'].str.slice(stop=3)
        combined_df['交易年'] = combined_df['交易年月日'] // 10000
        combined_df['交易月'] = combined_df['交易年月日'] % 10000 // 100
        combined_df.replace('', np.nan, inplace=True)
        return combined_df

if __name__ == '__main__':
    cFileHolder = CFileHolder()
    df = cFileHolder.get_year_season_data_by_mode(110, 4, 'pre_sale')
    print(df)

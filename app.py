import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import plotly.express as px
import pandas as pd
from datetime import datetime, timedelta
import dash_bootstrap_components as dbc
import json
import sys
import os

script_dir = os.path.dirname( __file__ )
mymodule_dir = os.path.join( script_dir, 'data_handler' )
sys.path.append( mymodule_dir )
from data_handler import CDataHandler

# stylesheet with the .dbc class to style  dcc, DataTable and AG Grid components with a Bootstrap theme
dbc_css = "https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-templates/dbc.min.css"

# read city - district json
with open('city_dist.json', 'r', encoding='utf-8') as file:
    city_dist_data = json.load(file)
#print(city_dist_data['臺北市'])

dataHanlder = CDataHandler()
df_house_data = dataHanlder.get_data_by_condition()
#print(df_house_data)

# 假設 house_data 中有一個名為 "交易年月日" 的欄位，表示交易日期

# 初始化 Dash 應用
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY, dbc.icons.FONT_AWESOME, dbc_css])

# 設定應用標題
app.title = "台灣預售屋"

# 設定縣市下拉式選單的選項
cities = ['臺北市', '新北市', '基隆市', '宜蘭縣', '桃園市', '新竹縣', '新竹市', '苗栗縣', 
            '臺中市', '彰化縣', '南投縣', '雲林縣', '嘉義縣', '嘉義市', '臺南市', '高雄市', 
            '屏東縣', '臺東縣', '花蓮縣', '金門縣', '澎湖縣', '連江縣']

# 台北市的區列表
taipei_districts = [
    '中正區', '大同區', '中山區', '松山區', '大安區', '萬華區', '信義區', '士林區', '北投區', '內湖區', '南港區', '文山區'
]

# 建立應用的布局
app.layout = dbc.Container([

    html.H1("台灣預售屋", className="bg-primary text-white p-2 mb-2 text-center"),

    # 年月選單
    dbc.Row([
        dbc.Col(
            html.Label('交易期間：'), 
            width={'size': 1, 'offset':2 }
        ),
        dbc.Col(
            dcc.Dropdown(
                id='start-year-dropdown',
                options=[{'label': str(year), 'value': year} for year in range(101, 114)],
                value=110  # 預設選擇110年
            ),
            width=1
        ),
        dbc.Col(
            dcc.Dropdown(
                id='start-month-dropdown',
                options=[{'label': str(month), 'value': month} for month in range(1, 13)],
                value=1  # 預設選擇1月
            ),
            width=1
        ),
        dbc.Col(
            html.Label('至', className="p-2 mb-2 text-center"), 
            width=1
        ),
        dbc.Col(
            dcc.Dropdown(
                id='end-year-dropdown',
                options=[{'label': str(year), 'value': year} for year in range(101, 114)],
                value=110  # 預設選擇110年
            ),
            width=1
        ),
        dbc.Col(
            dcc.Dropdown(
                id='end-month-dropdown',
                options=[{'label': str(month), 'value': month} for month in range(1, 13)],
                value=2  # 預設選擇12月
            ),
            width=1
        )
    ], className="mb-4"),

    # 下拉式選單
    dbc.Row([
        dbc.Col(
            html.Label('選擇縣市：'), 
            width={'size': 1, 'offset': 2}
        ),
        dbc.Col(
            dcc.Dropdown(
                id='county-dropdown',
                options=[{'label': city, 'value': city} for city in cities],
                value=cities[0]  # 預設選擇第一個縣市
            ),
            width=3
        ),
        dbc.Col(
            html.Label('選擇鄉鎮：'), 
            width=1
        ),
        dbc.Col(
            dcc.Dropdown(
                id='district-dropdown',
                options=[{'label': district, 'value': district} for district in taipei_districts],
                value=taipei_districts[0],  # 預設選擇台北市的第一個區
            ),
            width=3
        ),
        dbc.Col(
            dbc.Button('搜尋', id='submit-button', color='primary', className='mr-1'),
            width=1
        ),
        html.Div(id='output-table')
    ])
], className="dbc"
)

@app.callback(
    Output('district-dropdown', 'options'),
    Output('district-dropdown', 'value'),
    Input('county-dropdown', 'value')
)
def update_district_options(selected_county):
    districts = city_dist_data.get(selected_county, [])
    return [{'label': district, 'value': district} for district in districts], districts[0]

@app.callback(
    Output('output-table', 'children'),
    Input('submit-button', 'n_clicks'),
    State('county-dropdown', 'value'),
    State('district-dropdown', 'value'),
    State('start-year-dropdown', 'value'),
    State('start-month-dropdown', 'value'),
    State('end-year-dropdown', 'value'),
    State('end-month-dropdown', 'value')
)
def update_output(n_clicks, selected_county, selected_district, begin_year, begin_month, end_year, end_month):
    if n_clicks:
        df_house_data = dataHanlder.get_data_by_condition(begin_year=begin_year, begin_month=begin_month, end_year=end_year, end_month=end_month, city=selected_county, dist=selected_district)
        df_house_data['交易年月'] = df_house_data['交易年'].astype(str) + '-' + df_house_data['交易月'].astype(str).str.zfill(2)
        #grouped_data = df_house_data.groupby(['交易年月', '建案名稱']).size().reset_index(name='個數')
        #table = dbc.Table.from_dataframe(grouped_data, striped=True, bordered=True, hover=True)

        pivot_data = df_house_data.pivot_table(index='建案名稱', columns='交易年月', aggfunc='size', fill_value=0)
        pivot_data['total'] = pivot_data.sum(axis=1)
        # 转置表格
        pivot_data = pivot_data.T
        pivot_data['total'] = pivot_data.sum(axis=1)
        print(pivot_data)
        # 增加交易年月的索引名称
        pivot_data.index.name = '交易年月'
        # 重置索引
        pivot_data = pivot_data.reset_index()
        pivot_data.replace(0, '', inplace=True)
        # 创建表格
        table = dbc.Table.from_dataframe(pivot_data, striped=True, bordered=True, hover=True)
        return table
    
# 啟動 Dash 應用
if __name__ == '__main__':
    app.run_server(debug=True)

import requests
from io import StringIO
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import time
import numpy as np
from talib import abstract

def crawler_stock_df(date):
    datestr = str(date).split(' ')[0].replace('-', '')
    r = requests.post('http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=' + datestr + '&type=ALL')

    ##############################################################################################
    #df = pd.read_csv(StringIO("\n".join([i.translate({ord(c): None for c in ' '})
    #                                    for i in r.text.split('\n')
    #                                    if len(i.split('",')) == 17 and i[0] != '='])), header=0)
    ##############################################################################################
    m_str = ""
    for i in r.text.split('\n'):
        if len(i.split('",')) == 17:
            m_str += i.translate({ord(c): None for c in '= '}) + "\n"
    df = pd.read_csv(StringIO(m_str)).dropna(how='all', axis='columns')
    return df


def crawler_price(date):
    rt = crawler_stock_df(date)
    rt['成交金額'] = rt['成交金額'].str.replace(',', '')
    rt['成交股數'] = rt['成交股數'].str.replace(',', '')
    rt = rt.set_index('證券代號')
    return rt


data = {}
n_days = 16
date = datetime.datetime.now()
fail_count = 0
allow_continuous_fail_count = 5
while len(data) < n_days:
    try:
        data[date.date()] = crawler_price(date)
        fail_count = 0
        print("parsing ", date, " success")
    except:
        print("parsing ", date, " failed")
        fail_count += 1
        if fail_count >= allow_continuous_fail_count:
            raise
            break
    date -= datetime.timedelta(days=1)

close_price = pd.DataFrame({k: d['收盤價'] for k, d in data.items()}).transpose()
close_price.index = pd.to_datetime(close_price.index)
#close.to_excel('close.xlsx', sheet_name='sheet1')

open_price = pd.DataFrame({k: d['開盤價'] for k, d in data.items()}).transpose()
open_price.index = pd.to_datetime(open_price.index)

high_price = pd.DataFrame({k: d['最高價'] for k, d in data.items()}).transpose()
high_price.index = pd.to_datetime(high_price.index)

low_price = pd.DataFrame({k: d['最低價'] for k, d in data.items()}).transpose()
low_price.index = pd.to_datetime(low_price.index)

volume = pd.DataFrame({k: d['成交股數'] for k, d in data.items()}).transpose()
volume.index = pd.to_datetime(volume.index)

tsmc = pd.DataFrame({
    'close': close_price['2330']['2019'].dropna().astype(float),
    'open': open_price['2330']['2019'].dropna().astype(float),
    'high': high_price['2330']['2019'].dropna().astype(float),
    'low': low_price['2330']['2019'].dropna().astype(float),
    'volume': volume['2330']['2019'].dropna().astype(float),
}).sort_index()


print(tsmc)

kd = abstract.STOCH(tsmc)
print(kd)
#talib2df(abstract.STOCH(tsmc)).plot()
kd.plot()
tsmc['close'].plot(secondary_y=True)
plt.show()

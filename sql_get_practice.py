# coding=cp950
import datetime
import sqlalchemy as db
import pandas as pd
from talib import abstract
from memory_profiler import profile
import matplotlib.pyplot as plt


def table_exists(name, engine):
    ret = engine.dialect.has_table(engine, name)
    #print('Table "{}" exists: {}'.format(name, ret))
    return ret


#@profile
def main():
    engine = db.create_engine('sqlite:///sql_pratice_db')
    conn = engine.connect()

    data = {}
    n_days = 16
    date = datetime.datetime.now()
    fail_count = 0
    allow_continuous_fail_count = 5
    while len(data) < n_days:
        table_name = date.date().__str__()
        if table_exists(table_name, engine):
            try:
                data[date.date()] = pd.read_sql_table(table_name=table_name, con=conn)
                data[date.date()] = data[date.date()].set_index('證券代號')
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
    kd.plot()
    tsmc['close'].plot(secondary_y=True)
    # plt.show()


if __name__ == '__main__':
    main()


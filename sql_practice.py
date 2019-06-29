from io import StringIO
from time import sleep
import requests
import datetime
import sqlalchemy as db
import pandas as pd


def table_exists(name, engine):
    ret = engine.dialect.has_table(engine, name)
    #print('Table "{}" exists: {}'.format(name, ret))
    return ret


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

    df['成交金額'] = df['成交金額'].str.replace(',', '')
    df['成交股數'] = df['成交股數'].str.replace(',', '')
    df = df.set_index('證券代號')

    return df


if __name__ == '__main__':
    engine = db.create_engine('sqlite:///sql_pratice_db')
    conn = engine.connect()

    #days_count = 0
    #n_days = 16
    date = datetime.datetime.now()
    due_day = date - datetime.timedelta(days=365)
    fail_count = 0
    allow_continuous_fail_count = 5
    while date != due_day:
        is_requested = False
        if date.weekday() is not 6 and date.weekday() is not 5:
            table_name = date.date().__str__()
            if table_exists(table_name, engine) is False:
                is_requested = True
                try:
                    data = crawler_stock_df(date)
                    fail_count = 0
                    data.to_sql(table_name, con=engine)
                    print("parsing ", date, " success")
                except:
                    print("failed or skip ", date, " weekday ", date.weekday())
                    #fail_count += 1
                    #if fail_count >= allow_continuous_fail_count:
                    #    raise
                    #    break
        date -= datetime.timedelta(days=1)
        if is_requested:
            sleep(0.5)



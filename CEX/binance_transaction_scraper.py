"""
# File       : binance_transaction_scraper.py
# Time       ：2022/3/4 5:26 PM
# Author     ：Andy
# version    ：python 3.9
# Description：
"""

from binance import Client
import pandas as pd
from datetime import datetime, timedelta
from dateutil.parser import parse
from backup.mongo_class import my_mongo
from tqdm import tqdm
import time


def binance_transaction_scraper(base_token:str, quote_token:str, start_date:str, end_date:str, interval:str):
    """

    :param base_token:
    :param quote_token:
    :param start_date:
    :param end_date:
    :param interval:
    :return:
    """

    proxies = {'http': 'http://127.0.0.1:1086',
               'https': 'http://127.0.0.1:1086'}
    client = Client(api_key="",
                    api_secret="",
                    requests_params={'proxies': proxies})
    mongo = my_mongo(host='127.0.0.1', port=27017)

    symbol = base_token + quote_token
    # df = client.get_historical_klines(symbol, interval, start_date, end_date)

    date_range = pd.date_range(start=start_date, end=end_date, freq='1d')
    date_range = [x.strftime('%Y-%m-%d') for x in date_range]
    df = []
    missing_index = []
    for i in tqdm(range(len(date_range)-1)):
        try:
            klines = client.get_historical_klines(symbol, interval=interval,
                                                  start_str=date_range[i], end_str=date_range[i+1])
            df.extend(klines)
        except:
            print(date_range[i])
            missing_index.append(i)
            time.sleep(3)
            pass
    # 对于抓取失败的日期的数据 循环抓取(最大循环次数为10次)
    flg = 0
    while (len(missing_index) != 0) & (flg <= 10):
        for each in tqdm(missing_index):
            try:
                klines = client.get_historical_klines(symbol, interval=interval,
                                                      start_str=date_range[each], end_str=date_range[each+1])
                df.extend(klines)
                missing_index.pop(each)
            except:
                print(date_range[each], 'still failed to get data')
                time.sleep(3)
        flg += 1

    data = pd.DataFrame(df, columns=['Open_time', 'Open', 'High', 'Low', 'Close', 'Volume', 'End_time',
                                     'quote_volume', 'transaction_num', 'buy_volume', 'quote_buy_volume', 'ignore'])
    data['start_time'] = data['Open_time'].apply(lambda x: datetime.utcfromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    data['base_token'] = base_token
    data['quote_token'] = quote_token
    data['symbol'] = symbol
    data.drop(['ignore', 'Open_time', 'End_time', 'quote_volume', 'quote_buy_volume'], axis=1, inplace=True)
    data = data.astype({'Open': 'float64', 'High': 'float64', 'Low': 'float64', 'Close': 'float64',
                        'Volume': 'float64', 'buy_volume': 'float64', 'transaction_num': 'int64'})
    order = ['start_time', 'symbol', 'base_token', 'quote_token', 'Open', 'High', 'Low', 'Close',
             'Volume', 'buy_volume', 'transaction_num']
    data = data[order]
    data.drop_duplicates(subset=['start_time'], inplace=True)
    mongo.df_to_mongodb(db_name='cex_trade', col_name='binance_kline_data', data=data)

def get_latest_date(symbol: str):
    """

    :param symbol:
    :return:
    """
    mongo = my_mongo(host='127.0.0.1', port=27017)
    col = mongo.client['cex_trade']['binance_kline_data']
    latest_date = [x['start_time'] for x in col.find({'symbol': symbol}, {'_id': 0, 'start_time': 1})]
    latest_date.sort()
    latest_date = latest_date[-1]
    return pd.to_datetime(latest_date).strftime("%Y-%m-%d")


def init(base_token:str, quote_token:str, start_date:str, end_date:str, interval:str):
    """

    :param base_token:
    :param quote_token:
    :param start_date:
    :param end_date:
    :param interval:
    :return:
    """
    binance_transaction_scraper(base_token=base_token, quote_token=quote_token,
                                start_date=start_date, end_date=end_date, interval=interval)
def update(base_token: str, quote_token: str, interval:str):
    """

    :param base_token:
    :param quote_token:
    :param interval:
    :return:
    """
    symbol = base_token + quote_token
    start_date = get_latest_date(symbol)
    end_date = datetime.now().strftime("%Y-%m-%d")
    if start_date != end_date:
        binance_transaction_scraper(base_token=base_token, quote_token=quote_token,
                                start_date=start_date, end_date=end_date, interval=interval)
        print("更新完成，更新了从{}到{}的数据".format(start_date, end_date))
    else:
        print("无最新数据")
    pass

def fill_missing_data(base_token: str, quote_token: str, interval: str):
    """

    :param base_token:
    :param quote_token:
    :param interval:
    :return:
    """
    symbol = base_token + quote_token
    proxies = {'http': 'http://127.0.0.1:1086',
               'https': 'http://127.0.0.1:1086'}
    client = Client(api_key="",
                    api_secret="",
                    requests_params={'proxies': proxies})
    mongo = my_mongo(host='127.0.0.1', port=27017)
    date_lis = mongo.col_to_df(db_name='cex_trade', col_name='binance_kline_data',
                               query={'base_token': base_token, 'quote_token': quote_token}, return_col={'_id': 0, 'start_time': 1})
    date_lis.sort_values(by='start_time', inplace=True)
    date_lis = date_lis['start_time'].to_list()
    temp = pd.date_range(start=date_lis[0], end=date_lis[-1], freq='1min')
    temp = [str(x) for x in temp]
    missing_date = list(set(temp).difference(set(date_lis)))
    missing_date = list(set([pd.to_datetime(x).strftime("%Y-%m-%d") for x in missing_date]))
    df = []
    failed_date = []
    for date in tqdm(missing_date):
        end_date = (parse(missing_date[0]) + timedelta(days=1)).strftime("%Y-%m-%d")
        try:
            klines = client.get_historical_klines(symbol, interval=interval,
                                                  start_str=date, end_str=end_date)
            df.extend(klines)

        except:
            print(date, 'still failed to get data')
            failed_date.append(date)
            time.sleep(3)

    # 对于抓取失败的日期的数据 循环抓取(最大循环次数为10次)
    flg = 0
    while (len(failed_date) != 0) & (flg <= 10):
        for each in tqdm(failed_date):
            end_date = (parse(each) + timedelta(days=1)).strftime("%Y-%m-%d")
            try:
                klines = client.get_historical_klines(symbol, interval=interval,
                                                      start_str=each, end_str=end_date)
                df.extend(klines)
                failed_date.remove(each)
            except:
                print(failed_date[each], 'still failed to get data')
                time.sleep(3)
        flg += 1

    data = pd.DataFrame(df, columns=['Open_time', 'Open', 'High', 'Low', 'Close', 'Volume', 'End_time',
                                     'quote_volume', 'transaction_num', 'buy_volume', 'quote_buy_volume', 'ignore'])
    data['start_time'] = data['Open_time'].apply(lambda x: datetime.utcfromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    data['base_token'] = base_token
    data['quote_token'] = quote_token
    data['symbol'] = symbol
    data.drop(['ignore', 'Open_time', 'End_time', 'quote_volume', 'quote_buy_volume'], axis=1, inplace=True)
    data = data.astype({'Open': 'float64', 'High': 'float64', 'Low': 'float64', 'Close': 'float64',
                        'Volume': 'float64', 'buy_volume': 'float64', 'transaction_num': 'int64'})
    order = ['start_time', 'symbol', 'base_token', 'quote_token', 'Open', 'High', 'Low', 'Close',
             'Volume', 'buy_volume', 'transaction_num']
    data = data[order]
    data.drop_duplicates(subset=['start_time'], inplace=True)
    print(data.shape)
    mongo.df_to_mongodb(db_name='cex_trade', col_name='binance_kline_data', data=data)


if __name__ == '__main__':

    base_token = 'BTC'
    quote_token = 'USDT'
    start_date = '2017-01-01'
    end_date = datetime.now().strftime("%Y-%m-%d")
    interval = '1m'
    # init(base_token=base_token, quote_token=quote_token, start_date=start_date, end_date=end_date, interval=interval)
    # update(base_token=base_token, quote_token=quote_token, interval=interval)
    fill_missing_data(base_token=base_token, quote_token=quote_token, interval=interval)

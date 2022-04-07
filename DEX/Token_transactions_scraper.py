import requests
import time
import urllib3
import pandas as pd
from backup.mongo_class import my_mongo
from datetime import datetime
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# -*- coding: utf-8 -*-



def token_transaction_scraper(token, network, token_address, start_time, host):
    """

    :param token:
    :param network:
    :param token_address:
    :param start_time:
    :return:
    """
    headers ={
        "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
        "X-API-KEY": "BQYfJyxvTyxUzCXQYdkhmhWQTloN2w9s"
    }
    flg = 0
    url = 'https://graphql.bitquery.io'
    mongo = my_mongo(host=host, port=27017)

    while True:
        if flg:
            break
        for _ in range(5):
            try:
                query = """{
                  ethereum(network: %s) {
                    dexTrades(
                      options: {limit: 2000, asc: "timeInterval.second"}
                      baseCurrency: {is: "%s"}
                      time: {after: "%s"}
                    ) {
                      transaction {
                        hash
                        txFrom {
                          address
                        }
                        to {
                          address
                        }
                      }
                      timeInterval {
                        second
                      }
                      buyAmount
                      buyAmountInUsd: buyAmount(in: USD)
                      buyCurrency {
                        symbol
                      }
                      sellAmount
                      sellAmountInUsd: sellAmount(in: USD)
                      sellCurrency {
                        symbol
                      }
                    }
                  }
                }""" % (network, token_address, start_time)

                request = requests.post(url, json={'query': query}, headers=headers, verify=False)
                if request.status_code == 200:
                    r = request.json()
                    try:
                        if len(r['data']['ethereum']['dexTrades']) == 0:
                            flg = 1
                            break
                    except TypeError:
                        break
                    df = []
                    for i in r['data']['ethereum']['dexTrades']:
                        transaction_id = i['transaction']['hash']
                        start_time = i['timeInterval']['second']
                        taker = i['transaction']['txFrom']['address']
                        buyAmount = float(i['buyAmount'])
                        sellAmount = float(i['sellAmount'])
                        buysymbol = i['buyCurrency']['symbol']
                        buyAmountInUsd = float(i['buyAmountInUsd'])
                        sellAmountInUsd = float(i['sellAmountInUsd'])

                        if buysymbol == token:
                            side = 'sell'
                            amount = buyAmount
                            if sellAmountInUsd != 0:
                                price = sellAmountInUsd / buyAmount
                            else:
                                price = buyAmountInUsd / buyAmount
                            address = taker

                        else:
                            side = 'buy'
                            amount = sellAmount
                            if buyAmountInUsd != 0:
                                price = buyAmountInUsd / sellAmount
                            else:
                                price = sellAmountInUsd / sellAmount
                            address = taker

                        rst = [token, token_address, network, side, amount, address, price, start_time, transaction_id]
                        df.append(rst)
                    # 将数据写入数据库
                    data = pd.DataFrame(df, columns=['token', 'token_address', 'network', 'side', 'amount',
                                                     'address', 'price', 'timestamp', 'transaction_id'])
                    mongo.df_to_mongodb(db_name='dex_trade', col_name='{}'.format(network), data=data)

                    start_time = (
                        pd.Timestamp(r['data']['ethereum']['dexTrades'][-1]['timeInterval']['second'])).strftime(
                        '%Y-%m-%dT%H:%M:%S')
                    transaction_id = r['data']['ethereum']['dexTrades'][-1]['transaction']['hash']
                    print(start_time, transaction_id)
                else:
                    print('403')
                    time.sleep(3)
            except:

                import traceback
                traceback.print_exc()
                time.sleep(3)
                pass


def get_latest_date(token, network):
    mongo = my_mongo(host='127.0.0.1', port=27017)
    col = mongo.client['dex_trade'][network]

    latest_date = [x['timestamp'] for x in col.find({'token': token}, {'_id': 0, 'timestamp': 1})]
    latest_date.sort()
    latest_date = latest_date[-1]
    return latest_date.replace(' ', 'T')


def init(token, network, token_address, start_time, host):
    """

    :param token:
    :param network:
    :param token_address:
    :param start_time:
    :param host:
    :return:
    """
    token_transaction_scraper(token, network, token_address, start_time, host)

def update(token, network, token_address, host):
    start_time = get_latest_date(token, network)
    token_transaction_scraper(token, network, token_address, start_time, host)
    print("完成更新 从{}更新到了{}".format(start_time, datetime.now()))
    pass

if __name__ == "__main__":

    token_address = '0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce'
    start_time = '2021-05-10T00:00:00'
    token = 'SHIB'
    network = "ethereum"
    host = '127.0.0.1'
    # init(token, network, token_address, start_time, host)
    update(token, network, token_address, host)


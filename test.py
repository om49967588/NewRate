
import pandas
import pandas as pd
import requests
import datetime
from datetime import datetime as dt
from M_RATE_NEW import get_rate_date_currency
import logging

FORMAT = '%(asctime)s %(levelname)s: %(message)s'
logging.basicConfig(level=logging.DEBUG, filename='M_RATE.log', filemode='a', format=FORMAT)

# Line notify 通知
def Daily_currency_notify(token, msg):
    headers = {
        "Authorization": "Bearer " + token, 
        "Content-Type" : "application/x-www-form-urlencoded"
    }
    payload = {'message': msg }
    r = requests.post("https://notify-api.line.me/api/notify", headers = headers, params = payload)
    return r.status_code

# 獲取今天與明天日期
def get_today_and_tomorrow():
    today = datetime.date.today()
    tomorrow = today + datetime.timedelta(days=1)
    # today = input("今天日期")
    # tomorrow = input("明天日期")
    str_today = str(today)
    return str_today, tomorrow

    cursor.execute(
        "select CURID, INRAT, OTRAT from FJT.dbo.M_Rate where TRNDT = '2023-07-18' AND CO_TYPE ='PY' order by TRNDT desc"
        )
    result = cursor.fetchall()
    print(result)
    for row in result:
        print (row)
#1234
if __name__ == '__main__':
    str_today, tomorrow = get_today_and_tomorrow()
    try:
        token ='1VPrxu0vjdZSGxQUXu0HtjNqJfaTf7vTbQJYxXzfz1V'
        dff = get_rate_date_currency(rate_date=str_today)
        # dff = get_rate_date_currency(rate_date='2023-07-19')
        df = pd.DataFrame(dff)                                 
        usd = f"美金  買 {df.iat[0, 1]:7.4f}  賣  {df.iat[0, 2]:7.4f}"  # f是拿來可以在""裡面用{}插入變數
        eur = f"歐元  買 {df.iat[3, 1]:7.4f}  賣  {df.iat[3, 2]:7.4f}" # df.loc[[1,3],["CURID","INRAT"]]是取索引1跟3的數值
        sgd = f"新幣  買 {df.iat[1, 1]:7.4f}  賣  {df.iat[1, 2]:7.4f}"  # df.iloc[[1,3],[0,2]] 索引1跟3的第0跟2欄位
        jpy = f"日圓  買 {df.iat[2, 1]:7.4f}   賣  {df.iat[2, 2]:7.4f}" # DataFrame參數以下網址
        cny = f"人民幣 買  {df.iat[4, 1]:2.4f}  賣  {df.iat[4, 2]:7.4f}"  # https://www.learncodewithmike.com/2020/11/python-pandas-dataframe-tutorial.html
        message = '\n' + str_today + '  即期匯率\n'
        message += f"{usd} \n{eur} \n{sgd} \n{jpy} \n{cny}"
        Daily_currency_notify(token, message)
 
    except:
        logging.exception('Catch an exception.')

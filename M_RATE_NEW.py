import pandas
import pandas as pd
import pyodbc
from datetime import datetime as dt
# noinspection PyUnresolvedReferences
from datetime import date
import numpy as np
import datetime
import ssl
import logging
import warnings

FORMAT = '%(asctime)s %(levelname)s: %(message)s'
logging.basicConfig(level=logging.DEBUG, filename='M_RATE.log', filemode='a', format=FORMAT)

warnings.filterwarnings('ignore')
def get_today_and_tomorrow():
    today = datetime.date.today()
    tomorrow = today + datetime.timedelta(days=1)
    # today = input("今天日期")
    # tomorrow = input("明天日期")
    str_today = str(today)
    return str_today, tomorrow


# 爬蟲爬取匯率並轉成DateFrame #
def get_rate_date_currency(rate_date):
    ssl._create_default_https_context = ssl._create_unverified_context
    # CURID=幣別
    # INRAT=即期匯率本行買入
    # OTRAT=即期匯率本行賣出
    # CASHINRAT=現金匯率本行買入
    # CASHOTRAT現金匯率本行賣出
    dfs = pandas.read_html("https://rate.bot.com.tw/xrt/all/" + rate_date)
    currency = dfs[0]
    currency = currency.iloc[:, 0:5] #取二維陣列 第0-4維的所有數據
    currency.columns = [u'CURID', u'CASHINRAT', u'CASHOTRAT', u'INRAT', u'OTRAT']
    currency[u'CURID'] = currency[u'CURID'].str.extract('\((\w+)\)')
    currency = currency.drop([u'CASHINRAT', u'CASHOTRAT'], axis=1)
    currency = currency.drop([x for x in range(19) if x != 0 if x != 5 if x != 7 if x != 14 if x != 18], axis=0)

    currency[u'INRAT'] = pd.DataFrame(currency[u'INRAT'], dtype=np.float64)
    currency[u'OTRAT'] = pd.DataFrame(currency[u'OTRAT'], dtype=np.float64)

    

    # 計算平均匯率
    avrat = currency.iloc[:, 1:3].mean(axis=1)
    currency[u'AVRAT'] = avrat

    df = pd.DataFrame(currency)
    print(currency)
    return df


# 用資料庫行事曆中的資料表(V_STDDAY)判斷是否為上班日 #
def determine_today_date_from_sql(cnxn, str_today):
    sqlcom_1 = "SELECT * FROM FJT.dbo.V_STDDAY where SDATE = " + "'" + str_today + "'"
    df_todaysql = pd.read_sql_query(sqlcom_1, con=cnxn)
    df2 = pd.DataFrame(df_todaysql)
    date = df2.iat[0, 1]  # 若為N或H為假日，空值為上班日
    # date2 = df2.iat[0, 0] # 日期
    return date


# 三旬匯率 回傳抓取匯率日期(last_work_day)及寫入SQL日期(ymd_date)#
def determine_rdday_date(date, cnxn, str_today):
    today_datetime = datetime.datetime.strptime(str_today, '%Y-%m-%d')
    years = int(today_datetime.strftime('%Y'))
    month = int(today_datetime.strftime('%m'))
    day = int(today_datetime.strftime('%d'))

    if day == 5:
        last_work_datetime = datetime.datetime.strptime(str_today, '%Y-%m-%d')
        if date != '':
            print('今天是假日，所以要找上一個工作日的匯率，再把匯率寫入11號')
            work_date = "0"
            while work_date != "":
                rd_sqlcom = "SELECT * FROM FJT.dbo.V_STDDAY where SDATE = " + "'" + str_today + "'"
                rd_df_sql = pd.read_sql_query(rd_sqlcom, con=cnxn)
                df3 = pd.DataFrame(rd_df_sql)
                work_date = df3.iat[0, 1]
                # print(day)
                if work_date == "":
                    # print('上班日')
                    break
                else:
                    # print('假日')
                    last_work_datetime = last_work_datetime - datetime.timedelta(days=1)
                    str_today = str(last_work_datetime)
            last_work_day = last_work_datetime.strftime('%Y-%m-%d')
            ymd = str(years) + '-' + str(month) + '-' + '11'
            ymd_date = datetime.datetime.strptime(ymd, '%Y-%m-%d')
            return last_work_day, ymd_date
        else:
            print('今天是平日，把今天匯率直接寫入11號')
            ymd = str(years) + '-' + str(month) + '-' + '11'
            ymd_date = datetime.datetime.strptime(ymd, '%Y-%m-%d')
            last_work_day = last_work_datetime.strftime('%Y-%m-%d')
            return last_work_day, ymd_date
    elif day == 15:
        last_work_datetime = datetime.datetime.strptime(str_today, '%Y-%m-%d')
        if date != '':
            print('今天是假日，所以要找上一個工作日的匯率，再把匯率寫入21號')
            work_date = "0"
            while work_date != "":
                rd_sqlcom = "SELECT * FROM FJT.dbo.V_STDDAY where SDATE = " + "'" + str_today + "'"
                rd_df_sql = pd.read_sql_query(rd_sqlcom, con=cnxn)
                df3 = pd.DataFrame(rd_df_sql)
                work_date = df3.iat[0, 1]
                # print(day)
                if work_date == "":
                    # print('上班日')
                    break
                else:
                    # print('假日')
                    last_work_datetime = last_work_datetime - datetime.timedelta(days=1)
                    str_today = str(last_work_datetime)
            last_work_day = last_work_datetime.strftime('%Y-%m-%d')
            ymd = str(years) + '-' + str(month) + '-' + '21'
            ymd_date = datetime.datetime.strptime(ymd, '%Y-%m-%d')
            return last_work_day, ymd_date
        else:
            print('今天是平日，把今天匯率直接寫入21號')
            ymd = str(years) + '-' + str(month) + '-' + '21'
            ymd_date = datetime.datetime.strptime(ymd, '%Y-%m-%d')
            last_work_day = last_work_datetime.strftime('%Y-%m-%d')
            return last_work_day, ymd_date
    elif day == 25:
        last_work_datetime = datetime.datetime.strptime(str_today, '%Y-%m-%d')
        if date != '':
            print('今天是假日，所以要找上一個工作日的匯率，再把匯率寫入1號')
            work_date = "0"
            while work_date != "":
                rd_sqlcom = "SELECT * FROM FJT.dbo.V_STDDAY where SDATE = " + "'" + str_today + "'"
                rd_df_sql = pd.read_sql_query(rd_sqlcom, con=cnxn)
                df3 = pd.DataFrame(rd_df_sql)
                work_date = df3.iat[0, 1]
                # print(day)
                if work_date == "":
                    # print('上班日')
                    break
                else:
                    # print('假日')
                    last_work_datetime = last_work_datetime - datetime.timedelta(days=1)
                    str_today = str(last_work_datetime)
            last_work_day = last_work_datetime.strftime('%Y-%m-%d')
            if month == 12:
                int_m = 1
                years = years + 1
            else:
                int_m = int(month) + 1
            ymd = str(years) + '-' + str(int_m) + '-' + '1'
            ymd_date = datetime.datetime.strptime(ymd, '%Y-%m-%d')
            return last_work_day, ymd_date
        else:
            print('今天是平日，把今天匯率直接寫入1號')
            if month == 12:
                int_m = 1
                years = years + 1
            else:
                int_m = int(month) + 1
            ymd = str(years) + '-' + str(int_m) + '-' + '1'
            ymd_date = datetime.datetime.strptime(ymd, '%Y-%m-%d')
            last_work_day = last_work_datetime.strftime('%Y-%m-%d')
            return last_work_day, ymd_date
    else:
        print('今天不是5、15、25號，所以三旬匯率不執行')
        last_work_day = "False"
        ymd_date = "False"
        return last_work_day, ymd_date


# 三旬匯率寫入SQL
def insert_rdrate_into_sql(df, cursor, ymd_date):
    for row in df.itertuples():
        cursor.execute(
            "INSERT INTO FJT.dbo.M_RATE (CO_TYPE,TRNDT,CURID,INRAT,OTRAT,AVRAT,USRID,ENTDT) VALUES (?,?,?,?,?,?,?,?)",
            ("RD", ymd_date, row.CURID, row.INRAT, row.OTRAT, row.AVRAT, "MIS", dt.now()))


# 一般匯率 回傳抓取匯率日期(py_today)及寫入SQL日期(py_tomorrow)#
def determine_pyday_date(date, str_today, tomorrow):
    py_today = str_today
    if date != '':
        print('今天是假日，所以一般匯率不執行')
        py_today = "False"
        py_tomorrow = "False"
        return py_today, py_tomorrow
    else:
        print('今天是上班日，所以一般匯率執行')
        str_tomorrow = str(tomorrow)
        py_tomorrow = datetime.datetime.strptime(str_tomorrow, '%Y-%m-%d')
        work_date = '0'
        while work_date != '':
            sqlcom = "SELECT * FROM FJT.dbo.V_STDDAY where SDATE = " + "'" + str_tomorrow + "'"
            df_sql = pd.read_sql_query(sqlcom, con=cnxn)
            df3 = pd.DataFrame(df_sql)
            work_date = df3.iat[0, 1]
            if work_date == "":
                # print('上班日')
                break
            else:
                # print('假日')
                py_tomorrow = py_tomorrow + datetime.timedelta(days=1)
                str_tomorrow = str(py_tomorrow)
        return py_today, py_tomorrow


# 一般匯率寫入SQL
def insert_pyrate_into_sql(df, cursor, py_tomorrow):
    for row in df.itertuples():
        cursor.execute(
            "INSERT INTO FJT.dbo.M_RATE (CO_TYPE,TRNDT,CURID,INRAT,OTRAT,AVRAT,USRID,ENTDT) VALUES (?,?,?,?,?,?,?,?)",
            ("PY",py_tomorrow, row.CURID, row.INRAT, row.OTRAT, row.AVRAT, "MIS", dt.now()))


# 設定連線 ms sql 的連線參數
def connect_to_sql():
    cnxn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=172.16.1.5;DATABASE=FJT;UID=erp_adm;PWD=/apptzen/')
    cnxn.autocommit = True
    cursor = cnxn.cursor()
    if cnxn:
        print("資料庫連線成功!")
        logging.info('資料庫連線成功!')
    else:
        print("資料庫連線失敗!")
        logging.error('資料庫連線失敗!')
    return cnxn, cursor


# 設定關閉mssql連線
def close_connect_to_sql(cnxn):
    cnxn.commit()  # 資料庫寫入
    cnxn.close()  # 關閉資料連線s
    print("關閉資料連線!")
    logging.info('關閉資料連線!')

'''
if __name__ == '__main__':
    try:
        cnxn, cursor = connect_to_sql()
        str_today, tomorrow = get_today_and_tomorrow()
        date = determine_today_date_from_sql(cnxn=cnxn, str_today=str_today)
        last_work_day, ymd_date = determine_rdday_date(date=date, cnxn=cnxn, str_today=str_today)
        py_today, py_tomorrow = determine_pyday_date(date=date, str_today=str_today, tomorrow=tomorrow)

        if last_work_day != "False" and ymd_date != "False":
            insert_rdrate_into_sql(df=get_rate_date_currency(rate_date=last_work_day), cursor=cursor, ymd_date=ymd_date)
            logging.info('三旬匯率執行')
        else:
            print("三旬匯率不執行")
            logging.info('三旬匯率不執行')

        if py_today != "False" and py_tomorrow != "False":
            insert_pyrate_into_sql(df=get_rate_date_currency(rate_date=str_today), cursor=cursor,
                                  py_tomorrow=py_tomorrow) 
            logging.info('一般匯率執行')
        else:
            print("一般匯率不執行")
            logging.info('一般匯率不執行')

        close_connect_to_sql(cnxn)

    except:
        logging.exception('Catch an exception.')
'''
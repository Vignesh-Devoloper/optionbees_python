import time
from datetime import datetime
import requests
import mysql.connector
import pandas as pd
from flask import Flask,request

app = Flask(__name__)

def api_fetch_db_push(From,to):
    # From = "02/05/2022 09:15"
    # to = "02/05/2022 15:30"

    from_dt = datetime.strptime(From, "%d/%m/%Y-%H:%M")
    from_dt_timestamp = datetime.timestamp(from_dt)

    to_dt = datetime.strptime(to, "%d/%m/%Y-%H:%M")
    to_dt_timestamp = datetime.timestamp(to_dt)


    Flag = True
    end = from_dt_timestamp

    t1 = datetime.now()
    while (Flag):
        unix_timestamp = end  # current_time.timestamp() # works if Python >= 3.3
        unix_timestamp_plus_5_min = unix_timestamp + (5 * 60)
        end = unix_timestamp_plus_5_min

        if round(end) == to_dt_timestamp:
            Flag = False

        accessKey = '9ca07521-938f-425c-abda-8edda85d1f22'
        exchange = 'NFO'
        periodicity = 'Minute'
        period = '1'
        From = str(round(unix_timestamp))
        To = str(round(end))

        t3 = datetime.now()

        url = "http://nimblerest.lisuns.com:4531/getexchangesnapshot/?accessKey=%s&exchange=%s&periodicity=%s&period=%s&From=%s&To=%s" % (
        accessKey, exchange, periodicity, period, From, To)

        payload = {}
        headers = {}
        try:
            response = requests.request("GET", url, headers=headers, data=payload)
        except requests.exceptions.RequestException as e:  # This is the correct syntax
            print("Error Occured")
            time.sleep(1)
            response = requests.request("GET", url, headers=headers, data=payload)
        t4 = datetime.now()
        # print("Error :",response.text)
        response_data = response.json()
        current_datetime = datetime.now()
        print("Request took ", datetime.fromtimestamp(unix_timestamp), " - ", datetime.fromtimestamp(end),
              " to complete :", (t4 - t3).seconds, " seconds")
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Stoneage&8",
            database="option_chain"
        )

        mycursor = mydb.cursor()
        t5 = datetime.now()
        for i in range(len(response_data['EXCHANGESNAPSHOTITEMS'])):
            # print("SNAP:",i)
            API_fetched_ltt = datetime.fromtimestamp(response_data['EXCHANGESNAPSHOTITEMS'][i]['LASTTRADETIME'] / 1000)
            data = response_data['EXCHANGESNAPSHOTITEMS'][i]['SNAPSHOTITEMS']

            df = pd.DataFrame(data)
            for i in range(len(df)):
                created_by = 'CODIFI_BOT'
                INSTRUMENTIDENTIFIER = df['INSTRUMENTIDENTIFIER'][i].split('_')
                Instrument_Type = INSTRUMENTIDENTIFIER[0]
                Instrument_Name = INSTRUMENTIDENTIFIER[1]
                Instrument_Date = datetime.strptime(INSTRUMENTIDENTIFIER[2], '%d%b%Y')
                lt_time = datetime.fromtimestamp(df['LASTTRADETIME'][i] / 1000)
                if len(INSTRUMENTIDENTIFIER) > 3:
                    Instrument_Option_type = INSTRUMENTIDENTIFIER[3]
                    Instrument_Strike_price = INSTRUMENTIDENTIFIER[4]
                else:
                    Instrument_Option_type = ''
                    Instrument_Strike_price = ''
                sql = "INSERT INTO api_fetch_data (created_on,created_by,Exchange,Instrument_identifier,Last_trade_time,Traded_qty,Open_intrest,Open,High,Low,Close,Token_number,Instrument_type,Symbol_Name,Expiry_Date,Option_type,Strike_Price,API_fetched_ltt) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
                val = (
                    current_datetime, created_by, df['EXCHANGE'][i], df['INSTRUMENTIDENTIFIER'][i], lt_time,
                    df['TRADEDQTY'][i],
                    df['OPENINTEREST'][i], df['OPEN'][i], df['HIGH'][i], df['LOW'][i], df['CLOSE'][i],
                    df['TOKENNUMBER'][i],
                    Instrument_Type, Instrument_Name, Instrument_Date, Instrument_Option_type, Instrument_Strike_price,
                    API_fetched_ltt)
                mycursor.execute(sql % val)
        mydb.commit()
        # print(mycursor.rowcount, "record inserted.")
        mycursor.close()
        mydb.close()
        t6 = datetime.now()
        print("To Insert ", datetime.fromtimestamp(unix_timestamp), " - ", datetime.fromtimestamp(end),
              " minutes Data in DB :", (t6 - t5).seconds, " seconds")
    t2 = datetime.now()

    print("To Complete full process of ", from_dt, " - ", to_dt, " to complete:", t2 - t1)
    return {"stat": "ok" , "msg":"Process Completed"}

@app.route("/manual_push")
def manual_push():
    auth = request.authorization
    if auth:
        if auth['username'] == 'ADMIN' and auth['password'] == 'Stoneage&8':
            from_date = request.args.get('from')
            to_date = request.args.get('to')
            response = api_fetch_db_push(from_date,to_date)
            return response
        else:
            return {'stat':'not_ok','emsg':'Invalid Login credentials'}
    else:
        return {'stat':'not_ok','emsg':'Please Login'}


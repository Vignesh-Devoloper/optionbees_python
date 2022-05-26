from flask import Flask, request
import mysql.connector
# from mysql.connector.errors import Error
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
app = Flask(__name__)

@app.route("/server_check",methods=['POST'])
def server_check():
    return "Connected"

@app.route("/get_underlying",methods=['POST'])
def get_underlying():
    mydb = mysql.connector.connect(
        host="127.0.0.1",
        user="amadmin",
        password="amAdmin$2021.08",
        database="optionbees"
    )

    mycursor = mydb.cursor()
    try:
        try:
            mycursor.execute("SELECT DISTINCT underlying FROM tbl_previous_day_data")
            # NB : you won't get an IntegrityError when reading
        except (mysql.connector.Error, mysql.connector.Warning) as e:
            print(e)
            return {'stat':'Not_ok','emsg':str(e)}
        try:
            db_underlying_fetched_data = mycursor.fetchall()
        except TypeError as e:
            print(e)
            return {'stat':'Not_ok','emsg':str(e)}
    finally:
        mydb.close()

    list_underlying = [db_underlying_fetched_data[i][0] for i in range(len(db_underlying_fetched_data))]


    return {'list_underlying': list_underlying , 'stat':'ok'}

@app.route("/get_expiry_underlying",methods=['POST'])
def get_expiry_underlying():
    data = request.get_json()
    print(data)
    mydb = mysql.connector.connect(
        host="127.0.0.1",
        user="amadmin",
        password="amAdmin$2021.08",
        database="optionbees"
    )

    mycursor = mydb.cursor()
    try:
        try:
            mycursor.execute("SELECT  distinct expiry_date FROM tbl_previous_day_data where underlying = '%s';"%data['underlying'])
            # NB : you won't get an IntegrityError when reading
        except (mysql.connector.Error, mysql.connector.Warning) as e:
            print(e)
            return {'stat': 'Not_ok', 'emsg': str(e)}
        try:
            db_underlying_expiry_fetched_data = mycursor.fetchall()
        except TypeError as e:
            print(e)
            return {'stat': 'Not_ok', 'emsg': str(e)}
    finally:
        mydb.close()
    print(type(db_underlying_expiry_fetched_data[0][0]))
    # list_underlying_expiry = [datetime.strptime(db_underlying_expiry_fetched_data[i][0],'%Y-%m-%d %H:%M:%S').strftime("%Y%b%d").upper() for i in range(len(db_underlying_expiry_fetched_data))]
    list_underlying_expiry = [datetime.strptime(db_underlying_expiry_fetched_data[i][0], '%Y-%m-%d %H:%M:%S') for i in range(len(db_underlying_expiry_fetched_data))]

    current_month_expiry = None
    for i in range(len(list_underlying_expiry)):
        if list_underlying_expiry[i].strftime('%m') == datetime.now().strftime('%m') and list_underlying_expiry[i].strftime('%Y') == datetime.now().strftime('%Y'):
            current_month_expiry = list_underlying_expiry[i].strftime('%d%b%y').upper()
    next_month_expiry = 0
    for i in range(len(list_underlying_expiry)):
        if list_underlying_expiry[i].strftime('%m') == (datetime.now() + relativedelta(months=1)).strftime('%m') and list_underlying_expiry[i].strftime('%Y') == (datetime.now() + relativedelta(months=1)).strftime('%Y'):
            next_month_expiry = list_underlying_expiry[i].strftime('%d%b%y').upper()
    next_three_expiry = []
    if data['underlying'].upper() == 'NIFTY' or data['underlying'].upper() == 'BANKNIFTY' or data['underlying'].upper() == 'FINNIFTY' or data['underlying'].upper() == 'MIDCPNIFTY':
        for i in range(len(list_underlying_expiry)):
            if list_underlying_expiry[i].strftime('%m') == datetime.now().strftime('%m') and list_underlying_expiry[i].strftime('%Y') == datetime.now().strftime('%Y') and current_month_expiry != list_underlying_expiry[i].strftime('%d%b%y').upper():
                next_three_expiry.append(list_underlying_expiry[i].strftime('%d%b%y').upper())
            if len(next_three_expiry) < 3:
                if list_underlying_expiry[i].strftime('%m') == (datetime.now() + relativedelta(months=1)).strftime('%m') and list_underlying_expiry[i].strftime('%Y') == (datetime.now() + relativedelta(months=1)).strftime('%Y') and next_month_expiry != list_underlying_expiry[i].strftime('%d%b%y').upper():
                    next_three_expiry.append(list_underlying_expiry[i].strftime('%d%b%y').upper())
                if len(next_three_expiry) >= 3:
                    break
    next_three_expiry.append(current_month_expiry)
    next_three_expiry.append(next_month_expiry)
    return {
            "underlying":data['underlying'].upper(),
            "underlying_expiry":next_three_expiry,
            "stat":"Ok"
            }

@app.route("/get_strike_data",methods=['POST'])
def get_strike_data():
    data = request.get_json()
    symbol = data['underlying']
    expiry = datetime.strptime(data['expiry'],'%d%b%y').strftime("%Y-%m-%d %H:%M:%S")
    interval = data['interval']
    print(symbol,expiry,interval)
    mydb = mysql.connector.connect(
        host="127.0.0.1",
        user="amadmin",
        password="amAdmin$2021.08",
        database="optionbees"
    )

    mycursor = mydb.cursor()
    try:
        try:
            mycursor.execute("SELECT  * FROM tbl_previous_day_data where underlying = '%s' and expiry_date= '%s';" % (symbol, expiry))
            # NB : you won't get an IntegrityError when reading
        except (mysql.connector.Error, mysql.connector.Warning) as e:
            print(e)
        #             return {'stat': 'Not_ok', 'emsg': str(e)}
        try:
            db_data = mycursor.fetchall()
            field_names = [i[0] for i in mycursor.description]
        except TypeError as e:
            print(e)
    #             return {'stat': 'Not_ok', 'emsg': str(e)}
    finally:
        mydb.close()

    df_data = pd.DataFrame(db_data, columns=field_names)
    underlying_fut_script = df_data[(df_data['underlying'] == symbol) & (df_data['expiry_date'] == expiry) & (df_data['option_type'] == 'XX')].values.tolist()
    token_underlying_fut_script = ''
    if len(underlying_fut_script) != 0:
        token_underlying_fut_script = underlying_fut_script[0][5]
    lot_size = int(df_data.values.tolist()[0][6])
    spot = 16200
    min_spot_value = str(spot - (interval * lot_size))
    max_spot_value = str(spot + (interval * lot_size))
    total_strike_data = df_data.loc[(df_data['strike_price'] >= min_spot_value) & (df_data['strike_price'] <= max_spot_value)]
    lot_size = total_strike_data.values.tolist()[0][6]
    tick_size = total_strike_data.values.tolist()[0][7]
    exch_seg = total_strike_data.values.tolist()[0][5]
    strikes = []
    distinct_strike_price = total_strike_data['strike_price'].drop_duplicates().tolist()
    mydb = mysql.connector.connect(
        host="127.0.0.1",
        user="amadmin",
        password="amAdmin$2021.08",
        database="optionbees"
    )

    mycursor = mydb.cursor()
    try:
        try:
            mycursor.execute("SELECT * FROM gdfl_table;")
            # NB : you won't get an IntegrityError when reading
        except (mysql.connector.Error, mysql.connector.Warning) as e:
            print(e)
        #             return {'stat': 'Not_ok', 'emsg': str(e)}
        try:
            db_gdfl_data = mycursor.fetchall()
            gdfl_col_names = [i[0] for i in mycursor.description]
        except TypeError as e:
            print(e)
    #             return {'stat': 'Not_ok', 'emsg': str(e)}
    finally:
        mydb.close()
    gdfl_df = pd.DataFrame(db_gdfl_data, columns=gdfl_col_names)
    for i in range(len(distinct_strike_price)):
        #     total_strike_data[(total_strike_data['strike_price']=='16700')&].values.tolist()
        call = total_strike_data[(total_strike_data['strike_price'] == distinct_strike_price[i]) & (
                    total_strike_data['option_type'] == 'CE')].to_dict('records')
        put = total_strike_data[(total_strike_data['strike_price'] == distinct_strike_price[i]) & (
                    total_strike_data['option_type'] == 'PE')].to_dict('records')

        gdfl_call_token_filter = gdfl_df[gdfl_df['token'] == call[0]['token']]
        if len(gdfl_call_token_filter) != 0:
            latest_number = len(gdfl_call_token_filter)-1
            call_token_ltp = gdfl_call_token_filter.values.tolist()[latest_number][8]
            call_token_oi = gdfl_call_token_filter.values.tolist()[latest_number][7]

        call_data = {
            "oi": call_token_oi,
            "token": call[0]['token'],
            "pdc": call[0]['pdc'],
            "ltp": call_token_ltp,
            "pdoi": call[0]['pdoi'],
            "forInsName": call[0]['formatted_ins_name']
        }
        gdfl_put_token_filter = gdfl_df[gdfl_df['token'] == put[0]['token']]
        if len(gdfl_put_token_filter) != 0:
            latest_number = len(gdfl_put_token_filter) - 1
            put_token_ltp =gdfl_put_token_filter.values.tolist()[latest_number][8]
            put_token_oi = gdfl_put_token_filter.values.tolist()[latest_number][7]
        put_data = {
            "oi": put_token_oi,
            "token": put[0]['token'],
            "pdc": put[0]['pdc'],
            "ltp": put_token_ltp,
            "pdoi": put[0]['pdoi'],
            "forInsName": put[0]['formatted_ins_name']
        }
        data = {"strikeprice": distinct_strike_price[i], "PE": put_data, "CE": call_data}
        strikes.append(data)
    fut_token_ltp=''
    gdfl_fut_token_filter = gdfl_df[gdfl_df['token'] == token_underlying_fut_script]
    if len(gdfl_fut_token_filter) != 0:
        latest_number = len(gdfl_fut_token_filter) - 1
        fut_token_ltp = gdfl_fut_token_filter.values.tolist()[latest_number][8]
    send_data = {
        "stat": "Ok",
        "result": {
            "lotsize": lot_size,
            "ticksize": tick_size,
            "exchange": exch_seg,
            "spotLTP": "",
            "spotToken": "",
            "futLTP": fut_token_ltp,
            "futToken": token_underlying_fut_script,
            "data": {}
        }

    }
    send_data['result']['data'] = strikes
    print(send_data)
    return send_data
from datetime import datetime,timedelta
from nsepython import holiday_master
import pandas as pd
holiday_cal = pd.json_normalize(holiday_master()['FO'])

def check_holiday(date):
    # Get previous day if sat or sunday
    # date = datetime.strptime('2022-10-24', '%Y-%m-%d')
    # print(date.strftime('%d-%b-%Y') in holiday_cal['tradingDate'].tolist())
    nse_holiday_flag = date.strftime('%d-%b-%Y') in holiday_cal['tradingDate'].tolist()
    Holiday_flag = nse_holiday_flag
    while (nse_holiday_flag):
        date = date - timedelta(days=1)
        if date.strftime('%d-%b-%Y') in holiday_cal['tradingDate'].tolist():
            nse_holiday_flag = True
        else:
            nse_holiday_flag = False

    if date.weekday() == 6:
        Holiday_flag = True
        cur_Date = date - timedelta(days=2)
    elif date.weekday() == 5:
        Holiday_flag = True
        cur_Date = date - timedelta(days=1)
    else:
        cur_Date = date
    return {"Date":cur_Date,"Holiday":Holiday_flag}


date  = check_holiday(datetime.now())

print(date)
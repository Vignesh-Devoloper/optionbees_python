from utilities.CalendarUtil import CalendarUtil
from datetime import datetime


today = datetime.now()
print(datetime.now())
CalendarUtil.is_holiday(today)
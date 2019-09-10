import calendar
from datetime import date, timedelta
from time import strftime, localtime

year = strftime("%Y",localtime())
mon = strftime("%m",localtime())
day = strftime("%d",localtime())
hour = strftime("%H",localtime())
min = strftime("%M",localtime())
sec = strftime("%S",localtime())

#今天的日期
def today():
    return date.today()

def todaystr():
    return year + mon + day


def datetime():
    return strftime("%Y-%m-%d %H:%m:%S",localtime())


def datetimestr():
    return year + mon + day + hour + min + sec


def get_day_of_day(n = 0):
    if n < 0:
        n = abs(n)
        return date.today() - timedelta(days = n)
    else:
        return date.today() + timedelta(days = n)


def get_days_of_month(year, mon):
    return calendar.monthrange(year,mon)[1]


def addzero(n):
    nabs = abs(int(n))
    if nabs < 10:
        return "0" + str(nabs)
    else:
        return nabs



def getyearandmonth(n):
    thisyear = int(year)
    thismonth = int(mon)
    totalmon = thismonth + n
    if n >= 0:
        if totalmon <= 12:
            days = str(get_days_of_month(thisyear,totalmon))
            totalmon = addzero(totalmon)
            return year,totalmon,days
        else:
            i = totalmon // 12
            j = totalmon % 12
            if j == 0:
                i -= 1
                j = 12
            thisyear += i
            days = str(get_days_of_month(thisyear,j))
            j = addzero(j)
            return str(thisyear),str(j),days
    else:
        if totalmon > 0 and totalmon < 12:
            days = str(get_days_of_month(thisyear,totalmon))
            totalmon = addzero(totalmon)
            return year,totalmon,days
        else:
            i = totalmon // 12
            j = totalmon % 12
            if(j == 0):
                i -= 1
                j = 12
            thisyear += 1
            days = str(get_days_of_month(thisyear,j))
            j = addzero(j)
            return str(thisyear),str(j),days




def get_month_today(mon = 0):
    (y,m,d) = getyearandmonth(mon)
    arr = (y,m,d)
    if int(day) < int(d):
        arr = (y,m,day)
    return "-".join("%s"%i for i in arr)


def get_firstday_month(n = 0):
    (y,m,d) = getyearandmonth(n)
    arr = (y,m,"01")
    return "-".join("%s"%i for i in arr)
    pass


def main():
    print('Hour:',hour)
    print('today is:',today())
    print("today is:",todaystr())
    print("the date time is:",datetime())
    print("date time is:",datetimestr())
    print('2 days after today is',get_day_of_day(2))
    print("2 months after today is:",get_month_today(-2))
    print("2 months after this month is:",get_firstday_month(2))


if __name__ == "__main__":
    main()
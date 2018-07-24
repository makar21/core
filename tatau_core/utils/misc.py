import calendar
import datetime


def current_timestamp():
    return calendar.timegm(datetime.datetime.utcnow().utctimetuple())
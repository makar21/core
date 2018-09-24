import calendar
import datetime
import os


def current_timestamp():
    return calendar.timegm(datetime.datetime.utcnow().utctimetuple())


def get_dir_size(start_path='.'):
    total_size = 0
    for dir_path, dir_names, file_names in os.walk(start_path):
        for f in file_names:
            fp = os.path.join(dir_path, f)
            total_size += os.path.getsize(fp)
    return total_size

import time

def unix_timestamp(dt):
    return int(time.mktime(dt.timetuple()))

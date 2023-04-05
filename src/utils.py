import datetime as dt
from dateutil.relativedelta import relativedelta

# fill dates that dont exist
def fill_empty_dates(dt_from, dt_upto, group_type, agg_res):
    date_start = dt.datetime.fromisoformat(dt_from)
    date_end = dt.datetime.fromisoformat(dt_upto)

    curr_date = date_start

    while curr_date <= date_end:
        curr_date_iso = dt.datetime.isoformat(curr_date)

        if curr_date_iso not in agg_res['labels']:
            agg_res['labels'].append(curr_date_iso)
            agg_res['labels'].sort()

            date_idx = agg_res['labels'].index(curr_date_iso)
            agg_res['dataset'].insert(date_idx, 0)
            
        if group_type == "month":
            curr_date += relativedelta(months=1) 
        
        elif group_type == "day":
            curr_date += dt.timedelta(days=1) 
        
        elif group_type == "hour":
            curr_date += dt.timedelta(hours=1) 


def to_isoformat(values):
    str_to_format = "-".join([f"0{v}" if v < 10 else str(v) for v in values])
    if len(values) < 3:
        str_to_format += "-01"

    date_time_raw = dt.datetime.fromisoformat(str_to_format)
    isoformatted = dt.datetime.isoformat(date_time_raw)

    return isoformatted
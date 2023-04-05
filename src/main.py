import os
import pymongo as pm
import datetime as dt

from .conf import PATH_TO_BSON, DB_NAME
from .utils import fill_empty_dates, to_isoformat


def pipeline_aggregation(dt_from, dt_upto, group_type):
    stage_sort = {
        "$sort": {"_id": pm.ASCENDING},
    }
    stage_match = {
        "$match": {
            "dt": {
                "$gte": dt.datetime.fromisoformat(dt_from),
                "$lte": dt.datetime.fromisoformat(dt_upto),
            }
        }
    }
    stage_group = {
        "$group": {"_id": {"year": {"$year": "$dt"}}, "dataset": {"$sum": "$value"}}
    }
    if group_type == "hour":
        to_update = {
            "month": {"$month": "$dt"},
            "day": {"$dayOfMonth": "$dt"},
            "hour": {"$hour": "$dt"},
        }
        stage_group["$group"]["_id"].update(to_update)

    elif group_type == "day":
        to_update = {
            "month": {"$month": "$dt"},
            "day": {"$dayOfMonth": "$dt"},
        }
        stage_group["$group"]["_id"].update(to_update)

    elif group_type == "month":
        stage_group["$group"]["_id"].update({"month": {"$month": "$dt"}})
    
    pipeline = [
        stage_match,
        stage_group,
        stage_sort,

    ]
    return pipeline


def aggregate_data(dt_from, dt_upto, group_type, data_collection):
    pipeline = pipeline_aggregation(dt_from, dt_upto, group_type)
    aggregated_data = data_collection.aggregate(pipeline)
    
    res_dict = {"dataset": [], "labels": []}

    for data in aggregated_data:
        date_time_values = data["_id"].values()
        label = to_isoformat(date_time_values)

        res_dict["labels"].append(label)
        res_dict["dataset"].append(data["dataset"])

    fill_empty_dates(dt_from, dt_upto, group_type, res_dict)
    
    return res_dict

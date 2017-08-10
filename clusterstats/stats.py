"""
stats.py
~~~~~~~~
"""
import json
import pandas as pd

FIELD_APPLICATION = 'Application'
FIELD_VERSION = 'Version'
FIELD_SUCCESS_COUNT = 'Success_Count'

OPERATOR_ADD = "+"

def calc_qos(total_queries, success_queries_cnt):
    """ Calculate QoS """
    return (float(success_queries_cnt)/float(total_queries)) * 100

def check_qos(threshold, total_queries, success_queries_cnt):
    """ Validate QoS """
    return calc_qos(total_queries, success_queries_cnt) >= threshold

def calc_stats(data, group_by_fields, aggregate_field, aggregate_operator):
    """ Generic function to apply group by and aggregation operation on the dataset.
    Args:
    data - list of dictionary
    group_by_fields - fields that will be grouped by
    aggregate_field - field on which aggreagation operator applied
    aggregate_operator - aggregation operation, currently supports only sum.

    Returns: DataFrame Object
    """
    data_frame = pd.read_json(json.dumps(data))

    if aggregate_operator != OPERATOR_ADD:
        raise ValueError("Unsupported aggregate operator:{}".format(aggregate_operator))

    return data_frame.groupby(group_by_fields).agg({aggregate_field : sum})


def write_stats(data_frame, output_dir):
    """Given the data_frame write csv output in the output directory with the
    filename as current time stamp.
    Args:
    - data_frame: DataFrame that to be serialized to the file.
    - output_dir: File output directory.
    Returns: the file path
    """
    import time
    import os.path
    millis = int(round(time.time() * 1000))
    path = os.path.join(output_dir, "{}.csv".format(millis))
    data_frame.to_csv(path)
    return path

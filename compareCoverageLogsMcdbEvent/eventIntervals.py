import boto3
import os
import json
import time
import pytest
import pandas as pd
import random
import datetime
import numpy as np
from io import BytesIO

RAW_BUCKET = 'haoming-canserver-raw-test'
PARSED_BUCKET = 'haoming-canserver-test'
EVENT_BUCKET = 'haoming-canserver-event-test'
# RAW_BUCKET = 'matt3r-canserver-raw-us-west-2'
# PARSED_BUCKET = 'matt3r-canserver-us-west-2'
# EVENT_BUCKET = 'matt3r-canserver-event-us-west-2'
s3 = boto3.client('s3')

    
def get_intervals():
    result_file = s3.get_object(Bucket=EVENT_BUCKET, Key=f'{prefix}/{date}.json')     
    str_body = result_file["Body"].read().decode()
    event_data = json.loads(str_body)
    spreads = []
    for spread in event_data['imu_telematics']['parked_state']:
        spreads.append([spread['timestamp'][0], spread['timestamp'][1]])
        
    for spread in event_data['imu_telematics']['driving_state']:
        spreads.append([spread['start'], spread['end']])
    
    timestamps = []
    for spread in spreads:
        timestamps.extend(list(range(int(spread[0]), int(spread[1]+1))))
    timestamps = sorted(timestamps, reverse=False)
    prev = 0
    intervals = []
    interval = []
    for curr in timestamps:
        if prev == 0: 
            prev = curr
            continue
        if curr - prev < 5: 
            interval.append(curr)
            prev = curr
        else:
            intervals.append([interval[0], interval[-1]])
            interval = []
            interval.append(curr)
            prev = curr
    intervals.append([interval[0], prev])
    
    interval_dict = {}
    interval_dict['data'] = intervals
    with open('./data/eventIntervals.json', 'w') as outfile:
        json.dump(interval_dict, outfile)


if __name__ == "__main__":
    # prefix = "cheung/k3yusb-e731c27b"
    prefix = "franco-test/key789"
    date = "2023-04-18"
    get_intervals()
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

def compare():
    with open(file1) as f:
        data1 = json.load(f)
    with open(file2) as f:
        data2 = json.load(f)
    
    filenames1 = [i['filename'] for i in data1['item']]
    filenames2 = [i['filename'] for i in data2['item']]
    missing1 = [i for i in filenames2 if i not in filenames1]
    missing2 = [i for i in filenames1 if i not in filenames2]
    print("missing1", missing1)
    print("missing2", missing2)
    
def compareOrigin():
    with open(file1) as f:
        data1 = json.load(f)
    directory = os.listdir("./data/bin_logs/")
    
    filenames1 = []
    for i in data1['item']:
        filenames1.append(i['filename'])
    
    missing = [i for i in directory if i not in filenames1]
    print(missing)
        
if __name__ == "__main__":
    file1 = "./data/debug.json"
    # prefix = "franco-test/key789"
    # date = "2023-04-18"
    # compare()
    compareOrigin()
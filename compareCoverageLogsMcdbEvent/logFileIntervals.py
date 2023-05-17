import boto3
import os
import pandas as pd
from datetime import datetime, timezone
import json

RAW_BUCKET = 'haoming-canserver-raw-test'
PARSED_BUCKET = 'haoming-canserver-test'
EVENT_BUCKET = 'haoming-canserver-event-test'
PREFIX="cheung/k3yusb-e731c27b/"


s3_client = boto3.client('s3')
def download_files():
    filenames = s3_client.list_objects(Bucket=RAW_BUCKET, Prefix=PREFIX)
    for filename in filenames['Contents']:
        download_path = "./data/bin_logs/" + filename['Key'].replace(PREFIX, '')
        s3_client.download_file(RAW_BUCKET, filename['Key'], download_path)
        
        
def convert_files(): 
    path = './data/bin_logs/'  
    files = os.listdir(path)
    for file in files:
        os.system(f'python3 ./can_reader.py {path+file}')
        
        
def parse_csv():
    relevant_log_files = []
    timestamp_dict = {}
    timestamp_dict['data'] = []
    path = './data/csv_logs/'  
    files = os.listdir(path)
    print("All files submitted:", len(files))
    for file in files:
        df = pd.read_csv(path+file) 
        df['unix'] = df['timestamp'].apply(lambda x: int(datetime.timestamp(datetime.strptime(x,"%Y-%m-%d %H:%M:%S.%f"))))
        timestamps = df['unix'].tolist()
        prev = 0
        for curr in timestamps:
            if curr < start_stamp or curr > end_stamp: continue
            if file not in relevant_log_files:relevant_log_files.append(file)
            if curr == prev: continue
            timestamp_dict['data'].append(curr)
            prev = curr
    timestamp_dict['data'] = sorted(timestamp_dict['data'], reverse=False) 
    with open('./data/logFileTimestamps.json', 'w') as outfile1:
        json.dump(timestamp_dict, outfile1)
    with open('./data/relevant_filenames.json', 'w') as outfile2:
        json.dump(relevant_log_files, outfile2)
    print("Relevant_log_files:", len(relevant_log_files))
        
        
def get_intervals():
    interval_dict = {}
    with open('./data/logFileTimestamps.json') as json_file:
        data = json.load(json_file)
        timestamps = data['data']
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
    print('intervals :', intervals)
    interval_dict['data'] = intervals
    with open('./data/logFileIntervals.json', 'w') as outfile:
        json.dump(interval_dict, outfile)
     
                
if __name__ == "__main__":
    # year,month, day
    start_stamp = datetime.timestamp(datetime(int(2023), int(4), int(18),tzinfo =timezone.utc ))
    end_stamp = datetime.timestamp(datetime(int(2023), int(4), int(19),tzinfo =timezone.utc ))
    print('start_day:', start_stamp, 'end_day :', end_stamp)
    # download_files()
    # convert_files()
    parse_csv()
    get_intervals()
    
    
    
    
    
#def parse_csv():
#     timestamp_dict = {}
#     timestamp_dict['data'] = []
#     path = './data/csv_logs/'  
#     files = os.listdir(path)
#     print(files)
#     for file in files:
#         df = pd.read_csv(path+file) 
#         df['unix'] = df['timestamp'].apply(lambda x: int(datetime.timestamp(datetime.strptime(x,"%Y-%m-%d %H:%M:%S.%f"))))
#         # print(df['unix'].head(n=10))
#         timestamps = df['unix'].tolist()
#         # print(timestamps[:10])
#         prev = 0
#         for curr in timestamps:
#             if curr == prev: continue
#             timestamp_dict['data'].append(curr)
#             prev = curr
#     timestamp_dict['data'] = sorted(timestamp_dict['data'], reverse=False) 
#     with open('./data/timestamps.json', 'w') as outfile:
#         json.dump(timestamp_dict, outfile)
        
        
# def find_gaps(start_stamp, end_stamp):
#     interval_dict = {}
#     with open('./data/timestamps.json') as json_file:
#         data = json.load(json_file)
#         timestamps = data['data']
#         prev = 0
#         intervals = []
#         interval = []
#         for curr in timestamps:
#             if curr < start_stamp or curr > end_stamp: continue
#             if prev == 0: 
#                 prev = curr
#                 continue
#             if curr - prev < 5: 
#                 interval.append(curr)
#                 prev = curr
#             else:
#                 intervals.append([interval[0], interval[-1]])
#                 interval = []
#                 interval.append(curr)
#                 prev = curr
#         intervals.append([interval[0], prev])
#     print('intervals :', intervals)
#     interval_dict['data'] = intervals
#     with open('./data/intervals.json', 'w') as outfile:
#         json.dump(interval_dict, outfile)
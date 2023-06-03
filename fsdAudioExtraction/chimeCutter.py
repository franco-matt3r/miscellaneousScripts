import json
import uuid
from datetime import datetime
import boto3
import numpy as np
import io
import sys

CANSERVER_EVENT_BUCKET = "matt3r-canserver-event-us-west-2"
AUDIO_BUCKET = "matt3r-audio-recording-us-west-2"
RESULT_BUCKET = "matt3r-audio-analysis-results-us-west-2"
S3_CLIENT = boto3.client("s3")
DAY_SECOND = 86400
CHIME_LENGTH_SECOND = 5
SAMPLING_RATE = 11025

def get_audio_file_prefixes(organization_id, k3y_id):
    try:
        print("Getting audio files starting timestamp and prefix ...")
        prefix = organization_id + "/" + k3y_id + "/"
        paginator = S3_CLIENT.get_paginator("list_objects_v2")
        pagination_config = {"Bucket": AUDIO_BUCKET, "Prefix": prefix}
        prefix_dict = {}
        for page in paginator.paginate(**pagination_config):
            if 'Contents' in page:
                for obj in page["Contents"]:
                    if "npy" in obj["Key"]:
                        final_result["assessedAudioFilesNum"] += 1
                        final_result["assessedAudioFilesName"].append(obj["Key"])
                        date = obj["Key"].split("_")[1]
                        time = obj["Key"].split("_")[-1].split(".")[0]
                        unix_timestamp = datetime.timestamp(datetime.strptime(f"{date} {time}+0000", "%Y-%m-%d %H-%M-%S%z"))
                        prefix_dict[unix_timestamp] = {}
                        prefix_dict[unix_timestamp]['prefix'] = obj["Key"]
                        prefix_dict[unix_timestamp]['end'] = None
        if len(prefix_dict) == 0: raise Exception("No audio file found for", organization_id, k3y_id)
        sorted_keys = sorted(prefix_dict.keys())
        audio_filename_dict = {key:prefix_dict[key] for key in sorted_keys}
        return audio_filename_dict
    except Exception as e:
        print("get_audio_file_prefixes() failed with error: ", str(e))
        raise e

def get_audio_files_ending_timestamp(audio_filename_dict):
    try:
        print("Getting all audio files end ...")
        for key in audio_filename_dict.keys():
            response = S3_CLIENT.get_object(Bucket=AUDIO_BUCKET, Key=audio_filename_dict[key]['prefix'])
            npy_file = response["Body"].read()
            npy_arr = np.load(io.BytesIO(npy_file))
            length_second = npy_arr.shape[0] / SAMPLING_RATE
            audio_filename_dict[key]['end'] = key + length_second
    except Exception as e:
        print("get_audio_files_ending_timestamp() failed with error: ", str(e))
        raise e

def get_canserver_event_prefixes(min_timestamp, max_timestamp, organization_id, k3y_id):
    try:
        print("Getting CANserver prefixes ...")
        day_start_timestamp = min_timestamp - (min_timestamp % DAY_SECOND)
        day_end_timestamp = max_timestamp - (max_timestamp % DAY_SECOND) + DAY_SECOND
        day_num_float = (day_end_timestamp - day_start_timestamp) / DAY_SECOND
        day_num_rounded_up = int(-((-day_num_float) // 1))
        timestamp_day_list = [day_start_timestamp + (i * DAY_SECOND) for i in range(day_num_rounded_up)]
        prefixes = []
        for timestamp in timestamp_day_list:
            year, month, day = (datetime.utcfromtimestamp(timestamp).strftime("%Y %m %d").split())
            prefix = f"{organization_id}/{k3y_id}/{year}-{month}-{day}.json"
            prefixes.append(prefix)
        return prefixes
    except Exception as e:
        print("get_canserver_event_prefixes() failed with error: ", str(e))
        raise e

def get_canserver_event_fsd_states(canserver_event_prefixes):
    try:
        print("Getting CANServer-event autopilot states ...")
        engagements = []
        disengagements = []
        for prefix in canserver_event_prefixes:
            try:
                raw_data = S3_CLIENT.get_object(Bucket=CANSERVER_EVENT_BUCKET, Key=prefix)
                json_data = json.loads(raw_data["Body"].read().decode("utf-8"))
                engagements.extend([i["timestamp"] for i in json_data["dmd"]["autopilot_state"] if "_6" in i["canbus_state_change"]])
                disengagements.extend([i["timestamp"]for i in json_data["dmd"]["autopilot_state"]if "6_" in i["canbus_state_change"]])
                final_result["assessedCANserverFilesNum"] += 1
                final_result["assessedCANServerFilesName"].append(prefix)
            except Exception as e:
                print(CANSERVER_EVENT_BUCKET,prefix,"skipped because it failed in function get_canserver_event_fsd_states()")
                continue
        return engagements, disengagements
    except Exception as e:
        print("get_canserver_event_fsd_states failed with error: ", str(e))
        raise e

def create_audio_snippets(engagements, disengagements, audio_filename_dict):
    try:
        print("Finding audio snippets ...")
        audio_start_timestamps = list(audio_filename_dict.keys())
        for engagement in engagements:
            for i in range(len(audio_start_timestamps)):
                if audio_start_timestamps[i] <= engagement <= audio_filename_dict[audio_start_timestamps[i]]['end']:
                    if i > 0 and 0.1 > (audio_start_timestamps[i] - audio_filename_dict[audio_start_timestamps[i-1]]['end']):
                        prev = audio_filename_dict[audio_start_timestamps[i-1]]['prefix']
                    else:
                        prev = None
                    curr = audio_filename_dict[audio_start_timestamps[i]]['prefix']
                    if i < len(audio_start_timestamps)-1 and 0.1 > (audio_start_timestamps[i+1] - audio_filename_dict[audio_start_timestamps[i]]['end']):
                        next = audio_filename_dict[audio_start_timestamps[i+1]]['prefix']
                    else:
                        next = None
                    cut_audio_snippet(prev, curr, next, audio_start_timestamps[i], engagement, "engagement")

        for disengagement in disengagements:
            for i in range(len(audio_start_timestamps)):
                if audio_start_timestamps[i] <= disengagement <= audio_filename_dict[audio_start_timestamps[i]]['end']:
                    if i > 0 and 1 > (audio_start_timestamps[i] - audio_filename_dict[audio_start_timestamps[i-1]]['end']):
                        prev = audio_filename_dict[audio_start_timestamps[i-1]]['prefix']
                    else:
                        prev = None
                    curr = audio_filename_dict[audio_start_timestamps[i]]['prefix']
                    if i < len(audio_start_timestamps)-1 and 1 > (audio_start_timestamps[i+1] - audio_filename_dict[audio_start_timestamps[i]]['end']):
                        next = audio_filename_dict[audio_start_timestamps[i+1]]['prefix']
                    else:
                        next = None
                    cut_audio_snippet(prev, curr, next, audio_start_timestamps[i], disengagement, "disengagement")
    except Exception as e:
        print("create_audio_snippets() failed with error: ", str(e))
        raise e

def cut_audio_snippet(prev_prefix, curr_prefix, next_prefix, audio_start_timestamp, event_timestamp, event_type):
    try:
        print("Cutting", event_type, "audio snippet at", event_timestamp, "...")
        response1 = S3_CLIENT.get_object(Bucket=AUDIO_BUCKET, Key=curr_prefix)
        npy_file1 = response1["Body"].read()
        npy_arr1 = np.load(io.BytesIO(npy_file1))
        offset = event_timestamp - audio_start_timestamp
        start = int((offset - 2.5) * SAMPLING_RATE)
        end = int((offset + 2.5) * SAMPLING_RATE)

        # case 1: base case
        if start >= 0 and end <= npy_arr1.shape[0]:
            processed_file = npy_arr1[start:end]

        # case 2: snippet includes previous file and it's available
        elif start < 0 and prev_prefix:
            response0 = S3_CLIENT.get_object(Bucket=AUDIO_BUCKET, Key=prev_prefix)
            npy_file0 = response0["Body"].read()
            npy_arr0 = np.load(io.BytesIO(npy_file0))
            processed_file = np.concatenate((npy_arr0[start:], npy_arr1[:end]), axis=0)

        # case 3: snippet includes next file and it's available
        elif end > npy_arr1.shape[0] and next_prefix:
            response2 = S3_CLIENT.get_object(Bucket=AUDIO_BUCKET, Key=next_prefix)
            npy_file2 = response2["Body"].read()
            npy_arr2 = np.load(io.BytesIO(npy_file2))
            processed_file = np.concatenate((npy_arr1[start:], npy_arr2[:end-npy_arr1.shape[0]]), axis=0)

        # case 4: snippet includes previous file and it's NOT available
        elif start < 0 and not prev_prefix:
            processed_file = npy_arr1[:end]

        # case 5: snippet includes next file and it's NOT available
        elif end > npy_arr1.shape[0] and not next_prefix:
            processed_file = npy_arr1[start:]

        byte_stream = io.BytesIO()
        np.save(byte_stream, processed_file)
        byte_stream.seek(0)

        S3_CLIENT.put_object(
            Body=byte_stream,
            Bucket=RESULT_BUCKET,
            Key=f"{organization_id}/{k3y_id}/{event_type}/{event_type}_{event_timestamp}.npy",
        )
        final_result[f"{event_type}Files"][event_timestamp] = curr_prefix
    except Exception as e:
        print("cut_audio_snippet() failed with error: ", str(e))
        raise e
    
def save_result(request_id, organization_id, k3y_id):
    try:
        print("Saving result metadata ...")
        data_string = json.dumps(final_result)
        key_id = f"{organization_id}/{k3y_id}/summary/{request_id}.json"
        S3_CLIENT.put_object(Body=data_string, Bucket=RESULT_BUCKET, Key=key_id)
        print("Result Bucket:", RESULT_BUCKET, "Result Key:", key_id)
    except Exception as e:
        print("save_result() failed with error: ", str(e))
        raise e

if __name__ == "__main__":
    # =================================================
    # OPTION 1: USE INPUTS FROM COMMAND LINE
    if len(sys.argv) == 3:
        _, organization_id, k3y_id = sys.argv
    else: 
        print("Please provide organization_id and k3y_id as command line arguments. i.e. python3 chimeCutter.py org123 key123")
        exit()
    # OPTION 2: UPDATE VALUES MANUALLY HERE
    # organization_id = "org123"
    # k3y_id = "key123"
    # ================================================
    
    request_id = str(uuid.uuid4())
    final_result = {
        "request_id": request_id,
        "organization_id": organization_id,
        "k3y_id": k3y_id,
        "resultLocation": RESULT_BUCKET,
        "engagementFiles": {},
        "disengagementFiles": {},
        "assessedAudioFilesNum": 0,
        "assessedAudioFilesName": [],
        "assessedCANserverFilesNum": 0,
        "assessedCANServerFilesName":[],
    }
    audio_filename_dict = get_audio_file_prefixes(organization_id, k3y_id)
    get_audio_files_ending_timestamp(audio_filename_dict)
    min_timestamp = list(audio_filename_dict.keys())[0]
    max_timestamp = audio_filename_dict[list(audio_filename_dict.keys())[-1]]['end']
    canserver_event_prefixes = get_canserver_event_prefixes(min_timestamp, max_timestamp, organization_id, k3y_id)
    engagements, disengagements = get_canserver_event_fsd_states(canserver_event_prefixes)
    create_audio_snippets(engagements, disengagements, audio_filename_dict)
    save_result(request_id, organization_id, k3y_id)
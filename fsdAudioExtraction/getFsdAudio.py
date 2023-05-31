import json
import uuid
from datetime import datetime
import boto3
import wave

CANSERVER_EVENT_BUCKET = "matt3r-canserver-event-us-west-2"
AUDIO_BUCKET = "matt3r-audio-recording-us-west-2"
RESULT_BUCKET = "matt3r-audio-analysis-results-us-west-2"
S3_CLIENT = boto3.client('s3')
DAY_SECOND = 86400

def get_audio_file_prefixes():
    try:
        print("Getting all audio files from", AUDIO_BUCKET, "...")
        prefix = organization_id + "/" + k3y_id + "/"
        paginator = S3_CLIENT.get_paginator('list_objects_v2')
        pagination_config = {'Bucket': AUDIO_BUCKET,'Prefix': prefix}
        
        results = {}
        timestamps = []
        for page in paginator.paginate(**pagination_config):
            for obj in page['Contents']:
                final_result["assessedAudioFilesNum"] += 1
                final_result["assessedAudioFilesName"].append(obj['Key'])
                date = obj['Key'].split("_")[1]
                time = "-".join(obj['Key'].split("_")[-3:]).split(".")[0]
                curr_timestamp = datetime.timestamp(datetime.strptime(f"{date} {time}+0000","%Y-%m-%d %H-%M-%S%z"))
                timestamps.append(curr_timestamp)
                results[curr_timestamp] = obj['Key']
        if len(timestamps) == 0: raise Exception("No audio file found for", organization_id, k3y_id)
        return min(timestamps), max(timestamps), results
    except Exception as e:
        print("get_audio_file_prefixes() failed with error: ", str(e))
        raise e
    
def get_canserver_event_prefixes():
    try:
        print("Getting prefixes for", CANSERVER_EVENT_BUCKET, "...")
        day_start_timestamp = min_timestamp - (min_timestamp % DAY_SECOND)
        day_end_timestamp = max_timestamp - (max_timestamp % DAY_SECOND) + DAY_SECOND
        day_num_float = (day_end_timestamp - day_start_timestamp) / DAY_SECOND
        day_num_rounded_up = int(-((-day_num_float) // 1))
        timestamp_day_list = [day_start_timestamp + (i * DAY_SECOND) for i in range(day_num_rounded_up)]
        prefixes = []
        for timestamp in timestamp_day_list:
            year, month, day = datetime.utcfromtimestamp(timestamp).strftime("%Y %m %d").split()
            prefix = organization_id + "/" + k3y_id + "/" + year + "-" + month + "-" + day + ".json"
            prefixes.append(prefix)
        return prefixes
    except Exception as e:
        print("get_canserver_event_prefixes() failed with error: ", str(e))
        raise e
    
def get_canserver_event_fsd_states():
    try:
        print("Getting CANServer-event autopilot states ...")
        engagements = []
        disengagements = []
        
        for prefix in canserver_event_prefixes:
            try:
                raw_data = S3_CLIENT.get_object(Bucket=CANSERVER_EVENT_BUCKET, Key=prefix)
                json_data = json.loads(raw_data["Body"].read().decode('utf-8'))
                engagements.extend([i["timestamp"] for i in json_data['dmd']['autopilot_state'] if "_6" in i["canbus_state_change"]])
                disengagements.extend([i["timestamp"] for i in json_data['dmd']['autopilot_state'] if "6_" in i["canbus_state_change"]])
                final_result["assessedCANserverFilesNum"] += 1
                final_result["assessedCANserverFilesName"].append(prefix)
            except Exception as e:
                continue
        return engagements, disengagements
    except Exception as e:
        print("get_canserver_event_fsd_states failed with error: ", str(e))
        raise e
    
def get_audio_snippet(key, offset):
    try:  
        print("Creating audio snippets ...")
        response = S3_CLIENT.get_object(Bucket=AUDIO_BUCKET, Key=f"{organization_id}/{k3y_id}/{key}")
        BytesIO_obj = response['Body'].read()
        bin_file = io.BytesIO(BytesIO_obj)
        bin_file.seek(0)
        start = offset - 2.5
        end = offset + 2.5
            
        # Check if file contains the offset
        with wave.open(bin_file, "rb") as infile:
            frames = infile.getnframes()
            framerate = infile.getframerate()
            duration = frames / float(framerate)
            infile.close()
            if end > duration: return
        
        # extract the audio snippet
            nchannels = infile.getnchannels()
            sampwidth = infile.getsampwidth()
            infile.setpos(int(start * framerate))
            snippet = infile.readframes(int((end - start) * framerate))

        # write the audio snippet
        with wave.open('my_out_file.wav', 'w') as outfile:
            outfile.setnchannels(nchannels)
            outfile.setsampwidth(sampwidth)
            outfile.setframerate(framerate)
            outfile.setnframes(int(len(snippet) / sampwidth))
            outfile.writeframes(snippet)
            outfile.close()
            processed_file = io.BytesIO(outfile)
            
        S3_CLIENT.put_object(Body=processed_file, Bucket=RESULT_BUCKET, Key=f"audio/{organization_id}/{k3y_id}/{key}")
            
    except Exception as e:
        print("get_audio_snippet() failed with error: ", str(e))
        raise e
    
def create_audio_snippets():
    try:
        print("Finding audio snippets ...")
        maxAudioLength = 90
        for audioStartStamp in audio_file_names.keys():
            for engagementStamp in engagements:
                if  0 <= (engagementStamp - audioStartStamp) < maxAudioLength:
                    get_audio_snippet(audio_file_names[audioStartStamp], engagementStamp-audioStartStamp)
                    final_result["toFSDFiles"].append(audio_file_names[audioStartStamp])
            for disengagementStamp in disengagements:
                if  0 <= (disengagementStamp - audioStartStamp) < maxAudioLength:
                    get_audio_snippet(audio_file_names[audioStartStamp], disengagementStamp-audioStartStamp)
                    final_result["fromFSDFiles"].append(audio_file_names[audioStartStamp])
    except Exception as e:
        print("create_audio_snippets() failed with error: ", str(e))
        raise e
    
def save_result():
    try:
        print("Saving result metadata ...")
        data_string = json.dumps(final_result)
        key_id = "audio" + "/" + organization_id + "/" + k3y_id + "/" + request_id + ".json"
        S3_CLIENT.put_object(Body=data_string, Bucket=RESULT_BUCKET, Key=key_id)
        print("Result Bucket:", RESULT_BUCKET, "Result Key:", key_id)
    except Exception as e:
        print("save_result() failed with error: ", str(e))
        raise e

organization_id = "hamid"
k3y_id = "k3yusb-b9fc16de"
request_id = str(uuid.uuid4())
final_result = {"request_id": request_id,
                "organization_id":organization_id,
                "k3y_id":k3y_id,
                "resultLocation":RESULT_BUCKET,
                "fromFSDFiles":[],
                "toFSDFiles":[],
                "assessedAudioFilesNum":0,
                "assessedAudioFilesName":[],
                "assessedCANserverFilesNum":0,
                "assessedCANServerFilesName":[]
                }

if __name__ == "__main__":
    # min_timestamp, max_timestamp, audio_file_names = get_audio_file_prefixes()
    min_timestamp=1672799469.0
    max_timestamp=1672929589.0
    audio_file_names={1672799469.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_31_09.wav', 1672799493.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_31_33.wav', 1672799518.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_31_58.wav', 1672799542.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_32_22.wav', 1672799566.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_32_46.wav', 1672799591.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_33_11.wav', 1672799615.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_33_35.wav', 1672799640.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_34_00.wav', 1672799664.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_34_24.wav', 1672799690.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_34_50.wav', 1672799715.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_35_15.wav', 1672799739.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_35_39.wav', 1672799763.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_36_03.wav', 1672799787.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_36_27.wav', 1672799811.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_36_51.wav', 1672799836.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_37_16.wav', 1672799860.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_37_40.wav', 1672799886.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_38_06.wav', 1672799910.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_38_30.wav', 1672799934.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_38_54.wav', 1672799958.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_39_18.wav', 1672799983.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_39_43.wav', 1672800007.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_40_07.wav', 1672800031.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_40_31.wav', 1672800070.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_41_10.wav', 1672800094.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_41_34.wav', 1672800118.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_41_58.wav', 1672800144.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_42_24.wav', 1672800168.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_42_48.wav', 1672800192.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_43_12.wav', 1672800216.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_43_36.wav', 1672800240.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_44_00.wav', 1672800265.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_44_25.wav', 1672800289.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_44_49.wav', 1672800356.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_45_56.wav', 1672800380.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_46_20.wav', 1672800404.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_46_44.wav', 1672800428.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_47_08.wav', 1672800452.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_47_32.wav', 1672800476.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_47_56.wav', 1672800501.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_48_21.wav', 1672800525.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_48_45.wav', 1672800550.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_49_10.wav', 1672800575.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_49_35.wav', 1672800600.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_50_00.wav', 1672800626.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_50_26.wav', 1672800650.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_50_50.wav', 1672800675.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_51_15.wav', 1672800699.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_51_39.wav', 1672800724.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_52_04.wav', 1672800748.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_52_28.wav', 1672800772.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-04_02_52_52.wav', 1672928614.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_23_34.wav', 1672928638.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_23_58.wav', 1672928662.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_24_22.wav', 1672928686.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_24_46.wav', 1672928711.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_25_11.wav', 1672928735.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_25_35.wav', 1672928762.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_26_02.wav', 1672928786.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_26_26.wav', 1672928810.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_26_50.wav', 1672928841.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_27_21.wav', 1672928866.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_27_46.wav', 1672928894.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_28_14.wav', 1672928918.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_28_38.wav', 1672928945.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_29_05.wav', 1672928969.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_29_29.wav', 1672928993.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_29_53.wav', 1672929021.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_30_21.wav', 1672929045.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_30_45.wav', 1672929072.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_31_12.wav', 1672929096.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_31_36.wav', 1672929121.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_32_01.wav', 1672929152.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_32_32.wav', 1672929177.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_32_57.wav', 1672929205.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_33_25.wav', 1672929230.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_33_50.wav', 1672929258.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_34_18.wav', 1672929282.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_34_42.wav', 1672929308.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_35_08.wav', 1672929333.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_35_33.wav', 1672929357.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_35_57.wav', 1672929383.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_36_23.wav', 1672929408.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_36_48.wav', 1672929433.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_37_13.wav', 1672929459.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_37_39.wav', 1672929484.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_38_04.wav', 1672929508.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_38_28.wav', 1672929532.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_38_52.wav', 1672929565.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_39_25.wav', 1672929589.0: 'hamid/k3yusb-b9fc16de/audio-record_2023-01-05_14_39_49.wav'}
    canserver_event_prefixes = get_canserver_event_prefixes()
    engagements, disengagements = get_canserver_event_fsd_states()
    create_audio_snippets()
    save_result()
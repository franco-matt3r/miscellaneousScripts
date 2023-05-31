import json
import uuid
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

CANSERVER_PARSED_BUCKET = "matt3r-canserver-us-west-2"
CANSERVER_EVENT_BUCKET = "matt3r-canserver-event-us-west-2"
IMU_BUCKET = "matt3r-imu-us-west-2"
RESULT_BUCKET = "matt3r-imu-analysis-results-us-west-2"
S3_CLIENT = boto3.client('s3')

def get_prefixes(bucket, organization_id, k3y_id, date):
    try:
        print("Getting prefixes for", bucket, "...")
        dateSplits = date.split("-")
        year = dateSplits[0]
        month = dateSplits[1]
        day = dateSplits[2]
        prefixes = []
        for hour in range(24):
            try:
                if hour < 10: hour = "0" + str(hour)
                if bucket == IMU_BUCKET: prefix = organization_id + "/" + k3y_id + "/" + "accel" + "/" + year + "-" + month + "-" + day + "_" + str(hour) + "-00-00.parquet"
                if bucket == CANSERVER_PARSED_BUCKET: prefix = organization_id + "/" + k3y_id + "/" + year + "-" + month + "-" + day + "_" + str(hour) + "_00_00.parquet"
                S3_CLIENT.head_object(Bucket=bucket, Key=prefix)
                prefixes.append(prefix)
            except Exception as e:
                continue  
        return prefixes
    except Exception as e:
        print("get_prefixes() failed with error: ", str(e))
        raise e
        
def remove_stationary_state_from_driving_state(raw_stationary_state, raw_driving_state):
    clean_driving_state = []
    for driving_interval in raw_driving_state:
        mustSave = True
        for stationary_interval in raw_stationary_state:
            # case 0 stationary and driving are the same
            if (stationary_interval['start'] == driving_interval['start']) and (driving_interval['end'] == stationary_interval['end']):
                mustSave = False
                break
            # case 1 stationary overlaps beginning of driving
            if (stationary_interval['start'] <= driving_interval['start']) and (driving_interval['start'] <= stationary_interval['end']):
                driving_interval = {"start": stationary_interval['end'], "end": driving_interval['end']}
            # case 2 stationary overlaps end of driving
            if (stationary_interval['start'] <= driving_interval['end']) and (driving_interval['end'] <= stationary_interval['end']):
                driving_interval = {"start": driving_interval['start'], "end":stationary_interval['start']}
            # case 3 stationary is within driving
            if (driving_interval['start'] <= stationary_interval['start']) and (stationary_interval['end'] <= driving_interval['end']):
                clean_driving_state.extend(remove_stationary_state_from_driving_state(raw_stationary_state, [{"start":driving_interval['start'], "end":stationary_interval['start']}]))
                clean_driving_state.extend(remove_stationary_state_from_driving_state(raw_stationary_state, [{"start":stationary_interval['end'], "end":driving_interval['end']}]))
                mustSave = False
                break
        if mustSave: clean_driving_state.append([driving_interval['start'], driving_interval['end']])
    return clean_driving_state

def get_canserver_event_states(organization_id, k3y_id, date):
    try:
        print("Getting CANServer-event states ...")
        prefix = organization_id + "/" + k3y_id + "/" + date + ".json"
        raw_data = S3_CLIENT.get_object(Bucket=CANSERVER_EVENT_BUCKET, Key=prefix)
        json_data = json.loads(raw_data["Body"].read().decode('utf-8'))
        result = {}
        result["parked_state"] = [[i["timestamp"][0], i['timestamp'][1]] for i in json_data['imu_telematics']['parked_state']]
        result["stationary_state"] = [[i['start'], i['end']] for i in json_data['imu_telematics']['stationary_state']]
        raw_stationary_state = json_data['imu_telematics']['stationary_state']
        raw_driving_state = json_data['imu_telematics']['driving_state']
        result["driving_state"] = remove_stationary_state_from_driving_state(raw_stationary_state, raw_driving_state)
        return result
    except Exception as e:
        print("get_canserver_event_states() failed with error: ", str(e))
        raise e
    
def get_state_table(column, day_table, intervals):
    try:
        timestamp = day_table[column]
        filters = [pc.and_(pc.greater_equal(timestamp, start), pc.less_equal(timestamp, end)) for start, end in intervals]
        combined_filter = filters[0]
        for filter in filters[1:]: combined_filter = pc.or_(combined_filter, filter)
        return day_table.filter(combined_filter)
    except Exception as e:
        print("get_state_table() failed with error: ", str(e))
        raise e

def query(columns, bucket, prefixes, states):
    try:
        # get data for entire day
        print("Getting acceleration data for", bucket,"...")
        bucket_path = "s3://" + bucket + "/"
        raw_table = pq.ParquetDataset(prefixes, filesystem=bucket_path).read(columns)
        new_schema = pa.schema([pa.field(column, pa.float64()) for column in columns])
        day_table = raw_table.cast(target_schema=new_schema)
        
        # get data for each state
        query_Result = {}
        query_Result["NumberFiles"] = len(prefixes)
        query_Result["availableFiles"] = prefixes
        for state in states.keys():
            # get data
            state_table = get_state_table(columns[0], day_table, states[state])
            readingCount = pc.count(state_table.column(columns[1]))
            
            mean_bf_acc = pc.mean(state_table.column(columns[1]))
            mean_lr_acc = pc.mean(state_table.column(columns[2]))
            mean_vert_acc = pc.mean(state_table.column(columns[3]))
            
            min_bf_acc = pc.min(state_table.column(columns[1]))
            min_lr_acc = pc.min(state_table.column(columns[2]))
            min_vert_acc = pc.min(state_table.column(columns[3]))
            
            max_bf_acc = pc.max(state_table.column(columns[1]))
            max_lr_acc = pc.max(state_table.column(columns[2]))
            max_vert_acc = pc.max(state_table.column(columns[3]))
            
            squared_bf_acc = pc.power_checked(state_table.column(columns[1]), 2)
            squared_lr_acc = pc.power_checked(state_table.column(columns[2]), 2)
            squared_vert_acc = pc.power_checked(state_table.column(columns[3]), 2)
            sum_acc_1 = pc.add_checked(squared_bf_acc, squared_lr_acc)
            sum_acc_2 = pc.add_checked(sum_acc_1, squared_vert_acc)
            norm_acc = pc.sqrt_checked(sum_acc_2)
            
            mean_norm_acc = pc.mean(norm_acc)
            min_norm_acc = pc.min(norm_acc)
            max_norm_acc = pc.max(norm_acc)
            
            query_Result[state] = {
                "readingCount":readingCount.as_py(),
                "mean_bf_acc":mean_bf_acc.as_py(),
                "mean_lr_acc":mean_lr_acc.as_py(),
                "mean_vert_acc":mean_vert_acc.as_py(),
                "min_bf_acc":min_bf_acc.as_py(),
                "min_lr_acc":min_lr_acc.as_py(),
                "min_vert_acc":min_vert_acc.as_py(),
                "max_bf_acc":max_bf_acc.as_py(),
                "max_lr_acc":max_lr_acc.as_py(),
                "max_vert_acc":max_vert_acc.as_py(),
                "mean_norm_acc":mean_norm_acc.as_py(),
                "min_norm_acc":min_norm_acc.as_py(),
                "max_norm_acc":max_norm_acc.as_py()
                }
        return query_Result
    except Exception as e:
        print("query() for", bucket, "failed with error: ", str(e))
        raise e

def save_result(organization_id, k3y_id, date, final_result):
    try:
        data_string = json.dumps(final_result)
        key_id = "analysis" + "/" + organization_id + "/" + k3y_id + "/" + date + ".json"
        S3_CLIENT.put_object(Body=data_string, Bucket=RESULT_BUCKET, Key=key_id)
        print("Result Bucket:", RESULT_BUCKET, "Result Key:", key_id)
    except Exception as e:
        print("save_result() failed with error: ", str(e))
        raise e
    
if __name__ == "__main__":
    # ===================================
    # CHANGE INPUT VARIABLES HERE
    organization_id = "hamid"
    k3y_id = "k3y-9ed5b50e"
    date = "2023-05-05"
    # ===================================
    
    imu_columns = ["timestamp(epoch in sec)", "bf_acc(m/s^2)", "lr_acc(m/s^2)", "vert_acc(m/s^2)"]
    canserver_columns = ["timestamp", "bf_acc", "lr_acc", "vert_acc"]
    final_result = {"request_id": str(uuid.uuid4()),
                    "organization_id":organization_id,
                    "k3y_id":k3y_id,
                    "date":date,
                    "imu": {
                        "numberFiles": 0,
                        "availableFiles": [],
                        "parked_state":{},
                        "stationary_state":{},
                        "driving_state":{}
                        },
                    "canserver": {
                        "numberFiles": 0,
                        "availableFiles": [],
                        "parked_state":{},
                        "stationary_state":{},
                        "driving_state":{}
                        }
                    }
    imu_prefixes = get_prefixes(IMU_BUCKET, organization_id, k3y_id, date)
    if len(imu_prefixes) == 0: raise Exception("No IMU file found for", organization_id, k3y_id, date)
    canserver_prefixes = get_prefixes(CANSERVER_PARSED_BUCKET, organization_id, k3y_id, date)
    if len(canserver_prefixes) == 0: raise Exception("No CANServer_parsed file found for", organization_id, k3y_id, date)
    states = get_canserver_event_states(organization_id, k3y_id, date)
    final_result["imu"] = query(imu_columns, IMU_BUCKET, imu_prefixes, states)
    final_result["canserver"] = query(canserver_columns, CANSERVER_PARSED_BUCKET, canserver_prefixes, states)
    save_result(organization_id, k3y_id, date, final_result)

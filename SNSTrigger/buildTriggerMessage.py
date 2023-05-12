import json
import sys

_, BUCKET_NAME, OUTPUT_FILE_NAME, OUTPUT_TRIGGER_MESSAGE = sys.argv
ARN = f"arn:aws:s3:::{BUCKET_NAME}"

def build():
    filenames_path = f"./{OUTPUT_FILE_NAME}.json"
    with open(filenames_path) as f:
        filenames_dict = json.load(f)
    filenames = filenames_dict["Contents"]
    trigger = {}
    trigger["Records"] = []
    for file in filenames:
        if ".log" not in file["Key"]: continue
        upload_event = {
            "eventVersion": "2.0",
            "eventSource": "aws:s3",
            "awsRegion": "us-west-2",
            "eventTime": "2023-01-01T01:01:01.001Z",
            "eventName": "ObjectCreated:Put",
            "userIdentity": {"principalId": "AWS:AIDA6AT7VXIYOVAHHWRTW"},
            "requestParameters": {"sourceIPAddress": "24.84.28.147"},
            "responseElements": {
                "x-amz-request-id": "9DRMXAR0K56GDG7V",
                "x-amz-id-2": "PidrYuLSro7AvRveWqgsh5zndYbRL1OYeoJy22zyn8qdvmYc8/NHUCgQ6D7ZmQSfx1f5Re+YXR2XnnaXHSCS4qjBbpLXG+J5"
            },
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": "parse-test",
                "bucket": {
                    "name": BUCKET_NAME,
                    "ownerIdentity": {"principalId": file["Owner"]["ID"]},
                    "arn": ARN
                },
                "object": {
                    "key": file["Key"],
                    "size": file["Size"],
                    "eTag": file["ETag"],
                    "sequencer": "00645D22AB95C0AC41"
                }
            }
        }
        trigger["Records"].append(upload_event)

    with open(f"{OUTPUT_TRIGGER_MESSAGE}.json", "w") as outfile:
        json.dump(trigger, outfile)


if __name__ == "__main__":
    build()


            # "eventName": "ObjectCreated:CompleteMultipartUpload",

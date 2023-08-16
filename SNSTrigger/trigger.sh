# BUCKET_NAME="matt3r-canserver-raw-us-west-2"
# PREFIX="franco/key456/"
# PREFIX="cheung/k3yusb-e731c27b/"
#BUCKET_NAME_STAGE_1="haoming-canserver-raw-test"
#BUCKET_NAME_STAGE_2="haoming-canserver-test"
#BUCKET_NAME_STAGE_3="haoming-canserver-event-test"
BUCKET_NAME_STAGE_1="matt3r-canserver-raw-us-west-2"
BUCKET_NAME_STAGE_2="matt3r-canserver-us-west-2"
BUCKET_NAME_STAGE_3="matt3r-canserver-event-us-west-2"
PREFIX="franco-test/key123/"
# PREFIX="franco-test/key789/"
#PREFIX="cheung/k3yusb-e731c27b/"
# PREFIX="mohsen/k3y-7724670b/"
# PREFIX="hamid/k3y-78f8031e/"
# PREFIX="hamid/k3y-9ed5b50e/"
# PREFIX="test-suite/key123/"
# PREFIX="manual-test/key123/"
FILE_NAME_LIST_DIRECTORY="./data/filenameList"
MESSAGE_BATCH_DIRECTORY="./data/messageBatch"
COUNTER=0
# FOR DATE SPECIFIC TRIGGER ONE DAY AT A TIME, USE THE ALTERNATIVE AWS LIST-OBJECTS COMMAND AT LINE 35

rm -r ${FILE_NAME_LIST_DIRECTORY}/
rm -r ${MESSAGE_BATCH_DIRECTORY}/
mkdir -p ${FILE_NAME_LIST_DIRECTORY}
mkdir -p ${MESSAGE_BATCH_DIRECTORY}

echo After 30 seconds, the following files from s3://${BUCKET_NAME_STAGE_2}/${PREFIX} and s3://${BUCKET_NAME_STAGE_3}/${PREFIX} will be deleted:
aws s3 rm --dryrun --recursive s3://${BUCKET_NAME_STAGE_2}/${PREFIX}
aws s3 rm --dryrun --recursive s3://${BUCKET_NAME_STAGE_3}/${PREFIX}
sleep 30s
echo "********** Last chance to abord - Deleting in 15 seconds **********"
sleep 15s
aws s3 rm --recursive s3://${BUCKET_NAME_STAGE_2}/${PREFIX}
aws s3 rm --recursive s3://${BUCKET_NAME_STAGE_3}/${PREFIX}
# aws s3 cp ./debug.json s3://${BUCKET_NAME_STAGE_3}/${PREFIX}

aws --output json s3api list-objects --bucket ${BUCKET_NAME_STAGE_1} --prefix ${PREFIX} > ${FILE_NAME_LIST_DIRECTORY}"/filenames.json"
# aws --output json s3api list-objects --bucket ${BUCKET_NAME_STAGE_1} --prefix ${PREFIX} --query "Contents[?contains(Key, '2023-04-15')]" > ${FILE_NAME_LIST_DIRECTORY}"/filenames.json"
python3 ./deleteRelatedPostgresTableEntries.py ${PREFIX}
python3 ./buildTriggerMessage.py ${BUCKET_NAME_STAGE_1} ${MESSAGE_BATCH_DIRECTORY} ${FILE_NAME_LIST_DIRECTORY}"/filenames.json"
while [ -f ${MESSAGE_BATCH_DIRECTORY}/${COUNTER}.json ];
do
    echo published batch number $COUNTER: ${MESSAGE_BATCH_DIRECTORY}/${COUNTER}.json
    aws sns publish --topic-arn arn:aws:sns:us-west-2:963414178352:matt3r-dpl-sns --message file://./${MESSAGE_BATCH_DIRECTORY}/${COUNTER}.json
    COUNTER=$((COUNTER+1))
    sleep 100s
done

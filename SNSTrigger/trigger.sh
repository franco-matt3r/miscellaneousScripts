BUCKET_NAME="haoming-canserver-raw-test"
# BUCKET_NAME="matt3r-canserver-raw-us-west-2"
# PREFIX="franco/key456/"
# PREFIX="cheung/k3yusb-e731c27b/"
PREFIX="franco-test/key789/"
FILE_NAME_LIST_DIRECTORY="listObject"
MESSAGE_BATCH_DIRECTORY="franco-test"
FILE_NAME_LIST_JSON=${FILE_NAME_LIST_DIRECTORY}"/filenames.json"
COUNTER=0

if [ ! -f ${FILE_NAME_LIST_DIRECTORY} ]; then
    echo creating directory ${FILE_NAME_LIST_DIRECTORY}
    mkdir ${FILE_NAME_LIST_DIRECTORY}
fi
if [ ! -f ${MESSAGE_BATCH_DIRECTORY} ]; then
    echo creating directory ${MESSAGE_BATCH_DIRECTORY}
    mkdir ${MESSAGE_BATCH_DIRECTORY}
fi


aws --output json s3api list-objects --bucket ${BUCKET_NAME} --prefix ${PREFIX} > ${FILE_NAME_LIST_JSON}
python3 ./deleteRelatedPostgresTableEntries.py ${PREFIX}
python3 ./buildTriggerMessage.py ${BUCKET_NAME} ${MESSAGE_BATCH_DIRECTORY} ${FILE_NAME_LIST_JSON}
while [ -f ${MESSAGE_BATCH_DIRECTORY}/${COUNTER}.json ];
do
    echo published batch number $COUNTER: ${MESSAGE_BATCH_DIRECTORY}/${COUNTER}.json
    aws sns publish --topic-arn arn:aws:sns:us-west-2:963414178352:matt3r-dpl-sns --message file://./${MESSAGE_BATCH_DIRECTORY}/${COUNTER}.json
    COUNTER=$((COUNTER+1))
    sleep 90s
done

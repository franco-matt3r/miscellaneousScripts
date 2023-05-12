BUCKET_NAME="haoming-canserver-raw-test"
PREFIX="franco/key456/"
OUTPUT_FILE_NAME="filenames"
OUTPUT_TRIGGER_MESSAGE="triggerMessage"
aws --output json s3api list-objects --bucket ${BUCKET_NAME} --prefix ${PREFIX} > ${OUTPUT_FILE_NAME}.json
python3 ./deleteRelatedPostgresTableEntries.py ${PREFIX}
python3 ./buildTriggerMessage.py ${BUCKET_NAME} ${OUTPUT_FILE_NAME} ${OUTPUT_TRIGGER_MESSAGE}
jq -c '.MetaRecords[]' ${OUTPUT_TRIGGER_MESSAGE}.json | while read item; do
    echo ${item}
    # aws sns publish --topic-arn arn:aws:sns:us-west-2:963414178352:matt3r-dpl-sns --message <(echo ${item})
    # aws sns publish --topic-arn arn:aws:sns:us-west-2:963414178352:matt3r-dpl-sns --message <<< ${item}
    aws sns publish --topic-arn arn:aws:sns:us-west-2:963414178352:matt3r-dpl-sns --message <(printf '%s\n' "${item}")
    # echo ${item} | aws sns publish --topic-arn arn:aws:sns:us-west-2:963414178352:matt3r-dpl-sns --message
done
# aws sns publish --topic-arn arn:aws:sns:us-west-2:963414178352:matt3r-dpl-sns --message file://${OUTPUT_TRIGGER_MESSAGE}.json
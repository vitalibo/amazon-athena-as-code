#!/usr/bin/env bash
set -e

if [ $# -ne 2 ]; then
  echo "Usage: $0 [deployment-bucket] [athena-result-output-location]"
  echo ''
  echo 'Options:'
  echo '  deployment-bucket                 S3 bucket name where contains compiled lambdas'
  echo '  athena-result-output-location     The location in S3 where query results are stored'
  exit 1
fi

S3_BUCKET=$1
OUTPUT_LOCATION=$2

echo 'Create/Update stack initialized'
SOURCE_CODE="../athena-resource-provisioner/target/athena-resource-provisioner-1.0-SNAPSHOT.jar"
MD5=`md5sum ${SOURCE_CODE} | awk '{ print $1 }'`
aws s3 cp ${SOURCE_CODE} "s3://${S3_BUCKET}/amazon-athena-as-code/${MD5}"

aws cloudformation deploy --template-file 'stack.json' --stack-name "amazon-athena-as-code-infrastructure" \
  --parameter-overrides DeploymentBucket=${S3_BUCKET} AthenaResultOutputLocation=${OUTPUT_LOCATION} \
    LambdaCodeSource=${MD5}
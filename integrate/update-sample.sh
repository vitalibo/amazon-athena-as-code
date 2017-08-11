#!/usr/bin/env bash

if [ $# -ne 1 ]; then
  echo "Usage: $0 [source-bucket]"
  echo ''
  echo 'Options:'
  echo '  source-bucket        S3 bucket name where data will be stored'
  exit 1
fi

S3_BUCKET=$1

aws cloudformation deploy --template-file 'sample.json' --stack-name "amazon-athena-as-code-sample" \
  --parameter-overrides Bucket=${S3_BUCKET}
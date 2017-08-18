#!/usr/bin/env bash
set -e

if [ $# -ne 2 ]; then
  echo "Usage: $0 [user] [bucket]"
  echo ''
  echo 'Options:'
  echo '  user       User name'
  echo '  bucket     S3 bucket name where data will be stored'
  exit 1
fi

USER=$1
S3_BUCKET=$2

aws cloudformation deploy --template-file 'sample.json' --stack-name "${USER}-sample" \
  --parameter-overrides Bucket=${S3_BUCKET}

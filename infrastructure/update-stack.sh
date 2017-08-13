#!/usr/bin/env bash
set -e

if [ $# -ne 1 ]; then
  echo "Usage: $0 [conf]"
  echo ''
  echo 'Options:'
  echo '  conf    The JSON file which contains environment configuration'
  exit 1
fi

CONFIGURATION=$1

function _get() {
  jq -r ".[] | select(.ParameterKey == \"$1\") | .ParameterValue" ${CONFIGURATION}
}

USER=$(_get 'UserName')
S3_BUCKET=$(_get 'DeploymentBucket')

echo 'Create/Update stack initialized'
MODULE='athena-resource-provisioner'
MD5_SUM=$(md5sum "../${MODULE}/target/${MODULE}-1.0-SNAPSHOT.jar" | awk '{ print $1 }')
aws s3 cp "../${MODULE}/target/${MODULE}-1.0-SNAPSHOT.jar" "s3://${S3_BUCKET}/${USER}/${MD5_SUM}"

aws cloudformation deploy --template-file 'stack.json' --stack-name "${USER}-infrastructure" \
  --parameter-overrides UserName=${USER} DeploymentBucket=${S3_BUCKET} \
    AthenaResultOutputLocation=$(_get 'AthenaResultOutputLocation') LambdaCodeSource=${MD5_SUM}
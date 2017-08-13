#!/usr/bin/env bash
set -e

if [ $# -ne 1 ]; then
  echo "Usage: $0 [conf]"
  echo ''
  echo 'Options:'
  echo '  conf    The JSON file which contains environment configuration'
  exit 1
fi

CONFIGURATION_JSON=$1

function _get() {
  jq -r ".[] | select(.ParameterKey == \"$1\") | .ParameterValue" ${CONFIGURATION_JSON}
}

USER=$(_get 'UserName')

aws cloudformation deploy --template-file 'user.json' --stack-name "$USER-user" --no-execute-changeset \
  --capabilities 'CAPABILITY_NAMED_IAM' --parameter-overrides UserName=${USER} \
    DeploymentBucket=$(_get 'DeploymentBucket') AthenaResultOutputLocation=$(_get 'AthenaResultOutputLocation')

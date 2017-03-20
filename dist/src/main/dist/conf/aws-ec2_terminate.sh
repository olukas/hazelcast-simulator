#!/bin/bash

# exit on failure
set -e

# comma seperated list of private ip's of the agents to destroy
private_ips=$1

export AWS_ACCESS_KEY_ID=$CLOUD_IDENTITY
export AWS_SECRET_ACCESS_KEY=$CLOUD_CREDENTIAL

instance_ids=$(aws ec2 describe-instances \
                --region $REGION \
                --filter Name=private-ip-address,Values=$private_ips \
                --output text \
                --query 'Reservations[*].Instances[*].InstanceId')

echo "[INFO]Terminating [$instance_ids]"

if [[ -z "${instance_ids// }" ]]; then
    echo "[INFO]Nothing to terminate"
    exit 0
fi

aws ec2 terminate-instances \
    --region $REGION \
    --instance-ids $instance_ids
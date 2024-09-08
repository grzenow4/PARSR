#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <user> <password>"
    exit 1
fi

USER=$1
PASSWORD=$2

EXTRA_VARS="ansible_user=$USER ansible_password=$PASSWORD ansible_ssh_extra_args='-o StrictHostKeyChecking=no'"

ansible-playbook --extra-vars "$EXTRA_VARS" -i hosts deployment/clean.yaml

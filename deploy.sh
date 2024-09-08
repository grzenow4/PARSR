#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <user> <password>"
    exit 1
fi

USER=$1
PASSWORD=$2
EXTRA_VARS="ansible_user=$USER ansible_password=$PASSWORD ansible_ssh_extra_args='-o StrictHostKeyChecking=no'"

# Install essential packages
sudo apt -y install ansible sshpass maven docker docker-compose openjdk-17-jdk

# Upgrade ansible package
sudo add-apt-repository -y ppa:ansible/ansible
sudo apt update && sudo apt upgrade ansible

# Add other VMs to the known_hosts
for i in $(seq -w 01 10); do
    sshpass -p $PASSWORD ssh "$USER"@"$USER"vm1"$i".rtb-lab.pl \
    -o StrictHostKeyChecking=no -C "/bin/true"
done

# Build docker images
echo "Building load balancer Docker image..."
(cd load_balancer && sudo docker build -t my-proxy .)

echo "Building app Docker image..."
sudo mvn -Pdocker package

# Start aerospike
ansible-playbook --extra-vars "$EXTRA_VARS" -i hosts aerospike/aerospike.yaml

# Start kafka
ansible-playbook --extra-vars "$EXTRA_VARS" -i hosts kafka/kafka.yaml

# Start the app
ansible-playbook --extra-vars "$EXTRA_VARS" -i hosts deployment/deployment.yaml

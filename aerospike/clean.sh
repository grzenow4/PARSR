#!/bin/bash

sudo systemctl stop aerospike
sudo rm -rf /opt/aerospike/data/parsr1.dat
sudo rm -rf /opt/aerospike/data/parsr2.dat
sudo rm -rf /opt/aerospike/data/parsr3.dat
sudo rm -rf /opt/aerospike/data/parsr4.dat
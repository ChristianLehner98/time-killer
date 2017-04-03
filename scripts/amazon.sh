#!/bin/sh

# OPENS A SSH CONNECTION TO THE JOB MANAGER

# public ip of job manager node
IP=52.59.43.231

ssh -L "8081:${IP}:8081" "ubuntu@$IP"

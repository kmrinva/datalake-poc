#!/usr/bin/python

import boto3
import os
import sys
import watchtower
import logging
import datetime

client = boto3.client('emr')
s3client = boto3.client('s3')
job_id = os.urandom(16).encode('hex')
cluster_id = ''
print(job_id)

today = datetime.datetime.now()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('HMDA_EMR_process-'+str((today-datetime.datetime(1970,1,1)).total_seconds()))
logger.addHandler(watchtower.CloudWatchLogHandler())

hive_args = "hive -f s3://datalake-poc-data/feeds/hmda/hmda_lar/scripts/hmda_lar_table_create_partition_add.sql"

hive_args_list = hive_args.split()

def add_emr_partition_job(cluster_id):
    
    try:
        response = client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[{
                'Name': 'HMDA Table and Partition Creation',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': hive_args_list}}])
    except Exception:
        print(sys.exc_info()[1])
        msg = 'partition step add failed'
        logger.info(msg)
        print(msg)

def find_cluster():
    clusters = client.list_clusters()
    clusters = [c["Id"] for c in clusters["Clusters"]
                if c["Status"]["State"] in ["RUNNING","WAITING"]]
    if clusters:
        foundClusterId = clusters[0]
        print(foundClusterId)
    if not clusters:
        msg ='No Active clusters found'
        logger.info(msg)
        print(msg)

    return foundClusterId
    
if __name__ == "__main__":
    msg = 'Find an active cluster'
    logger.info(msg)
    print(msg)
    cluster_id = find_cluster()
    logger.info('EMR Partition Cluster: ' +cluster_id)
    logger.info('EMR Partition script: ' + hive_args)

    msg = 'adding emr partition step'
    logger.info(msg)
    print(msg)

    try:
        add_emr_partition_job(cluster_id)
    except Exception:
        print(sys.exc_info()[0])
        msg = 'partition job failed'
        logger.info(msg)
        print(msg)

#!/usr/bin/python

#This script will download data from the CFPB HMDA API listed below.  This is a single site reference for CFPB HMDA.  
#The script will check or create (if necessary) the destination directory.  Each directory will have the year embedded in the name
#Requirements : EC2 instance (to park files) with about 40 Gbs
#             : AWS CLI configure - to allow EC2 to S3 synching
#             : Github watchtower to allow logging to Cloudwatch (sudo pip install watchtower)
#             : manually create EC2 base path or add code to create in script

#all my imports
import requests
import os
import sys
import errno
import subprocess
import timeit
import watchtower
import logging
import datetime


#define/set variables
urlBase = 'https://api.consumerfinance.gov:443/data/hmda/slice/hmda_lar.csv?$where=as_of_year+%3D+'
urlEnd = '&$limit=100000'

hmdaYearlist = ['2009']
#hmdaYearlist = ['2009','2010','2011','2012','2013','2014','2015','2016' ]

#hmdaYearlist = ['2016','2017' ]

pathBase = '/home/ec2-user/data/hmda'
pathFeed = '/feeds/hmda/hmda_lar/raw/csv/'
pathPartition = 'as_of_year='
destFile = 'hmda_lar.csv'

targetBucket = 'datalake-poc-data'

today = datetime.datetime.now()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('HMDA_load_process-'+str((today-datetime.datetime(1970,1,1)).total_seconds()))
logger.addHandler(watchtower.CloudWatchLogHandler())

#define functions

#directory checking and creation function
def make_sure_path_exists(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise
        
#get record count in file passed - note that this is not re-reading the file
def file_len(fname):
    p = subprocess.Popen(['wc', '-l', fname], stdout=subprocess.PIPE, 
                                              stderr=subprocess.PIPE)
    result, err = p.communicate()
    if p.returncode != 0:
        raise IOError(err)
    return int(result.strip().split()[0])

#Throwing the run configs to the log process
logger.info('Landing path: '+pathBase+pathFeed+pathPartition)
#logger.info('Extract years: '+str(hmdaYearList))
logger.info('Destination file: '+destFile)
logger.info('Target S3 Bucket: '+targetBucket)

def extract_hmda():
# measure process time
    start_time = timeit.default_timer()

    for varYear in hmdaYearlist:
        msg = 'Downloading HMDA data for '+ varYear
        print(msg)
        logger.info(msg)
        url = urlBase+varYear+urlEnd
    
        pathEnd = varYear+'/'
        path = pathBase+pathFeed+pathPartition+pathEnd
        fileName = path+destFile
        make_sure_path_exists(path)
        msg = 'Making directory '+path
        print(msg)
        logger.info(msg)

        #get the HMDA data for the varYear variable
        req = requests.get(url, stream=True)
        file = open(fileName, 'wb')

        msg = 'Downloading data '+fileName
        print(msg)
        logger.info(msg)
        
        if req.ok:
            for chunk in req.iter_content(100000):
                if not chunk:
                    break
                file.write(chunk)
        
        file.close()

        fileCount = file_len(fileName)
        msg = 'Completed download of file ' + fileName + ' with '+  str(fileCount) + ' records '
        print(msg)
        logger.info(msg)

        msg = 'Synch to S3 bucket'
        print(msg)
        logger.info(msg)

        #synch these files to S3 bucket  -add variables to the stmt below
        syncReturn_code = subprocess.call("aws s3 sync "+pathBase +" s3://"+targetBucket, shell=True)

        msg = 'Synch process completed with status ' + str(syncReturn_code)
        print(msg)
        logger.info(msg)

        if syncReturn_code == 0:
            msg = 'Deleting EC2 staging data'
            print(msg)
            logger.info(msg)
            delReturn_code = subprocess.call("rm -r "+pathBase, shell=True)
            if delReturn_code == 0:
                msg = 'Delete of EC2 files successful'
                print(msg)
                logger.info(msg)
            else:
                msg = 'Delete of EC2 files failed'
                print(msg)
                logger.info(msg)
        else:
            msg = 'S3 synch failed - stop here'
            print(msg)
            logger.info(msg)
# of the of year partition loop

    #end clock, calc time and print
    elapsed = timeit.default_timer() - start_time
    msg = str(elapsed) + ' seconds processed - elapsed time'
    print(msg)
    logger.info(msg)

if __name__ == "__main__":
    try:
        extract_hmda()
    except Exception as e:
        logging.exception("message")

    

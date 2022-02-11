#!/usr/bin/env python3

# Copyright (C) 2019 Christoph Gorgulla
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# This file is part of VirtualFlow.
#
# VirtualFlow is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# VirtualFlow is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with VirtualFlow.  If not, see <https://www.gnu.org/licenses/>.

# ---------------------------------------------------------------------------
#
# Description: Submit jobs to AWS Batch
#
# Revision history:
# 2021-06-29  Original version
#
# ---------------------------------------------------------------------------


import os
import json
import boto3
import botocore
import re
import argparse
import sys
import time
from botocore.config import Config
from pathlib import Path
import shutil

def parse_config(filename):
    with open(filename, "r") as read_file:
        config = json.load(read_file)

    return config


def run_bash(config, current_workunit, jobline):
    # Yet to be implemented
    pass
    

def submit_aws_batch(config, client, current_workunit, jobline):

    jobline_str = str(jobline)

    # how many jobs are there that we need to submit?
    subjobs_count = len(current_workunit['subjobs'])

    # AWS Batch doesn't allow an array job of only 1 -- so if it's one
    # we will launch 2, but the second will exit quickly since it has
    # no work

    if(subjobs_count == 1):
        subjobs_count = 2

    # Path to the data files
    object_store_input_path = f"{config['object_store_job_prefix_full']}"

    # Which queue to submit to
    batch_queue_number = ((jobline - 1) % int(config['aws_batch_number_of_queues'])) + 1


    try:
        response = client.submit_job(
            jobName=f'vfvs-{config["job_letter"]}-{jobline}',
            timeout={
                'attemptDurationSeconds': 10800
            },
            jobQueue=f"{config['aws_batch_prefix']}-queue{batch_queue_number}",
            arrayProperties={
                'size': subjobs_count
            },
            jobDefinition=f"{config['aws_batch_jobdef']}",
            containerOverrides={
                'resourceRequirements': [
                    {
                        'type': 'VCPU',
                        'value': '8',
                    },
                    {
                        'type': 'MEMORY',
                        'value': '15000',
                    },
                ],
                'environment': [
                
                    {
                        'name': 'VFVS_RUN_MODE',
                        'value': "awsbatch"
                    },
                    {
                        'name': 'VFVS_JOB_STORAGE_MODE',
                        'value': "s3"
                    },
                    {
                        'name': 'VFVS_VCPUS',
                        'value': config['threads_to_use']
                    },
                    {
                        'name': 'VFVS_TMP_PATH',
                        'value': config['tempdir_default']
                    },
                    {
                        'name': 'VFVS_RUN_SEQUENTIAL',
                        'value': "0"
                    },
                    {
                        'name': 'VFVS_WORKUNIT',
                        'value': jobline_str
                    },
                    {
                        'name': 'VFVS_CONFIG_JOB_OBJECT',
                        'value': current_workunit['s3_download_path']
                    },
                    {
                        'name': 'VFVS_CONFIG_JOB_BUCKET',
                        'value': config['object_store_job_bucket']
                    },
                    
                ]
            }
        )

        current_workunit['status'] = {
            'vf_job_status': 'SUBMITTED',
            'job_arn': response['jobArn'],
            'job_name': response['jobName'],
            'job_id': response['jobId']
        }

    except botocore.exceptions.ClientError as error: 
        print("invalid")
        raise error

    # Slow ourselves down a bit
    time.sleep(0.1)

def process(config, start, stop):

    aws_config = Config(
        region_name=config['aws_region']
    )
    client = boto3.client('batch', config=aws_config)

    status = {}

    # load the status file that is keeping track of the data
    with open("../workflow/status.json", "r") as read_file:
        status = json.load(read_file)
        collections = status['collections']
        workunits = status['workunits']

    for jobline in range(start, stop + 1):

        jobline_str = str(jobline)
        if jobline_str not in workunits:
            print(f"Jobline {jobline_str} was not found")
        else:
            current_workunit = workunits[jobline_str]

            print(f"Submitting jobline {jobline_str}... ")

            # Now see if any of them have been submitted before
            if 'status' in current_workunit:
                print("jobs were already submitted for this....")
            else:
                submit_type="awsbatch"
                if(submit_type == "awsbatch"):
                    submit_aws_batch(config, client, current_workunit, jobline)
                elif(submit_type == "bash"):
                    run_bash(config, current_workunit, jobline)
                else:
                    print(f"Unknown submit type {submit_type}")
                

    # Output all of the information about the workunits into JSON so we can easily grab this data in the future
    
    print("Writing the json status file out")

    with open("../workflow/status.json.tmp", "w") as json_out:
        json.dump(status, json_out, indent=4)

    Path("../workflow/status.json.tmp").rename("../workflow/status.json")

    print("Making copy for status.submission as backup")
    shutil.copyfile("../workflow/status.json", "../workflow/status.submission.json")

    print("Done")

def main():

    if len(sys.argv) != 3:
        print('You must supply exactly two arguments -- the start jobline and the end jobline.')
        print('Joblines start from 1')
        sys.exit()

    config = parse_config("../workflow/config.json")
    process(config, int(sys.argv[1]), int(sys.argv[2]))


if __name__ == '__main__':
    main()

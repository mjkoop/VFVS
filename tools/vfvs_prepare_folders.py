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
# Description: Parse the config file and generate the workflow directory
#
# Revision history:
# 2021-09-28  Initial version of VFVS ported to Python
# 2022-01-18  Additional hash addressing mode
# ---------------------------------------------------------------------------


import json
import re
import os
import argparse
from pathlib import Path
import shutil


def parse_config(filename):

    config = {}

    with open(filename, "r") as read_file:
        for index, line in enumerate(read_file):
            #match = re.search(r'^(?P<parameter>.*?)\s*=\s*(?P<parameter_value>.*?)\s*$', line)
            match = re.search(r'^(?P<parameter>[a-zA-Z0-9_]+)\s*=\s*(?P<parameter_value>.*?)\s*$', line)
            if(match):
                matches = match.groupdict()
                config[matches['parameter']] = matches['parameter_value']

    config['object_store_job_prefix_full'] = f"{config['object_store_job_prefix']}/{config['job_letter']}"


    config['docking_scenario_names'] = config['docking_scenario_names'].split(
        ":")
    config['docking_scenario_programs'] = config['docking_scenario_programs'].split(
        ":")
    config['docking_scenario_replicas'] = config['docking_scenario_replicas'].split(
        ":")
    config['docking_scenario_inputfolders'] = config['docking_scenario_inputfolders'].split(
        ":")

    return config


def empty_value(config, value):
    if(value not in config):
        return 1
    elif(config[value] == ""):
        return 1
    else:
        return 0


def check_parameters(config):
    error = 0


    if(empty_value(config, 'threads_to_use')):
        print("* 'threads_to_use' must be set in all.ctrl")
        error = 1

    if(empty_value(config, 'object_store_data_collection_addressing_mode')):
        print("* 'object_store_data_collection_addressing_mode' must be set in all.ctrl")
        error = 1
    else:
        if(config['object_store_data_collection_addressing_mode'] == "hash"):
            if(empty_value(config, 'object_store_data_collection_identifier')):
                print("* 'object_store_data_collection_identifier' must be set if object_store_data_collection_addressing_mode is 'hash'")
                error = 1
    
    if(empty_value(config, 'object_store_job_addressing_mode')):
        print("* 'object_store_job_addressing_mode' must be set in all.ctrl")
        error = 1

    if(empty_value(config, 'job_storage_mode') or (config['job_storage_mode'] != "s3" and config['job_storage_mode'] != "sharedfs")):
        print("* 'job_storage_mode' must be set to 's3' or 'sharedfs'")
        error = 1
    else:
        if(config['job_storage_mode'] == "s3"):
            if(empty_value(config, 'object_store_job_bucket')):
                print("* 'object_store_job_bucket' must be set if job_storage_mode is 's3'")
                error = 1
            if(empty_value(config, 'object_store_job_prefix')):
                print("* 'object_store_job_prefix' must be set if job_storage_mode is 's3'")
                error = 1
            else:
                config['object_store_job_prefix'].rstrip("/")
            if(empty_value(config, 'object_store_data_bucket')):
                print("* 'object_store_data_bucket' must be set if job_storage_mode is 's3'")
                error = 1
            if(empty_value(config, 'object_store_data_collection_prefix')):
                print("* 'object_store_data_collection_prefix' must be set if job_storage_mode is 's3'")
                error = 1
            else:
                config['object_store_data_collection_prefix'].rstrip("/")


    if(empty_value(config, 'batchsystem')):
        print("* 'batchsystem' must be set in all.ctrl")
        error = 1
    else:
        if(config['batchsystem'] == "awsbatch"):
            if(empty_value(config, 'aws_batch_prefix')):
                print("* 'aws_batch_prefix' must be set if batchsystem is 'awsbatch'")
                error = 1
            if(empty_value(config, 'aws_batch_number_of_queues')):
                print("* 'aws_batch_number_of_queues' must be set if batchsystem is 'awsbatch'")
                error = 1
            if(empty_value(config, 'aws_batch_array_job_size')):
                print("* 'aws_batch_array_job_size' must be set if batchsystem is 'awsbatch'")
                error = 1
            if(empty_value(config, 'aws_ecr_repository_name')):
                print("* 'aws_ecr_repository_name' must be set if batchsystem is 'awsbatch'")
                error = 1
            if(empty_value(config, 'aws_region')):
                print("* 'aws_ecr_repository_name' must be set if batchsystem is 'awsbatch'")
                error = 1

            if(empty_value(config, 'tempdir_default') or config['tempdir_default'] != "/dev/shm"):
                print("* RECOMMENDED that 'tempdir_default' be '/dev/shm' if awsbatch is used")
                error = 1


    if(empty_value(config, 'ligand_library_format')):
        print("* 'ligand_library_format' must be set in all.ctrl")
        error = 1


    if(empty_value(config, 'program_timeout')):
        print("* 'program_timeout' must be set in all.ctrl (seconds to wait for completion)")
        error = 1


    if(empty_value(config, 'docking_scenario_basefolder')):
        print("* 'docking_scenario_basefolder' must be set in all.ctrl")
        error = 1
    else:
        # Verify that the docking scenario basefolder contains the docking scenario files
        if(not os.path.isdir(config['docking_scenario_basefolder'])):
            print("* 'docking_scenario_basefolder' does not appear to be a directory")
            error = 1
        else:
            for scenario_dir in config['docking_scenario_inputfolders']:
                if(not os.path.isdir(os.path.join(config['docking_scenario_basefolder'],scenario_dir))):
                    print(f"* docking_scenario_inputfolders '{scenario_dir}' does not appear to be a directory in 'docking_scenario_basefolder'")
                    error = 1



    return error



def main():

    config = parse_config("templates/all.ctrl")


    # Check some of the parameters we care about
    error = check_parameters(config)

    # Make sure we have all of the arguments set...

    parser = argparse.ArgumentParser()
       
    parser.add_argument('--overwrite', action='store_true', 
        help="Deletes existing workflow and all associated data")
    parser.add_argument('--skip_errors', action='store_true', 
        help="Ignore validation errors and continue")

    args = parser.parse_args()

    workflow_dir = Path("/".join(["..", "workflow"]))
    workflow_dir.mkdir(parents=True, exist_ok=True)
    workflow_config = workflow_dir / "config.json"

    if workflow_config.is_file() and not args.overwrite:
        print(
'''
Workflow already has a config.json. If you are sure that you want to delete
the existing data, then re-run vfvs_prepare_folders.py --overwrite
'''
        )
        exit(1)

    if(error and not args.skip_errors):
        print(
'''
Workflow has validation errors. If you are sure it is correct, 
then re-run vfvs_prepare_folders.py --skip_errors (Not recommended!)
'''
        )
        exit(1)
    # Delete anything that is currently there...
    shutil.rmtree(workflow_dir)
    workflow_dir.mkdir(parents=True, exist_ok=True)

    # Update a few fields so they do not need to be computed later
    if(config['job_storage_mode'] == "sharedfs"):
        workunits_path = workflow_dir / "workunits"
        workunits_path.mkdir(parents=True, exist_ok=True)

        #
        config['sharedfs_workunit_path'] = workunits_path.resolve().as_posix()
        config['sharedfs_workflow_path'] = workflow_dir.resolve().as_posix()
        config['sharedfs_collection_path'] = Path(config['collection_folder']).resolve().as_posix()
     

    with open(workflow_config, "w") as json_out:
        json.dump(config, json_out, indent=4)


    collection_values = {}
    with open('templates/todo.all', "r") as read_file:
        for line in read_file:
            collection_key, ligand_count = line.strip().split(maxsplit=1)
            if(re.search(r'^\d+$', ligand_count)):
                collection_values[collection_key] = int(ligand_count)
            else:
                print(f"{collection_key} had non positive int count of '{ligand_count}'")
        with open('../workflow/todo.all', "w") as write_file:
            for collection_key in sorted(collection_values, key=collection_values.get, reverse=True):
                write_file.write(f"{collection_key} {collection_values[collection_key]}\n")



if __name__ == '__main__':
    main()
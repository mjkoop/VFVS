#!/usr/bin/env python3

# Copyright (C) 2019 Christoph Gorgulla
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# This file is part of VirtualFlow.
#
# VirtualFlow is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
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
# Description: Main runner for the individual workunits/job lines
#
# Revision history:
# 2021-06-29  Original version
# 2021-08-02  Added additional handling for case where there is only 
#             a single subjob in a job
# 2022-04-20  Adding support for output into parquet format
#
# ---------------------------------------------------------------------------


import tempfile
import tarfile
import gzip
import os
import json
import re
import boto3
import multiprocessing
import subprocess
import botocore
import logging
import time
import shutil
import hashlib
import pandas as pd
from pathlib import Path
from botocore.config import Config
from statistics import mean
from multiprocessing import Process
from multiprocessing import Queue
from queue import Empty
import math


# Download
# -
# Get the file and move it to a local location
#

def downloader(download_queue, unpack_queue, tmp_dir):


    botoconfig = Config(
       retries = {
          'max_attempts': 25,
          'mode': 'standard'
       }
    )

    s3 = boto3.client('s3', config=botoconfig)


    while True:
        try:
            item = download_queue.get(timeout=20.5)
        except Empty:
            #print('Consumer: gave up waiting...', flush=True)
            continue

        if item is None:
            break


        item['temp_dir'] = tempfile.mkdtemp(prefix=tmp_dir)
        item['local_path'] = f"{item['temp_dir']}/tmp.{item['ext']}"


        # Move the data either from S3 or a shared filesystem

        if('s3_download_path' in item['collection']):

            remote_path = item['collection']['s3_download_path']
            job_bucket = item['collection']['s3_bucket']

            try:
                with open(item['local_path'], 'wb') as f:
                    s3.download_fileobj(job_bucket, remote_path, f)
            except botocore.exceptions.ClientError as error:
                    logging.error(f"Failed to download from S3 {job_bucket}/{remote_path} to {item['local_path']}, ({error})")
                    continue

        elif('sharedfs_path' in item['collection']):

            shutil.copyfile(Path(item['collection']['sharedfs_path']), item['local_path'])


        # Move it to the next step if there's space

        while unpack_queue.qsize() > 35:
            time.sleep(0.2)

        unpack_queue.put(item)




def untar(unpack_queue, collection_queue, sensor_screen_mode, sensor_screen_count):
    print('Unpacker: Running', flush=True)

    while True:
        try:
            item = unpack_queue.get(timeout=20.5)
        except Empty:
            #print('unpacker: gave up waiting...', flush=True)
            continue

        # check for stop
        if item is None:
            break

        item['ligands'] = unpack_item(item, sensor_screen_mode, sensor_screen_count)

        # Next step is to process this
        while collection_queue.qsize() > 35:
            time.sleep(0.2)

        collection_queue.put(item)



def unpack_item(item, sensor_screen_mode, sensor_screen_count):

    ligands = {}

    os.chdir(item['temp_dir'])
    try:
        tar = tarfile.open(item['local_path'])
        for member in tar.getmembers():
            if(not member.isdir()):
                _, ligand = member.name.split("/", 1)

                if(ligand == ".listing"):
                    continue

                ligand_name = ligand.split(".")[0]

                ligands[ligand_name] = {
                    'path':  os.path.join(item['temp_dir'], item['collection']['collection_number'], ligand),
                    'collection_key': item['collection_key']
                }

        tar.extractall()
        tar.close()
    except Exception as err:
        logging.error(
            f"ERR: Cannot open {item['local_path']} type: {str(type(err))}, err: {str(err)}")
        return None

    if(int(sensor_screen_mode) == 1):

        sensor_screen_ligands = {}

        # We should read the sparse file to know which ligands we actually need to keep
        with open(os.path.join(item['temp_dir'], item['collection']['collection_number'], ".listing"), "r") as read_file:
            for index, line in enumerate(read_file):
                line = line.strip()

                screen_collection_key, screen_ligand_name, screen_index = line.split(",")

                if(int(screen_index) >= int(sensor_screen_count)):
                    continue

                sensor_screen_ligands[screen_ligand_name] = ligands[screen_ligand_name]
                sensor_screen_ligands[screen_ligand_name]['collection_key'] = screen_collection_key

        return sensor_screen_ligands

    else:
        return ligands





def collection_process(ctx, collection_queue, docking_queue, summary_queue):

    while True:
        try:
            item = collection_queue.get(timeout=20.5)
        except Empty:
            #print('unpacker: gave up waiting...', flush=True)
            continue

        # check for stop
        if item is None:
            break

        expected_ligands = 0
        completions_per_ligand = 0


        # How many do we run per ligand?
        for scenario_key in ctx['main_config']['docking_scenarios']:
            scenario = ctx['main_config']['docking_scenarios'][scenario_key]
            for replica_index in range(scenario['replicas']):
                completions_per_ligand += 1


        # Process every ligand after making sure it is valid

        for ligand_key in item['ligands']:
            ligand = item['ligands'][ligand_key]

            coords = {}
            skip_ligand = 0

            with open(ligand['path'], "r") as read_file:
                for index, line in enumerate(read_file):
                    match = re.search(r'(?P<letters>\s+(B|Si|Sn)\s+)', line)
                    if(match):
                        matches = match.groupdict()
                        logging.error(
                            f"Found {matches['letters']} in {ligand}. Skipping.")
                        skip_reason = f"failed(ligand_elements:{matches['letters']})"
                        skip_reason_json = f"ligand includes elements: {matches['letters']})"
                        skip_ligand = 1
                        break

                    match = re.search(r'^ATOM', line)
                    if(match):
                        parts = line.split()
                        coord_str = ":".join(parts[5:8])

                        if(coord_str in coords):
                            logging.error(
                                f"Found duplicate coordinates in {ligand}. Skipping.")
                            skip_reason = f"failed(ligand_coordinates)"
                            skip_reason_json = f"duplicate coordinates"
                            skip_ligand = 1
                            break
                        coords[coord_str] = 1

            if(skip_ligand == 0):

                # We can submit this for processing
                smi = get_smi(ctx['main_config']['ligand_library_format'], ligand['path'])
                submit_ligand_for_docking(ctx, docking_queue, ligand_key, ligand['path'], ligand['collection_key'], smi, item['temp_dir'])
                expected_ligands += completions_per_ligand

            else:
                # log this to know we skipped
                # put on the logging queue
                pass

        # Let the summary queue know that it can delete the directory after len(ligands)
        # number of ligands have been processed


        summary_item = {
            'type': "delete",
            'temp_dir': item['temp_dir'],
            'collection_key': item['collection_key'],
            'expected_completions': expected_ligands
        }

        summary_queue.put(summary_item)




def get_smi(ligand_format, ligand_path):

    valid_formats = ['pdbqt', 'mol2']

    if ligand_format in valid_formats:
        with open(ligand_path, "r") as read_file:
            for line in read_file:
                line = line.strip()
                match = re.search(r"SMILES:\s*(?P<smi>.*)$", line)
                if(match):
                    return match.group('smi')
                match = re.search(r"SMILES_current:\s*(?P<smi>.*)$", line)
                if(match):
                    return match.group('smi')

    return "N/A"


def submit_ligand_for_docking(ctx, docking_queue, ligand_name, ligand_path, collection_key, smi, temp_dir):

    for scenario_key in ctx['main_config']['docking_scenarios']:
        scenario = ctx['main_config']['docking_scenarios'][scenario_key]
        for replica_index in range(scenario['replicas']):
            docking_item = {
                'ligand_key': ligand_name,
                'ligand_path': ligand_path,
                'scenario_key': scenario_key,
                'collection_key': collection_key,
                'smi': smi,
                'config_path': scenario['config'],
                'program': scenario['program'],
                'program_long': scenario['program_long'],
                'input_files_dir':  os.path.join(ctx['temp_dir'], "vf_input", "input-files"),
                'timeout': int(ctx['main_config']['program_timeout']),
                'tools_path': ctx['tools_path'],
                'threads_per_docking': int(ctx['main_config']['threads_per_docking']),
                'temp_dir': temp_dir
            }

        docking_queue.put(docking_item)

    while docking_queue.qsize() > 100:
            time.sleep(0.2)


def docking_process(docking_queue, summary_queue):

    while True:
        try:
            item = docking_queue.get(timeout=20.5)
        except Empty:
            #print('unpacker: gave up waiting...', flush=True)
            continue

        # check for stop
        if item is None:
            break

        print(f"processing {item['ligand_key']}")

        item['output_path'] = f"{item['temp_dir']}/{item['ligand_key']}.output"
        item['ligand_path.old'] = item['ligand_path']
        item['ligand_path'] = f"{item['temp_dir']}/{item['ligand_key']}.pdbqt"

        # COMPND -- seems to be invalid?
        with open(item['ligand_path'], "w") as write_file:
            with open(item['ligand_path.old'], "r") as read_file:
                for line in read_file:
                    if(re.search(r"TITLE|SOURCE|KEYWDS|EXPDTA|COMPND|HEADER|AUTHOR", line)):
                        continue
                    write_file.write(line)


        start_time = time.perf_counter()

        # x
        try:
            cmd = program_runstring_array(item)
        except RuntimeError as err:
            logging.error(f"Invalid cmd generation for {item['ligand_key']} (program: '{item['program']}')")
            raise(err)

        try:
            ret = subprocess.run(cmd, capture_output=True,
                         text=True, cwd=item['input_files_dir'], timeout=item['timeout'])
        except subprocess.TimeoutExpired as err:
            logging.error(f"timeout on {item['ligand_key']}")

        if ret.returncode == 0:

            if(item['program'] == "qvina02"
                    or item['program'] == "qvina_w"
                    or item['program'] == "vina"
                    or item['program'] == "vina_carb"
                    or item['program'] == "vina_xb"
                    or item['program'] == "gwovina"
               ):

                match = re.search(
                    r'^\s+1\s+(?P<value>[-0-9.]+)\s+', ret.stdout, flags=re.MULTILINE)
                if(match):
                    matches = match.groupdict()
                    item['score'] = float(matches['value'])
                    item['status'] = "success"
                else:
                    logging.error(
                        f"Could not find score for {item['collection_key']} {item['ligand_key']} {item['scenario_key']}")

            elif(task['program'] == "smina"):
                found = 0
                for line in reversed(ret.stdout.splitlines()):
                    match = re.search(r'^1\s{4}\s*(?P<value>[-0-9.]+)\s*', line)
                    if(match):
                        matches = match.groupdict()
                        item['score'] = float(matches['value'])
                        item['status'] = "success"
                        found = 1
                        break
                if(found == 0):
                    logging.error(
                        f"Could not find score for {item['collection_key']} {item['ligand_key']} {item['scenario_key']}")

            elif(task['program'] == "adfr"):
                logging.error(
                    f"adfr not implemented {item['collection_key']} {item['ligand_key']} {item['scenario_key']}")
            elif(task['program'] == "plants"):
                logging.error(
                    f"plants not implemented {item['collection_key']} {item['ligand_key']} {item['scenario_key']}")

        else:
            logging.error(
                f"Non zero return code for {item['collection_key']} {item['ligand_key']} {item['scenario_key']}")
            logging.error(f"stdout:\n{ret.stdout}\nstderr:{ret.stderr}\n")

        # Place output into files
        #with open(task['log_path'], "w") as output_f:
        #    output_f.write(f"STDOUT:\n{ret.stdout}\n")
        #    output_f.write(f"STDERR:\n{ret.stderr}\n")

        end_time = time.perf_counter()

        item['seconds'] = end_time - start_time
        print(f"processing {item['ligand_key']} - done in {item['seconds']}")

        while summary_queue.qsize() > 200:
            time.sleep(0.2)


        item['type'] = "docking_complete"
        summary_queue.put(item)



def check_for_completion_of_collection_key(collection_completions, collection_key):
    current_completions = collection_completions[collection_key]['current_completions']
    expected_completions = collection_completions[collection_key]['expected_completions']

    if current_completions == expected_completions:
        shutil.rmtree(collection_completions[collection_key]['temp_dir'])
        collection_completions.pop(collection_key, None)



def summary_process(ctx, summary_queue, upload_queue):

    print("starting summary process")

    dockings_processed = 0
    summary_data = {}

    collection_completions = {}

    while True:
        try:
            item = summary_queue.get()
        except Empty:
            #print('unpacker: gave up waiting...', flush=True)
            continue

        # check for stop
        if item is None:
            # clean up anything extra that is around
            if(dockings_processed > 0):
                generate_summary_file(ctx, summary_data, upload_queue, ctx['temp_dir'])

            break


        if(item['type'] == "delete"):
            if item['collection_key'] in collection_completions:
                collection_completions[item['collection_key']]['expected_completions'] = item['expected_completions']
                collection_completions[item['collection_key']]['temp_dir'] = item['temp_dir']
            else:
                collection_completions[item['collection_key']] = {
                    'expected_completions':item['expected_completions'],
                    'current_completions': 0,
                    'temp_dir': item['temp_dir']
                }

            check_for_completion_of_collection_key(collection_completions, item['collection_key'])

        elif(item['type'] == "docking_complete"):
            # Save off the data we need

            summary_key = f"({item['ligand_key']})({item['scenario_key']})"
            log_index = int(dockings_processed / 1000)

            if summary_key not in summary_data:
                summary_data[summary_key] = {
                    'ligand': item['ligand_key'],
                    'smi': item['smi'],
                    'collection_key': item['collection_key'],
                    'scenario': item['scenario_key'],
                    'scores': [ item['score'] ]
                }
            else:
                summary_data[summary_key]['scores'].append(item['score'])


            dockings_processed += 1


            # See if this was the last completion for this collection_key

            if item['collection_key'] in collection_completions:
                collection_completions[item['collection_key']]['current_completions'] += 1
                check_for_completion_of_collection_key(collection_completions, item['collection_key'])
            else:
                collection_completions[item['collection_key']] = {
                    'expected_completions': -1,
                    'current_completions': 1,
                    'temp_dir': ""
                }


        else:
            logging.error(f"received invalid summary completion {item['type']}")
            raise





def generate_summary_file(ctx, summary_data, upload_queue, tmp_dir):

    csv_ordering = ['ligand', 'smi', 'collection_key', 'scenario', 'score_average']
    max_scores = 0

    # Need to run all of the averages
    for summary_key, summary_value in summary_data.items():

        if(len(summary_value['scores']) > max_scores):
            max_scores = len(summary_value['scores'])

        for index, score in enumerate(summary_value['scores']):
            summary_value[f"score_{index}"] = score

        summary_value['score_average'] = mean(summary_value['scores'])
        summary_value.pop('scores', None)


    if 'parquet' in ctx['main_config']['summary_formats']:

        # Now we can generate a parquet file with all of the data
        df = pd.DataFrame.from_dict(summary_data, "index")

        upload_tmp_dir = tempfile.mkdtemp(prefix=tmp_dir)

        uploader_item = {
            'storage_type': ctx['main_config']['job_storage_mode'],
            'remote_path': f"{ctx['main_config']['object_store_job_prefix']}/{ctx['main_config']['job_name']}/parquet/{ctx['workunit_id']}/{ctx['subjob_id']}.parquet",
            's3_bucket': ctx['main_config']['object_store_job_bucket'],
            'local_path': f"{upload_tmp_dir}/summary.parquet",
            'temp_dir': upload_tmp_dir
        }

        df.to_parquet(uploader_item['local_path'], compression='gzip')
        upload_queue.put(uploader_item)

    if 'csv.gz' in ctx['main_config']['summary_formats']:

        for index in range(max_scores):
            csv_ordering.append(f"score_{index}")

        upload_tmp_dir = tempfile.mkdtemp(prefix=tmp_dir)

        with gzip.open(f"{upload_tmp_dir}/summary.txt.gz", "wt") as summmary_fp:

            # print header
            summmary_fp.write(",".join(csv_ordering))
            summmary_fp.write("\n")

            for summary_key, summary_value in summary_data.items():

                ordered_summary_list = list(map(lambda key: str(summary_value[key]), csv_ordering))
                summmary_fp.write(",".join(ordered_summary_list))
                summmary_fp.write("\n")


        uploader_item_csv = {
            'storage_type': ctx['main_config']['job_storage_mode'],
            'remote_path': f"{ctx['main_config']['object_store_job_prefix']}/{ctx['main_config']['job_name']}/csv/{ctx['workunit_id']}/{ctx['subjob_id']}.csv.gz",
            's3_bucket': ctx['main_config']['object_store_job_bucket'],
            'local_path': f"{upload_tmp_dir}/summary.txt.gz",
            'temp_dir': upload_tmp_dir
        }
        upload_queue.put(uploader_item_csv)



def upload_process(ctx, upload_queue):

    print('Uploader: Running', flush=True)

    botoconfig = Config(
       retries = {
          'max_attempts': 25,
          'mode': 'standard'
       }
    )

    s3 = boto3.client('s3', config=botoconfig)

    while True:
        try:
            item = upload_queue.get(timeout=20.5)
        except Empty:
            #print('unpacker: gave up waiting...', flush=True)
            continue

        # check for completion
        if item is None:

            # clean up anything extra that is around
            break

        # Save off the data we need
        # Basically.. if s3 then use boto3

        if(item['storage_type'] == "s3"):
            try:
                print(f"Uploading to {item['remote_path']}")
                response = s3.upload_file(item['local_path'], item['s3_bucket'], item['remote_path'])
            except botocore.exceptions.ClientError as e:
                logging.error(e)
                raise

        else:
            # if sharedfs.. .then just do a copy

            parent_directory = Path(Path(item['remote_path']).parent)
            parent_directory.mkdir(parents=True, exist_ok=True)

            shutil.copyfile(item['local_path'], item['remote_path'])


        # Get rid of the temp directory
        shutil.rmtree(item['temp_dir'])






def process_config(ctx):

    # Create absolute directories based on the other parameters

    ctx['main_config']['collection_working_path'] = os.path.join(
        ctx['temp_dir'], "collections")
    ctx['main_config']['output_working_path'] = os.path.join(
        ctx['temp_dir'], "output-files")

    # Determine full config.txt paths for scenarios
    ctx['main_config']['docking_scenarios'] = {}


    if('summary_formats' not in ctx['main_config']):
        ctx['main_config']['summary_formats'] = {
                'csv.gz': 1
        }

    for index, scenario in enumerate(ctx['main_config']['docking_scenario_names']):

        program_long = ctx['main_config']['docking_scenario_programs'][index]
        program = program_long

        logging.debug(f"Processing scenario '{scenario}' at index '{index}' with {program}")

        # Special handing for smina* and gwovina*
        match = re.search(r'^(?P<program>smina|gwovina)', program_long)
        if(match):
            matches = match.groupdict()
            program = matches['program']
            logging.debug(f"Found {program} in place of {program_long}")
        else:
            logging.debug(f"No special match for '{program_long}'")

        ctx['main_config']['docking_scenarios'][scenario] = {
            'key': scenario,
            'config': os.path.join(ctx['temp_dir'], "vf_input", "input-files",
                                   ctx['main_config']['docking_scenario_inputfolders'][index],
                                   "config.txt"
                                   ),
            'program': program,
            'program_long': program_long,
            'replicas': int(ctx['main_config']['docking_scenario_replicas'][index])
        }


def get_workunit_from_s3(ctx, workunit_id, subjob_id, job_bucket, job_object, download_dir):
    # Download from S3

    download_to_workunit_file = "/".join([download_dir, "vfvs_input.tar.gz"])

    try:
        with open(download_to_workunit_file, 'wb') as f:
            ctx['s3'].download_fileobj(job_bucket, job_object, f)
    except botocore.exceptions.ClientError as error:
        logging.error(
            f"Failed to download from S3 {job_bucket}/{job_object} to {download_to_workunit_file}, ({error})")
        return None

    os.chdir(download_dir)

    # Get the file with the specific workunit we need to work on
    try:
        tar = tarfile.open(download_to_workunit_file)
        tar.extractall()
        file = tar.extractfile(f"vf_input/config.json")

        all_config = json.load(file)
        if(subjob_id in all_config['subjobs']):
            ctx['subjob_config'] = all_config['subjobs'][subjob_id]
        else:
            logging.error(f"There is no subjob ID with ID:{subjob_id}")
            # AWS Batch requires that an array job have at least 2 elements,
            # sometimes we only need 1 though
            if(subjob_id == "1"):
                exit(0)
            else:
                raise RuntimeError(f"There is no subjob ID with ID:{subjob_id}")

        tar.close()
    except Exception as err:
        logging.error(
            f"ERR: Cannot open {download_to_workunit_file}. type: {str(type(err))}, err: {str(err)}")
        return None


    ctx['main_config'] = all_config['config']



def get_workunit_from_sharedfs(ctx, workunit_id, subjob_id, job_tar, download_dir):
    # Download from sharedfs

    download_to_workunit_file = "/".join([download_dir, "vfvs_input.tar.gz"])

    shutil.copyfile(job_tar, download_to_workunit_file)

    os.chdir(download_dir)

    # Get the file with the specific workunit we need to work on
    try:
        tar = tarfile.open(download_to_workunit_file)
        tar.extractall()
        file = tar.extractfile(f"vf_input/config.json")

        all_config = json.load(file)
        if(subjob_id in all_config['subjobs']):
            ctx['subjob_config'] = all_config['subjobs'][subjob_id]
        else:
            logging.error(f"There is no subjob ID with ID:{subjob_id}")
            raise RuntimeError(f"There is no subjob ID with ID:{subjob_id}")

        tar.close()
    except Exception as err:
        logging.error(
            f"ERR: Cannot open {download_to_workunit_file}. type: {str(type(err))}, err: {str(err)}")
        return None


    ctx['main_config'] = all_config['config']


# Generate the run command for a given program

def program_runstring_array(task):

    cpus_per_program = str(task['threads_per_docking'])

    cmd = []

    if(task['program'] == "qvina02"
            or task['program'] == "qvina_w"
            or task['program'] == "vina"
            or task['program'] == "vina_carb"
            or task['program'] == "vina_xb"
            or task['program'] == "gwovina"
       ):
        cmd = [
            f"{task['tools_path']}/{task['program']}",
            '--cpu', cpus_per_program,
            '--config', task['config_path'],
            '--ligand', task['ligand_path'],
            '--out', task['output_path']
        ]
    elif(task['program'] == "smina"):
        cmd = [
            f"{task['tools_path']}/smina",
            '--cpu', cpus_per_program,
            '--config', task['config_path'],
            '--ligand', task['ligand_path'],
            '--out', task['output_path'],
            '--log', f"task['output_path_base'].flexres.pdb",
            '--atom_terms', f"task['output_path_base'].atomterms"
        ]
    else:
        raise RuntimeError(f"Invalid program type of {task['program']}")

    return cmd


def get_workunit_information():

    workunit_id = os.getenv('VFVS_WORKUNIT','') 
    subjob_id = os.getenv('VFVS_WORKUNIT_SUBJOB','')

    if(workunit_id == "" or subjob_id == ""):
        raise RuntimeError(f"Invalid VFVS_WORKUNIT and/or VFVS_WORKUNIT_SUBJOB")

    return workunit_id, subjob_id


def setup_job_storage_mode(ctx):

    ctx['job_storage_mode'] = os.getenv('VFVS_JOB_STORAGE_MODE', 'INVALID')

    if(ctx['job_storage_mode'] == "s3"):

        botoconfig = Config(
           region_name = os.getenv('VFVS_AWS_REGION'),
           retries = {
              'max_attempts': 50,
              'mode': 'standard'
           }
        )

        ctx['job_object'] = os.getenv('VFVS_CONFIG_JOB_OBJECT')
        ctx['job_bucket'] = os.getenv('VFVS_CONFIG_JOB_BUCKET')

        # Get the config information
        ctx['s3'] = boto3.client('s3', config=botoconfig)
    
    elif(ctx['job_storage_mode'] == "sharedfs"):
        ctx['job_tar'] = os.getenv('VFVS_CONFIG_JOB_TGZ')
    else:
        raise RuntimeError(f"Invalid jobstoragemode of {ctx['job_storage_mode']}. VFVS_JOB_STORAGE_MODE must be 's3' or 'sharedfs' ")


def get_subjob_config(ctx, workunit_id, subjob_id):

    if(ctx['job_storage_mode'] == "s3"):
        get_workunit_from_s3(ctx, workunit_id, subjob_id, 
            ctx['job_bucket'], ctx['job_object'], ctx['temp_dir'])
    elif(ctx['job_storage_mode'] == "sharedfs"):
        get_workunit_from_sharedfs(ctx, workunit_id, subjob_id,
            ctx['job_tar'], ctx['temp_dir'])
    else:
        raise RuntimeError(f"Invalid jobstoragemode of {ctx['job_storage_mode']}. VFVS_JOB_STORAGE_MODE must be 's3' or 'sharedfs' ")




def process(ctx):


    ctx['vcpus_to_use'] = int(os.getenv('VFVS_VCPUS', 1))
    ctx['run_sequential'] = int(os.getenv('VFVS_RUN_SEQUENTIAL', 0))

    # What job are we running?

    workunit_id, subjob_id =  get_workunit_information()

    # Setup paths appropriately depending on if we are using S3
    # or a shared FS
    
    setup_job_storage_mode(ctx)

    # This includes all of the configuration information we need
    # After this point ctx['main_config'] has the configuration options
    # and we have specific subjob information in ctx['subjob_config']

    get_subjob_config(ctx, workunit_id, subjob_id)

    # Update some of the path information

    process_config(ctx)


    ctx['workunit_id'] = workunit_id
    ctx['subjob_id'] = subjob_id

    ctx.pop('s3', None)

    print(ctx['temp_dir'])

    ligand_format = ctx['main_config']['ligand_library_format']

    get_smi = 0
    if('print_smi_in_summary' in ctx['main_config'] and int(ctx['main_config']['print_smi_in_summary']) == 1):
        get_smi = True


    # Need to expand out all of the collections in this subjob
    
    subjob = ctx['subjob_config']


    download_queue = Queue()
    unpack_queue = Queue()
    collection_queue = Queue()
    docking_queue = Queue()
    summary_queue = Queue()
    upload_queue = Queue()


    downloader_processes = []
    for i in range(0, math.ceil(ctx['vcpus_to_use'] / 8.0)):
        downloader_processes.append(Process(target=downloader, args=(download_queue, unpack_queue, ctx['temp_dir'])))
        downloader_processes[i].start()

    unpacker_processes = []
    for i in range(0, math.ceil(ctx['vcpus_to_use'] / 8.0)):
        unpacker_processes.append(Process(target=untar, args=(unpack_queue, collection_queue, ctx['main_config']['sensor_screen_mode'], ctx['main_config']['sensor_screen_count'])))
        unpacker_processes[i].start()

    collection_processes = []
    for i in range(0, math.ceil(ctx['vcpus_to_use'] / 8.0)):
        # collection_process(ctx, collection_queue, docking_queue, summary_queue)
        collection_processes.append(Process(target=collection_process, args=(ctx, collection_queue, docking_queue, summary_queue)))
        collection_processes[i].start()

    docking_processes = []
    for i in range(0, ctx['vcpus_to_use']):
        # docking_process(docking_queue, summary_queue)
        docking_processes.append(Process(target=docking_process, args=(docking_queue, summary_queue)))
        docking_processes[i].start()

    # There should never be more than one summary process
    summary_processes = []
    summary_processes.append(Process(target=summary_process, args=(ctx, summary_queue, upload_queue)))
    summary_processes[0].start()

    uploader_processes = []
    for i in range(0, 1):
        # docking_process(docking_queue, summary_queue)
        uploader_processes.append(Process(target=upload_process, args=(ctx, upload_queue)))
        uploader_processes[i].start()

    for collection_key in subjob['collections']:
        collection = subjob['collections'][collection_key]

        download_item = {
            'collection_key': collection_key,
            'collection': subjob['collections'][collection_key],
            'ext': "tar.gz",
        }

        download_queue.put(download_item)

        # Don't overflow the queues
        while download_queue.qsize() > 25:
            time.sleep(0.2)


    flush_queue(download_queue, downloader_processes, "download")
    flush_queue(unpack_queue, unpacker_processes, "unpack")
    flush_queue(collection_queue, collection_processes, "collection")
    flush_queue(docking_queue, docking_processes, "docking")
    flush_queue(summary_queue, summary_processes, "summary")
    flush_queue(upload_queue, uploader_processes, "upload")


def flush_queue(queue, processes, description):
    logging.error(f"Sending {description} flush")
    for process in processes:
        queue.put(None)
    logging.error(f"Join {description}")
    for process in processes:
        process.join()
    logging.error(f"Finished Join of {description}")



def main():

    ctx = {}

    log_level = os.environ.get('VFVS_LOGLEVEL', 'INFO').upper()
    logging.basicConfig(level=log_level)

    ctx['tools_path'] = os.getenv('VFVS_TOOLS_PATH', "/opt/vf/tools/bin")

    # Temp directory information
    temp_path = os.getenv('VFVS_TMP_PATH', None)
    if(temp_path):
        temp_path = os.path.join(temp_path, '')

    with tempfile.TemporaryDirectory(prefix=temp_path) as temp_dir:
        ctx['temp_dir'] = temp_dir

        # stat = shutil.disk_usage(path)
        stat = shutil.disk_usage(ctx['temp_dir'])
        if(stat.free < (1024 * 1024 * 1024 * 1)):
            raise RuntimeError(f"VFVS needs at least 1GB of space free in tmp dir ({ctx['temp_dir']}) free: {stat.free} bytes")


        print(ctx['temp_dir'])
        process(ctx)


if __name__ == '__main__':
    main()

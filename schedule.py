#!/usr/bin/env python3

import os
import re
import string
from collections import namedtuple
from typing import Dict, Final, Optional
import glob 
import yaml
from google.cloud import bigquery_datatransfer
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import bigquery_datatransfer_v1

class BigQuery(object):
    def __init__(self,cread_file,parent,sv_name,project_id,dataset_id):
        self.parent = parent
        self.credentials = self.read_credentials(cread_file)
        self.jobs = {}
        self.transfer_clientv1 = self.get_transfer_clientv1()
        self.transfer_client = self.get_transfer_client()
        self.service_account_name = sv_name
        self.project_id = project_id
        self.dataset_id = dataset_id

    '''
        Get transfer v1 clients
    '''
    def get_transfer_clientv1(self):
        return bigquery_datatransfer_v1.DataTransferServiceClient(credentials=self.credentials)

    '''
        Get transfer client
    '''
    def get_transfer_client(self):
        return bigquery_datatransfer.DataTransferServiceClient(credentials=self.credentials)

    '''
        Read creadentials from JSON file.
    '''
    @staticmethod
    def read_credentials(file_path):
        return service_account.Credentials.from_service_account_file(file_path)

    '''
        Check if scheduled job exists
    '''
    def check_if_job_exists(self,job_name):
        if job_name in self.jobs.keys():
            return True
        return False

    '''
        Delete scheduled job
    '''
    def delete_job(self,job_name):
        if self.check_if_job_exists(job_name):
            self.transfer_client.delete_transfer_config(name=self.jobs[job_name])

    '''
        Add scheduled job
    '''
    def add_job(self,kwargs):
        parent = self.transfer_client.common_project_path(self.project_id)
        transfer_config = bigquery_datatransfer.TransferConfig(
            destination_dataset_id=self.dataset_id,
            display_name=kwargs["display_name"],
            data_source_id=kwargs["scheduled_query"],
            params={
                "query": kwargs["query_string"],
                "destination_table_name_template": kwargs["table_name"],
                "write_disposition": kwargs["write_disposition"],
                "partitioning_field": kwargs["partitioning_field"],
            },
            schedule=kwargs["schedule"],
        )
        resp = self.transfer_client.create_transfer_config(
            bigquery_datatransfer.CreateTransferConfigRequest(
                parent=parent,
                transfer_config=transfer_config,
                service_account_name=self.service_account_name,
            )
        )
        #validate if job is created.
        return True
    
    '''
        List all jobs in project
    '''
    def list_job(self):
        request = bigquery_datatransfer_v1.ListTransferConfigsRequest(
            parent=self.parent,
        )
        page_result = self.transfer_clientv1.list_transfer_configs(request=request)
        for page in page_result:
            self.jobs[page.display_name] = page.name

class Jobs(object):
    def __init__(self, kwargs):
        self.files = self.getall_files(kwargs["path"])
        self.path = kwargs["path"]
        self.jobs = {}
        self.job_prefix = "summary_"
        self.bq = BigQuery(kwargs["cread_file"],kwargs["parent"],kwargs["sv_name"],kwargs["project_id"],kwargs["dataset_id"])

    @staticmethod
    def getall_files(path):
        return glob.glob(path)
    '''
        Parse all the YAML files and extract data elements from files.
    '''
    def parse_all_files(self):
        for file_path in self.files:
            file_name = file_path.split('/')[-1]
            with open(file_path, 'r') as f:
                self.jobs[file_name] = yaml.safe_load(f)
                if not self.validate_file_data(self.jobs[file_name]):
                    print("Cannot process data for ",file_name)

    '''
        Validate if YAML file has all the keys necessary
        Validate if YAML file has proper data.
    '''
    @staticmethod
    def validate_file_data(job_data):
        keys = ['name', 'start_time', 'end_time', 'destination_dataset', 
                'destination_table', 'partition_on', 'results_append', 
                'location', 'query', 'version']
        for key in keys:
            if key not in job_data.keys():
                return False
        return True

    def upsert_perodic_jobs(self):
        self.bq.list_job()
        for job,prams in self.jobs.items():
            if not self.bq.check_if_job_exists(job):
                self.bq.add_job(prams)

'''
    Scan all the yaml files and create/update bigquery jobs.
'''
if __name__ == "__main__":
    MODULE_PATH = os.path.dirname(os.path.realpath('__file__'))
    TEMPLATES_PATH = os.path.join(MODULE_PATH + "/templates/*")
    j = Jobs(TEMPLATES_PATH)
    j.parse_all_files()
    j.upsert_perodic_jobs()


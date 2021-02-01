from multiprocessing import Process, Pipe
import os
import time
import subprocess
import json
import requests
import argparse
from datetime import datetime, timedelta
import googleapiclient.discovery


def transfer_job(projectId,source_bucket,sink_bucket,conn):
    """Create a daily transfer from Standard to Nearline Storage class."""
    storagetransfer = googleapiclient.discovery.build('storagetransfer', 'v1')
    
    # Edit this template with desired parameters.
    now = datetime.now()
    d = now.strftime("%d")
    m = now.strftime("%m")
    y = now.strftime("%Y")
    m1 = now.strftime("%M")
    h = now.strftime("%H")
    s = now.strftime("%S")
    transfer_job = {
        'description': "data transfer job",
        'status': 'ENABLED',
        'projectId': projectId,
        'schedule': { 
		'scheduleStartDate': {
                'day': d,
                'month': m,
                'year': y
            },
                'scheduleEndDate': {
                'day': d,
                'month': m,
                'year': y
            }
           
        },
        'transferSpec': {
        
            
            'gcsDataSource': {
                'bucketName': source_bucket
            },
            'gcsDataSink': {
                'bucketName': sink_bucket
            },
            'transferOptions': {
                'deleteObjectsFromSourceAfterTransfer': 'false'
            }
        }
    }

    result = storagetransfer.transferJobs().create(body=transfer_job).execute()
    conn.send(result)

def start_command(projectId,source_bucket,TOKEN,conn):
    print("starting transfer...")
    data = {
        "outputUrlPrefix": "gs://"+source_bucket,
    }
    API_ENDPOINT = "https://datastore.googleapis.com/v1/projects/"+projectId+":export"
    r = requests.post(url = API_ENDPOINT, data = data, headers={'Authorization': 'Bearer '+TOKEN}) 
    conn.send(r.json()["name"].split("/")[-1])
    conn.close()
    print("datastore to "+source_bucket+" transfer started...")

    
def check_transfer_status(job_name):
    storagetransfer = googleapiclient.discovery.build('storagetransfer', 'v1')

    filterString = (
        '{{"project_id": "{project_id}", '
        '"job_names": ["{job_name}"]}}'
    ).format(project_id="$PROJECT_ID", job_name=job_name)

    result = storagetransfer.transferOperations().list(
        name="transferOperations",
        filter=filterString).execute()
    return result

def rsync(source_bucket,dest_bucket,encryption_key_path):
    command = "gsutil -o 'GSUtil:encryption_key="+encryption_key_path+"' -m rsync -r gs://"+source_bucket+" gs://"+dest_bucket+""
    os.system(command)


if __name__ == '__main__':
    acc_token = subprocess.run(['gcloud', 'auth','print-access-token'], stdout=subprocess.PIPE)
    ACCESS_TOKEN = acc_token.stdout.decode('utf-8').rstrip()
    projectId = "[Project_ID]"
    source_bucket = "[Name_of_the_source_bucket]"
    sink_bucket = "[Name_of_the_bucket-a]"
    csek_bucket = "[Name_of_the_bucket-b]"
    encryption_key_path = "[Encryption_Key_Value]"
    parent_conn, child_conn = Pipe()
    p = Process(target=start_command, args=(projectId,source_bucket,ACCESS_TOKEN,child_conn))
    p.start()
    operation = parent_conn.recv()
    state = "PROCESSING"
    while(state=="PROCESSING"):
        r = requests.get(url="https://datastore.googleapis.com/v1/projects/"+projectId+"/operations/"+operation,headers={'Authorization': 'Bearer '+ACCESS_TOKEN})
        state = r.json()["metadata"]["common"]["state"]
        print(state)
    p.join()
    p.close()
    parent_conn_transfer, child_conn_transfer = Pipe()
    
    p_transfer = Process(target=transfer_job,args=(projectId,source_bucket,sink_bucket,child_conn_transfer,))
    p_transfer.start()
    transfer_job_id = parent_conn_transfer.recv()
    print("----transfer job started-----")
    transfer_status = "IN_PROGRESS"
    while(transfer_status=="IN_PROGRESS"):
        transfer_result = check_transfer_status(transfer_job_id["name"])
        if(transfer_result=={}):
            transfer_status="IN_PROGRESS"
        else:
            transfer_status = transfer_result["operations"][0]["metadata"]["status"]
        print(transfer_status)
    
    p_transfer.join()
    if(transfer_status=="SUCCESS"):
        print("sync process from "+sink_bucket+" to "+csek_bucket+" started with CSEK...")
        rsync(sink_bucket,csek_bucket,encryption_key_path)

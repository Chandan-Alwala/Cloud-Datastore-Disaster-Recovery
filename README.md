# Cloud-Datastore-Disaster-Recovery
This repository sets up the disaster recovery solution for Cloud Datastore (GCP) with python

## Key Job: 
Disaster Recovery method has to be applied to export the data present in Datastore to Google Cloud Storage(GCS) bucket and transfer the data to another GCS bucket using Customer Supplied Encryption Key(CSEK). 

## Workflow of DR process:
Create a service account with the roles:
Cloud Datastore Import Export Admin
Storage Admin
Storage Transfer Admin
Create three Google Cloud Storage Buckets 

### Steps to run the code on Virtual Machine:
Create a Compute Engine VM with Linux Operating System and give the service account created above
The python version in the VM should be ‘Python 3.7’ or more.
Install Requests Module with the following command
 *    pip3 install requests
Install Google-API-Client with the following command
 *    pip3 install google-api-python-client 
The following is the command to run the python code after giving details of variables
*     python3 dr_code.py
where, ‘dr_code’ is the name of the python file in the VM.

Note: Apply lifecycle rules to GCS bucket-a so that unencrypted data in bucket-a can be deleted automatically after a certain time period.



from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook


import json
import logging
from datetime import datetime, timedelta
import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def get_jira_data(url, username, password):
    """Retrieves data from Jira instance using the provided parameters url, username, password.
    Args:
        url (_str_): Url of Jira instance
        username (_str_): Username used to authenticate Jira
        password (_str_): Password used to authenticate Jira
    """
    try:
        auth = (username, password)
        # Example REST API call to retrieve Jira data
        response = requests.get(f"{url}/rest/api/2/search?jql=project=AL", auth=auth)
        if response.status_code == 200:
            jira_data = response.json()
            #write_json_data_to_file(jira_data)
            return jira_data            
        else:
            print(f"Failed to retrieve data from Jira. Status code: {response.status_code}")
    except Exception as e:
        logging.error("Failed to retrieve Jira data: %s", str(e))

def extract_and_print_fields(ti):
    """Extracts necessary fields from the Jira data and prints them.
    Args:
        jira_data (dict): Data retrieved from Jira
    Returns:
        dict: Extracted data
    """
    jira_data = ti.xcom_pull(task_ids='get_jira_data_task')

    extracted_data = []
    for data in jira_data['issues']:
        job_id = data['id']
        email_address = data['fields']['reporter']['emailAddress']
        assignee_email = None
        if 'assignee' in data['fields'] and data['fields']['assignee'] is not None:
            assignee_email = data['fields']['assignee']['emailAddress']
        if assignee_email is not None:
            print("Assignee Email Address:", assignee_email)
        else:
            print("Assignee Email Address not available.")
        assignee = None  
        if 'assignee' in data['fields'] and data['fields']['assignee'] is not None:
            assignee = data['fields']['assignee']['displayName']
        if assignee is not None:
            print("Assignee Name:", assignee)
        else:
            print("Assignee Name not available.")    
        status = data['fields']['status']['name']
        summary = data['fields']['summary']
        reporter = data['fields']['reporter']['displayName']
        created_time = data['fields']['created']
        # Store extracted data as a dictionary
        extracted_data.append({
                'job_id': job_id,
                'email_address': email_address,
                'status': status,
                'summary': summary,
                'reporter': reporter,
                'created_time': created_time,
                'Assignee_email': assignee_email,
                'assignee_Name': assignee
            })
    return extracted_data  
      

def upload_to_azure_storage(ti):
    extracted_data = ti.xcom_pull(task_ids='extract_and_print_fields')

    hook = WasbHook(wasb_conn_id='adf_conn')
    container_name = 'airflow-destination'
    blob_name = 'jira_data.json'
    # Convert the list of dictionaries to JSON string
    extracted_data_json = json.dumps(extracted_data)
    hook.load_string(extracted_data_json, container_name, blob_name)

with DAG('jira_to_azure_storage',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    get_jira_data_task = PythonOperator(
        task_id='get_jira_data_task',
        python_callable=get_jira_data,
        op_kwargs={
            'url': '{{conn.jira_conn.host}}',
            'username': '{{conn.jira_conn.login}}',
            'password': '{{conn.jira_conn.password}}'
        },
        provide_context=True
    )

    extract_and_print_fields_task = PythonOperator(
        task_id='extract_and_print_fields',
        python_callable=extract_and_print_fields,
        #op_args=['extracted_data'],
        provide_context=True
    )

    upload_to_storage_task = PythonOperator(
        task_id='upload_to_azure_storage',
        python_callable=upload_to_azure_storage,
        provide_context=True
    )

    # Define task dependencies
    get_jira_data_task >> extract_and_print_fields_task >> upload_to_storage_task

    
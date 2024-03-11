from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import json
import logging
from datetime import datetime, timedelta
import requests
import uuid

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
    """Retrieves data from Jira instance using the provided parameters url, 
    username, password.
    Args:
        url (_str_): Url of Jira instance
        username (_str_): Username used to authenticate Jira
        password (_str_): Password used to authenticate Jira
    """
    try:
        auth = (username, password)
        # Example REST API call to retrieve Jira data
        response = requests.get(
            f"{url}/rest/api/2/search?jql=project=AL", auth=auth)
        if response.status_code == 200:
            jira_data = response.json()
            return jira_data
        else:
            print(
                f"Failed to retrieve data from Jira."
                f"Status code: {response.status_code}")
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
        if 'assignee' in data['fields'] and \
           data['fields']['assignee'] is not None:
            assignee_email = data['fields']['assignee']['emailAddress']
        else:
            print("Assignee Email Address not available.")
        assignee = None  
        if 'assignee' in data['fields'] and\
           data['fields']['assignee'] is not None:
            assignee = data['fields']['assignee']['displayName']
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
    # Generate a unique blob name using timestamp and UUID
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    # Truncate UUID to first 8 char for brevity
    unique_id = str(uuid.uuid4())[:8]
    blob_name = f'jira_data_{timestamp}_{unique_id}.json'
    # Convert the list of dictionaries to JSON string
    extracted_data_json = json.dumps(extracted_data)
    # Load extracted data from a JSON string into a container
    hook.load_string(extracted_data_json, container_name, blob_name)
    # Returning the blob name for XCom communication
    return blob_name


def load_data_from_azure_storage(ti):
    blob_name = ti.xcom_pull(task_ids='upload_to_azure_storage')
    if not blob_name:
        raise ValueError("No XCom value found.")
    logging.info("XCom value retrieved: %s", blob_name)
    hook = WasbHook(wasb_conn_id='adf_conn')
    container_name = 'airflow-destination'
    extracted_data_json = hook.read_file(container_name, blob_name)
    extracted_data = json.loads(extracted_data_json)
    if not isinstance(extracted_data, list):
        raise ValueError("Invalid data format: not a list.")
    return extracted_data


def filter_valid_tasks(ti):
    extracted_data = ti.xcom_pull(task_ids='load_data_from_azure_storage')
    valid_tasks = [item for item in extracted_data if item.get('Assignee_email')]
    if not valid_tasks:
        raise ValueError("No valid tasks found with assignee emails.")
    logging.info("Valid tasks found with assignee emails: %s", valid_tasks)
    return valid_tasks


def send_emails(valid_tasks):
    sender_email = 'bharaniseru@gmail.com'
    password = 'olsd xfuk dghv zeai'
    for item in valid_tasks:
        assignee_emails = item.get('Assignee_email')
        subject = f"Task Status: {item['status']}"
        if item['status'] == "Completed":
            body = "Dear Assignee,\nYour task has been completed."
        elif item['status'] == "Open":
            body = "Dear Assignee,\nYour task is still open and due soon."
        else:
            body = f"Dear Assignee,\nYour task is in status: {item['status']}"
        prepare_email(sender_email, password, assignee_emails, subject, body)


def prepare_email(sender_email, password, assignee_emails, subject, body):
    if isinstance(assignee_emails, list):
        for assignee_email in assignee_emails:
            send_email(sender_email, password, assignee_email, subject, body)
    else:
        send_email(sender_email, password, assignee_emails, subject, body)        


def send_email(sender_email, password, receiver_email, body):
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = receiver_email
    message['Subject'] = 'testing for Bharani Airflow project'
    message.attach(MIMEText(body, 'plain'))
    try:
        smtp_server = smtplib.SMTP('smtp.gmail.com', 587)
        smtp_server.starttls()
        smtp_server.login(sender_email, password)
        text = message.as_string()
        smtp_server.sendmail(sender_email, receiver_email, text)
        smtp_server.quit()
        print(f"Email sent successfully to {receiver_email}.")
    except Exception as e:
        logging.warning(f"Error sending email to {receiver_email}: {str(e)}")


with DAG('test_with_gmail',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    get_jira_data_task = PythonOperator(
        task_id='get_jira_data_task',
        python_callable=get_jira_data,
        provide_context=True,
        op_kwargs={
            'url': '{{conn.jira_conn.host}}',
            'username': '{{conn.jira_conn.login}}',
            'password': '{{conn.jira_conn.password}}'
        }
    )

    extract_and_print_fields_task = PythonOperator(
        task_id='extract_and_print_fields',
        python_callable=extract_and_print_fields,
        provide_context=True
    )

    upload_to_storage_task = PythonOperator(
        task_id='upload_to_azure_storage',
        python_callable=upload_to_azure_storage,
        provide_context=True
    )

    load_data_from_azure_storage_task = PythonOperator(
        task_id='load_data_from_azure_storage',
        python_callable=load_data_from_azure_storage,
        provide_context=True
    )

    filter_valid_tasks_task = PythonOperator(
        task_id='filter_valid_tasks',
        python_callable=filter_valid_tasks,
        provide_context=True
    )

    send_emails_task = PythonOperator(
        task_id='send_emails',
        python_callable=send_emails,
        provide_context=True
    )

    # Define task dependencies
    get_jira_data_task >> extract_and_print_fields_task
    extract_and_print_fields_task >> upload_to_storage_task
    upload_to_storage_task >> load_data_from_azure_storage_task
    load_data_from_azure_storage_task >> filter_valid_tasks_task
    filter_valid_tasks_task >> send_emails_task

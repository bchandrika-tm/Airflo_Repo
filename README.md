# Data Engineering Airflow Task Tracker & Notifications Project 

![image](https://github.com/bchandrika-tm/Airflow/assets/131654149/a87773c3-56b7-48db-a186-346c6e4eee82)


### The main objective of the project is to automation of sending email notifications to the employee whose task has been completed or due.

## Requirements
- Basic knowledge of Airflow
- Basic knowledge of Azure
- Intermediate level Python
- Basic knowledge of Azure datalake
- Basic knowledge of REST api
  
## To do list step by step
Create DAG file:
1. Retrieve Jira data using REST Api
2. Process Jira data into JSON format
3. Configure Airflow in Azure Data Factory
4. Store JSON data in ADLS
5. Schedule events to send notifications

Configure the Data Factory instance in Azure and launch the studio. Then, go to the 'Manage' section in the left side panel to find the Airflow instance. Spin up the resource, enabling Git sync during setup for easier synchronization with files when there are changes in DAGs. After setup, start monitoring the DAGs from the user interface.

Below are a few connections that need to be configured in the Airflow UI for the project, including connections for Data Factory, Jira, and blob storage.

## 1.1. Create a connection in Airflow for Azure Data Factory:

By configuring a connection in Airflow UI, it enables Airflow to seamlessly integrate with Data Factory, facilitating automated workflows and task execution within the project.

Give the details as shown:

- Connection ID: <name>
- Connection Type: Azure Data Factory 
- Client ID:
- Tenant ID:
- Subscription ID:
- Resource Group Name:
- Factory Name:

Test it, if it is successfully connected then save it and use it in DAG.

## 1.2. Create a connection for Jira in Airflow:

Establishing a connection in Airflow for Jira enables seamless integration between Airflow workflows and Jira tasks or projects. This connection allows Airflow to interact with Jira's API, enabling tasks such as creating, updating, or querying issues, fetching project information, or managing workflows directly from Airflow.

Open Airflow webserver and go to Admin in the top ribbon and go to 'connections'

![image](https://github.com/bchandrika-tm/Airflow/assets/131654149/f0e3e726-962c-409e-89d4-b2ecee54e749)

Press on '+' to add a new connection and give the neccesary details as below:

- Connection id: Jira_conn, the connection name will be used in the script or DAG file
- Connection type: HTTP(need to install corresponding Airflow package)
- Host: Jira instance url
- Description: <As you need>
- Login: username(your Techmodal email) 
- Schema: https
- Password: API token from Jira instance
- Steps to get API token from Jira
  
  ![image](https://github.com/bchandrika-tm/Airflow/assets/131654149/76d9690e-08c8-4c4f-b401-e00fe1cc8d4a)



 ![image](https://github.com/bchandrika-tm/Airflow/assets/131654149/f12ab4a3-8aa6-4adb-9c32-0bc26d20fef8)

And save the connection and use it in the DAG or test if the button is enabled and just save it.

![image](https://github.com/bchandrika-tm/Airflow/assets/131654149/d270d242-4a1f-4fb5-9833-477327196fa5)

The connection will be saved like this:
![image](https://github.com/bchandrika-tm/Airflow/assets/131654149/cddbbdef-3f4a-44d2-a294-108423640c84)


Now, we need a connection for Airflow to Azure storage account.

## 1.3. Create a connection for blob storage in Airflow:

Configuring a connection in Airflow for blob storage facilitates the integration of Airflow workflows with blob storage. This connection enables Airflow to securely access and manage files stored in blob storage containers as part of its data processing tasks.

Add another connection as same way as above for blob storage.

There are many ways to connect to blob storage: 

https://docs.astronomer.io/learn/connections/azure-blob-storage#:~:text=In%20the%20Airflow%20UI%20for,a%20name%20for%20the%20connection.

![image](https://github.com/bchandrika-tm/Airflow/assets/131654149/ceb8be1c-d0a5-4bcc-9619-9700bfdf7469)


Follow above link to connect to blob storage, test it and save it.


## 1.4. Create Airflow DAG and Tasks:

gmail_dag.py is the python script for DAG and Tasks. There are few tasks in this DAG.

This script retrieves data from Jira using the connection established in Airflow. 

The script processes the retrieved data to extract the necessary fields in JSON format.

It then writes the processed data to a blob storage in Azure and sends email notifications to the assignees if the conditions are met. 

The script utilizes SMTP server details for a Gmail account to send the emails.

```
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
"""
    Uploads extracted data to Azure Blob Storage.

    Parameters:
        ti (TaskInstance): The task instance object provided by Airflow.

    Returns:
        str: The name of the uploaded blob for XCom communication.

    This function retrieves extracted data from the 'extract_and_print_fields' task using XCom,
    generates a unique blob name based on timestamp and UUID, converts the extracted data to a JSON string,
    and uploads it to the specified container in Azure Blob Storage using the configured connection.
    The blob name is returned for XCom communication.
    """
    extracted_data = ti.xcom_pull(task_ids='extract_and_print_fields')
    hook = WasbHook(wasb_conn_id='adf_conn')
    container_name = 'airflow-destination'
    # Generate a unique blob name using timestamp and UUID
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    unique_id = str(uuid.uuid4())[:8]  # Truncate UUID to first 8 characters for brevity
    blob_name = f'jira_data_{timestamp}_{unique_id}.json'
    # Convert the list of dictionaries to JSON string
    extracted_data_json = json.dumps(extracted_data)
    # Load extracted data from a JSON string into a designated container and blob
    hook.load_string(extracted_data_json, container_name, blob_name)
    # Returning the blob name for XCom communication
    return blob_name


def send_email_to_assignee(ti, **kwargs):
"""
    Sends email notifications to assignees based on task status.

    Parameters:
        task_instance (TaskInstance): The task instance object provided by Airflow.
        **kwargs: Arbitrary keyword arguments. Expects 'ti' for accessing XCom.

    Raises:
        ValueError: If no XCom value is found, data format is invalid, or no valid tasks are found with assignee emails.
        ImportError: If the required modules (WasbHook, json, logging) are not available.

    This function retrieves the blob name from the 'upload_to_azure_storage' task using XCom,
    reads the extracted data from Azure Blob Storage, filters tasks with assignee emails,
    constructs email messages based on task status, and sends email notifications to assignees.
    The sender's email address and password are used for authentication.
    """
    ti = kwargs['ti']
    # Pulling the blob name from XCom
    blob_name = ti.xcom_pull(task_ids='upload_to_azure_storage')
    if not blob_name:
        raise ValueError("No XCom value found.")
    logging.info("XCom value retrieved: %s", blob_name)
    hook = WasbHook(wasb_conn_id='adf_conn')
    container_name = 'airflow-destination'
    # Load data from blob
    extracted_data_json = hook.read_file(container_name, blob_name)
    extracted_data = json.loads(extracted_data_json)
    if not isinstance(extracted_data, list):
        raise ValueError("Invalid data format: not a list.")
    # Filter out tasks without assignee emails
    valid_tasks = [item for item in extracted_data if item.get('Assignee_email')]
    if not valid_tasks:
        raise ValueError("No valid tasks found with assignee emails.")
    logging.info("Valid tasks found with assignee emails: %s", valid_tasks)
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
        # If multiple assignee emails, send email to each one
        if isinstance(assignee_emails, list):
            for assignee_email in assignee_emails:
                send_email(sender_email, password, assignee_email, subject, body)
        else:
            # Single assignee email
            send_email(sender_email, password, assignee_emails, subject, body)


def send_email(sender_email, password, receiver_email, subject, body):
"""
    Sends an email message using SMTP protocol.

    Parameters:
        sender_email (str): The sender's email address.
        password (str): The sender's email password or authentication token.
        receiver_email (str): The recipient's email address.
        body (str): The content of the email message.

    Raises:
        SMTPException: If an error occurs during the SMTP connection or sending process.
        ImportError: If the required modules (smtplib, email.mime.multipart, email.mime.text) are not available.

    This function constructs an email message with the provided sender, recipient, subject, and body,
    and sends it using the Simple Mail Transfer Protocol (SMTP) via the Gmail SMTP server.
    The sender's email address and password are used for authentication.
    """
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
        provide_context=True
    )

    upload_to_storage_task = PythonOperator(
        task_id='upload_to_azure_storage',
        python_callable=upload_to_azure_storage,
        provide_context=True
    )

    send_email_to_assignee_task = PythonOperator(
        task_id='send_email_to_assignee',
        python_callable=send_email_to_assignee,
        provide_context=True
    )

    # Define task dependencies
    get_jira_data_task >> extract_and_print_fields_task >> upload_to_storage_task >> send_email_to_assignee_task


```
## 1.5. Run the DAG

Upload the DAG to blob storage and create a linked service for Data Factory pointing to this blob storage.

Then, navigate to Azure Data Factory, access the Airflow instance, and import the DAG from the blob using the linked service.

Login to the Airflow web server and monitor the DAG that runs successfully, sending emails out to the assignees.

![image](https://github.com/bchandrika-tm/Airflo_Repo/assets/131654149/a04f9dce-5ae4-42c2-a9d0-4bdf6867734a)


or

Enabling Git sync when configuring the Airflow instance in Data Factory simplifies the process of importing files into the

Airflow instance whenever there is a change to DAG files.

The DAG is scheduled to run daily automatically.

The DAG can be paused or have its schedule_interval set to 'None' if the intention is not to run daily.

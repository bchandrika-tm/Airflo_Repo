from airflow import DAG
from airflow.utils.email import send_email
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from airflow.operators.python import PythonOperator
import smtplib
from email.mime.text import MIMEText

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 6),
    'email_on_failure': True,
    'email': 'bharaniseru@gmail.com'  # Your email address here
}

dag = DAG(
    'send_email_example',
    default_args=default_args,
    description='A DAG to send email using Gmail from Airflow',
    schedule_interval=None
)

def send_email_task():
    sender_email = 'bharaniseru@gmail.com'
    receiver_email = 'b.chandrika@techmodal.com'
    password = 'olsd xfuk dghv zeai'

    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = receiver_email
    message['Subject'] = 'Test Email from Airflow'

    body = 'This is a test email sent from Airflow using Gmail SMTP.'
    message.attach(MIMEText(body, 'plain'))

    try:
        smtp_server = smtplib.SMTP('smtp.gmail.com', 587)
        smtp_server.starttls()
        smtp_server.login(sender_email, password)
        text = message.as_string()
        smtp_server.sendmail(sender_email, receiver_email, text)
        smtp_server.quit()
        print("Email sent successfully.")
    except Exception as e:
        print(f"Error sending email: {str(e)}")

send_email_task = PythonOperator(
    task_id='send_email_task',
    python_callable=send_email_task,
    dag=dag
)

send_email_task

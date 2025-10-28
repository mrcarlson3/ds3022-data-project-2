from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import boto3
import requests
import time
import csv
import os

# config 

UVAID = 'mjy7nw'
API_URL = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{UVAID}"
SUBMISSION_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
AWS_REGION = 'us-east-1'
STORAGE_DIR = '/tmp/sqs_messages'

# dag arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Purpose: Creates and returns an SQS client using local AWS credentials.

def get_sqs_client():

    return boto3.client('sqs', region_name=AWS_REGION)


# Purpose: Sends a POST request to the scatter API to trigger message generation in SQS.

def trigger_api(**context):

    print(f"Triggering scatter API: {API_URL}")
    
    try:
        response = requests.post(API_URL, timeout=30)
        response.raise_for_status()
        payload = response.json()
        
        sqs_url = payload.get('sqs_url')
        if not sqs_url:
            raise AirflowException("API response missing 'sqs_url' field")
        
        print(f"API triggered successfully. Queue URL: {sqs_url}")
        return sqs_url
        
    except requests.exceptions.RequestException as e:
        raise AirflowException(f"Failed to trigger API: {e}")


# Purpose: Intelligently collects messages by checking queue availability BEFORE polling.
# Uses GetQueueAttributes to monitor message count, avoiding blind 900-second waits.
def collect_messages(**context):

    sqs_url = context['task_instance'].xcom_pull(task_ids='trigger_api')
    print(f"Starting message collection from queue: {sqs_url}")
    
    sqs = get_sqs_client()
    messages = {}  # Use dict to avoid duplicates: {order_no: word}
    
    os.makedirs(STORAGE_DIR, exist_ok=True)
    csv_file = f'{STORAGE_DIR}/messages_{context["ts_nodash"]}.csv'
    
    start_time = time.time()
    check_count = 0
    poll_count = 0
    
    # Exponential backoff parameters
    wait_time = 5  # Start checking every 2 seconds
    max_wait = 30  # Cap at 30 seconds between checks
    backoff_multiplier = 1.5
    
    while len(messages) < 21:
        check_count += 1
        elapsed = time.time() - start_time
        
        try:
            # Check queue attributes to determine if messages are available
            attrs_response = sqs.get_queue_attributes(
                QueueUrl=sqs_url,
                AttributeNames=['ApproximateNumberOfMessages']
            )
            
            available = int(attrs_response['Attributes'].get('ApproximateNumberOfMessages', 0))
            
            print(f"Check {check_count}: Available={available} | Collected={len(messages)}/21 | "
                  f"Wait={wait_time:.1f}s | Elapsed={elapsed:.1f}s")
            
            if available > 0 or (check_count % 3 == 0):
                # Messages detected - poll the queue
                poll_count += 1
                
                response = sqs.receive_message(
                    QueueUrl=sqs_url,
                    MaxNumberOfMessages=10,
                    MessageAttributeNames=['All'],
                    WaitTimeSeconds=2
                )
                
                batch_count = 0
                if 'Messages' in response:
                    for msg in response['Messages']:
                        try:
                            attrs = msg.get('MessageAttributes', {})
                            if 'order_no' in attrs and 'word' in attrs:
                                order_no = int(attrs['order_no']['StringValue'])
                                word = attrs['word']['StringValue']
                                
                                if order_no not in messages:
                                    messages[order_no] = word
                                    batch_count += 1
                                    
                                    # Write to CSV
                                    with open(csv_file, 'a', newline='') as f:
                                        writer = csv.writer(f)
                                        writer.writerow([order_no, word, context['ts']])
                                    
                                    print(f"  Message #{order_no}: '{word}'")
                                
                                # Delete processed message
                                sqs.delete_message(QueueUrl=sqs_url, ReceiptHandle=msg['ReceiptHandle'])
                                
                        except Exception as e:
                            print(f"  Error processing message: {e}")
                            if 'ReceiptHandle' in msg:
                                sqs.delete_message(QueueUrl=sqs_url, ReceiptHandle=msg['ReceiptHandle'])
                
                if batch_count > 0:
                    # Reset to aggressive checking after finding messages
                    wait_time = 2
                
            else:
                # No messages available - back off
                wait_time = min(wait_time * backoff_multiplier, max_wait)
            
            time.sleep(wait_time)
            
        except Exception as e:
            print(f"Error in check {check_count}: {e}")
            time.sleep(2)
    
    elapsed_total = time.time() - start_time
    print(f"\nCollection complete: {len(messages)} messages in {elapsed_total:.1f}s")
    print(f"Total checks: {check_count} | Actual polls: {poll_count} | "
          f"Efficiency: {poll_count/check_count*100:.1f}%")
    
    # Convert dict to sorted list of tuples for phrase assembly
    message_list = [(order, word) for order, word in sorted(messages.items())]
    
    return {
        'messages': message_list,
        'storage_file': csv_file
    }


# Purpose: Reads messages from persistent storage, sorts by order number, and reassembles the phrase.
def process_and_print_phrase(**context):

    result = context['task_instance'].xcom_pull(task_ids='collect_messages')
    storage_file = result['storage_file']
    
    print(f"Reading messages from persistent storage: {storage_file}")
    
    # Read from persistent storage (not just memory)
    messages = []
    with open(storage_file, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            order_no = int(row[0])
            word = row[1]
            messages.append((order_no, word))
    
    print(f"Read {len(messages)} messages from storage")
    
    if not messages:
        raise AirflowException("No messages to process!")
    
    # Sort by order number
    messages.sort(key=lambda x: x[0])
    
    # Log ordered fragments
    print("MESSAGE FRAGMENTS (ORDERED)")
    for order_no, word in messages:
        print(f"  Position {order_no:2d}: {word}")
    
    # Reassemble phrase
    words = [word for _, word in messages]
    phrase = ' '.join(words)
    
    # Log reassembled phrase
    print("REASSEMBLED PHRASE")
    print(f'  "{phrase}"')

    # Return for submission task
    return {
        'phrase': phrase,
        'word_count': len(messages),
        'uvaid': UVAID
    }


# Purpose: Submits the reassembled phrase to the submission queue with required attributes.
def submit_phrase(**context):

    result = context['task_instance'].xcom_pull(task_ids='process_phrase')
    phrase = result['phrase']
    uvaid = result['uvaid']
    
    print(f"Submitting phrase for {uvaid}: {phrase}")
    
    sqs = get_sqs_client()
    
    try:
        response = sqs.send_message(
            QueueUrl=SUBMISSION_QUEUE_URL,
            MessageBody="Phrase submission",
            MessageAttributes={
                'uvaid': {
                    'StringValue': uvaid,
                    'DataType': 'String'
                },
                'phrase': {
                    'StringValue': phrase,
                    'DataType': 'String'
                },
                'platform': {
                    'StringValue': 'airflow',
                    'DataType': 'String'
                }
            }
        )
        
        message_id = response.get('MessageId')
        print(f"Submission successful - Message ID: {message_id}")
        
        return {
            'success': True,
            'message_id': message_id,
            'uvaid': uvaid,
            'phrase': phrase,
            'platform': 'airflow'
        }
        
    except Exception as e:
        print(f"Submission failed: {e}")
        raise AirflowException(f"Failed to submit phrase: {e}")


# Define the DAG
with DAG(
    dag_id='sqs_message_reassembly_pipeline',
    default_args=default_args,
    description='Reassemble scattered SQS messages into complete phrase',
    start_date=datetime(2025, 1, 26),
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['mjy7nw'],
    doc_md=__doc__,
) as dag:

    # Task 1: Trigger scatter API to populate queue
    trigger_api_task = PythonOperator(
        task_id='trigger_api',
        python_callable=trigger_api,
        doc_md="Trigger API to scatter message fragments into SQS queue"
    )

    # Task 2: Collect all message fragments using intelligent availability checking
    collect_task = PythonOperator(
        task_id='collect_messages',
        python_callable=collect_messages,
        execution_timeout=timedelta(hours=1),
        doc_md="Collect messages using check-before-poll strategy with persistent storage"
    )

    # Task 3: Process and print reassembled phrase
    process_task = PythonOperator(
        task_id='process_phrase',
        python_callable=process_and_print_phrase,
        doc_md="Reassemble phrase from persistent storage and print result"
    )

    # Task 4: Submit phrase to submission queue
    submit_task = PythonOperator(
        task_id='submit_phrase',
        python_callable=submit_phrase,
        doc_md="Submit reassembled phrase with uvaid and platform to submission queue"
    )

    # Define task dependencies
    trigger_api_task >> collect_task >> process_task >> submit_task
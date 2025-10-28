import boto3
import requests
import time
import csv
from pathlib import Path
from prefect import flow, task, get_run_logger
from typing import Dict, List, Tuple

# Configuration for persistent message storage
STORAGE_DIR = Path('/tmp/sqs_messages')


@task
def trigger_api_and_get_sqs_url():
    """Make POST request to trigger the external API and retrieve the SQS queue URL."""
    logger = get_run_logger()
    url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/mjy7nw"
    
    try:
        # Send POST request to API endpoint to generate SQS messages
        response = requests.post(url)
        response.raise_for_status()
        payload = response.json()
        
        # Extract and return SQS URL from API response
        sqs_url = payload['sqs_url']
        logger.info(f"API triggered successfully. SQS URL: {sqs_url}")
        return sqs_url
        
    except Exception as e:
        # Log and raise errors if API request fails
        logger.error(f"Failed to trigger API: {e}")
        raise


@task
def collect_all_messages(sqs_url: str) -> Dict:
    logger = get_run_logger()
    sqs = boto3.client('sqs')
    messages = {}  # Changed: Use dict to prevent duplicates by order_no
    
    # Create persistent storage directory and file for collected messages
    STORAGE_DIR.mkdir(exist_ok=True)
    csv_file = STORAGE_DIR / f'messages_{int(time.time())}.csv'
    logger.info(f"Storing messages to: {csv_file}")
    logger.info("Starting message collection - will collect until 21 messages received")
    
    check_count = 0
    poll_count = 0
    
    # Exponential backoff parameters
    wait_time = 5
    max_wait = 30
    backoff_multiplier = 1.5
    
    # Keep going until we have all 21 messages
    while len(messages) < 21:
        check_count += 1
        
        try:
            # Check queue attributes to see if messages are available
            attrs_response = sqs.get_queue_attributes(
                QueueUrl=sqs_url,
                AttributeNames=['ApproximateNumberOfMessages']
            )
            
            available = int(attrs_response['Attributes'].get('ApproximateNumberOfMessages', 0))
            
            logger.info(
                f"Check {check_count}: Available={available} | Collected={len(messages)}/21 | "
                f"Wait={wait_time:.1f}s"
            )
            
            # Only poll if messages are actually available
            if available > 0 or (check_count % 3 == 0):
                logger.info("Messages detected, polling now")
                poll_count += 1
                
                # Receive up to 10 messages
                response = sqs.receive_message(
                    QueueUrl=sqs_url,
                    MaxNumberOfMessages=10,
                    MessageAttributeNames=['All'],
                    WaitTimeSeconds=2
                )
                
                batch_count = 0
                # Process received messages if available
                if 'Messages' in response:
                    for message in response['Messages']:
                        try:
                            # Extract message data
                            order_no = int(message['MessageAttributes']['order_no']['StringValue'])
                            word = message['MessageAttributes']['word']['StringValue']
                            receipt_handle = message['ReceiptHandle']
                            
                            # Use dict to automatically handle duplicates
                            if order_no not in messages:
                                messages[order_no] = word
                                batch_count += 1
                                
                                # Write to CSV file immediately for persistence
                                with open(csv_file, 'a', newline='') as f:
                                    writer = csv.writer(f)
                                    writer.writerow([order_no, word, time.time()])
                                
                                logger.info(f"Collected message {order_no}: '{word}' - total: {len(messages)}/21")
                            else:
                                logger.info(f"Duplicate message {order_no} detected, skipping: '{word}'")
                            
                            # Delete message from queue
                            sqs.delete_message(QueueUrl=sqs_url, ReceiptHandle=receipt_handle)
                            
                        except KeyError as e:
                            logger.error(f"Missing expected message attribute: {e}")
                            if 'ReceiptHandle' in message:
                                sqs.delete_message(QueueUrl=sqs_url, ReceiptHandle=message['ReceiptHandle'])
                        except Exception as e:
                            logger.error(f"Error processing message: {e}")
                            if 'ReceiptHandle' in message:
                                sqs.delete_message(QueueUrl=sqs_url, ReceiptHandle=message['ReceiptHandle'])
                
                if batch_count > 0:
                    # Reset to aggressive checking after finding messages
                    wait_time = 2
                    
            else:
                # No messages available - exponential backoff
                wait_time = min(wait_time * backoff_multiplier, max_wait)
            
            time.sleep(wait_time)
                    
        except Exception as e:
            logger.error(f"Error during message collection attempt {check_count}: {e}")
            time.sleep(2)
    
    logger.info(f"Successfully collected all 21 messages in {check_count} checks")
    logger.info(f"Actual polls performed: {poll_count}")
    logger.info(f"Efficiency: {poll_count/check_count*100:.1f}% of checks resulted in polls")
    logger.info(f"Messages stored persistently to: {csv_file}")
    
    # Convert dict to sorted list for return
    message_list = [(order, word) for order, word in sorted(messages.items())]
    
    return {
        'messages': message_list,
        'storage_file': str(csv_file)
    }



@task
def reassemble_and_print_locally(result: Dict):

    logger = get_run_logger()
    storage_file = result['storage_file']
    
    logger.info(f"Reading messages from persistent storage: {storage_file}")
    
    # Read messages from CSV file instead of just using the in-memory list
    # This proves the persistent storage is working
    messages = []
    with open(storage_file, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            order_no = int(row[0])
            word = row[1]
            messages.append((order_no, word))
    
    logger.info(f"Read {len(messages)} messages from storage")
    
    if not messages:
        logger.error("No messages to reassemble")
        return
    
    # Sort messages by order number
    messages.sort(key=lambda x: x[0])
    
    # Join words to reconstruct phrase
    words = [word for order, word in messages]
    phrase = ' '.join(words)
    
    # Print phrase to console for verification
    print(f"\n{'='*50}")
    print(f"REASSEMBLED PHRASE:")
    print(f"'{phrase}'")
    print(f"{'='*50}\n")
    
    logger.info(f"Reassembled phrase printed locally: '{phrase}'")
    logger.info("Note: This phrase was NOT submitted to the submission queue")
    
    return phrase


@task
def reassemble_and_submit(result: Dict):

    logger = get_run_logger()
    storage_file = result['storage_file']
    
    logger.info(f"Reading from persistent storage for submission: {storage_file}")
    
    # Read messages from the CSV file
    messages = []
    with open(storage_file, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            order_no = int(row[0])
            word = row[1]
            messages.append((order_no, word))
    
    if not messages:
        logger.error("No messages to reassemble")
        return
    
    # Sort messages by order number
    messages.sort(key=lambda x: x[0])
    
    # Join words to form full phrase
    words = [word for order, word in messages]
    phrase = ' '.join(words)
    
    logger.info(f"Reassembled phrase: '{phrase}'")
    
    # Define submission queue and send final phrase
    submission_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
    sqs = boto3.client('sqs')
    
    try:
        # Send final phrase with metadata attributes
        response = sqs.send_message(
            QueueUrl=submission_url,
            MessageBody=f"Submission for mjy7nw: {phrase}",
            MessageAttributes={
                'uvaid': {'DataType': 'String', 'StringValue': 'mjy7nw'},
                'phrase': {'DataType': 'String', 'StringValue': phrase},
                'platform': {'DataType': 'String', 'StringValue': 'prefect'}
            }
        )
        
        logger.info(f"Submission successful. Response: {response}")
        logger.info(f"Final phrase submitted: '{phrase}'")
        
    except Exception as e:
        # Log submission errors and raise
        logger.error(f"Failed to submit phrase: {e}")
        raise


@flow(name="sqs-message-pipeline")
def main_flow():
    """Orchestrate the full Prefect pipeline: trigger API, collect messages, and reassemble phrase."""
    logger = get_run_logger()
    logger.info("Starting SQS Message Pipeline")
    
    # Step 1: Trigger API to populate SQS and retrieve queue URL
    sqs_url = trigger_api_and_get_sqs_url()
    
    # Step 2: Collect messages from SQS queue with persistent storage
    result = collect_all_messages(sqs_url)
    
    # Step 3: Print reassembled phrase locally for verification
    reassemble_and_print_locally(result)

    # Step 4: Submit final phrase to submission queue
    reassemble_and_submit(result)
    
    logger.info("Pipeline completed successfully")


if __name__ == "__main__":
    # Entry point to execute the full Prefect flow
    main_flow()
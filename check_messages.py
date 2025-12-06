"""Quick script to check if messages have been received for the sender number."""

import os
import json
from dotenv import load_dotenv
from core import KafkaConsumer
from datetime import datetime

load_dotenv()

def check_recent_messages(limit=10):
    """Check for recent messages in the inbound topic."""
    broker = os.getenv('KAFKA_BROKER')
    topic_in = os.getenv('KAFKA_TOPIC_IN')
    sender_number = os.getenv('SENDER_NUMBER', 'N/A')
    
    print(f"Checking for messages sent to: {sender_number}")
    print(f"Topic: {topic_in}")
    print(f"Looking for last {limit} messages...\n")
    
    consumer = KafkaConsumer(
        broker,
        topic_in,
        group_id=f"{os.getenv('KAFKA_GROUP_ID', 'series-swarm-group')}-check-{datetime.now().timestamp()}",
        sasl_username=os.getenv('KAFKA_SASL_USERNAME'),
        sasl_password=os.getenv('KAFKA_SASL_PASSWORD'),
        client_id=os.getenv('KAFKA_CLIENT_ID')
    )
    
    messages_found = []
    timeout_count = 0
    max_timeouts = 5  # Stop after 5 seconds of no messages
    
    try:
        while len(messages_found) < limit and timeout_count < max_timeouts:
            message = consumer.consume(timeout=1.0)
            if message:
                timeout_count = 0  # Reset timeout counter
                messages_found.append(message)
            else:
                timeout_count += 1
                if timeout_count == 1:
                    print("No messages found yet, continuing to check...")
    except KeyboardInterrupt:
        print("\nStopped checking...")
    finally:
        consumer.close()
    
    if messages_found:
        print(f"\n✅ Found {len(messages_found)} message(s):\n")
        for i, msg in enumerate(messages_found, 1):
            value = msg.get('value', {})
            data = value.get('data', {})
            
            # Extract message details from Series API structure
            text = data.get('text', value.get('text', 'N/A'))
            from_num = data.get('from_phone', value.get('from', 'Unknown'))
            sent_at = data.get('sent_at', value.get('created_at', 'Unknown'))
            event_type = value.get('event_type', 'unknown')
            
            # Find recipient (your sender number)
            chat_handles = data.get('chat_handles', [])
            recipient = sender_number
            for handle in chat_handles:
                if handle.get('identifier') == sender_number:
                    recipient = sender_number
                    break
            
            print(f"{i}. 📨 {event_type.upper()}")
            print(f"   From: {from_num}")
            print(f"   To: {recipient}")
            print(f"   Time: {sent_at}")
            print(f"   Text: {text}")
            print()
    else:
        print(f"\n❌ No messages found in the topic.")
        print(f"\nTo test:")
        print(f"1. Send a text message to {sender_number}")
        print(f"2. Run: python monitor_messages.py inbound")
        print(f"   (This will continuously monitor for new messages)")
    
    return len(messages_found)

if __name__ == '__main__':
    import sys
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    check_recent_messages(limit)


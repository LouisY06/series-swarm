"""Monitor and test Kafka messages for SeriesSwarm."""

import os
import json
import sys
from dotenv import load_dotenv
from core import KafkaConsumer, KafkaProducer

load_dotenv()

def monitor_inbound():
    """Monitor messages coming in from the inbound topic."""
    broker = os.getenv('KAFKA_BROKER')
    topic_in = os.getenv('KAFKA_TOPIC_IN')
    sender_number = os.getenv('SENDER_NUMBER', 'N/A')
    
    print(f"Monitoring inbound topic: {topic_in}")
    print(f"Sender Number: {sender_number}")
    print("Waiting for messages sent to this number...")
    print("Press Ctrl+C to stop...\n")
    
    consumer = KafkaConsumer(
        broker,
        topic_in,
        group_id=f"{os.getenv('KAFKA_GROUP_ID', 'series-swarm-group')}-monitor",
        sasl_username=os.getenv('KAFKA_SASL_USERNAME'),
        sasl_password=os.getenv('KAFKA_SASL_PASSWORD'),
        client_id=os.getenv('KAFKA_CLIENT_ID')
    )
    
    message_count = 0
    try:
        while True:
            message = consumer.consume(timeout=1.0)
            if message:
                message_count += 1
                value = message.get('value', {})
                
                print("=" * 60)
                print(f"📨 MESSAGE #{message_count} RECEIVED")
                print("=" * 60)
                print(f"Partition: {message.get('partition')}, Offset: {message.get('offset')}")
                print(f"Key: {message.get('key')}")
                print(f"Headers: {message.get('headers')}")
                
                # Extract and display key information from Series API structure
                data = value.get('data', {})
                text = data.get('text', value.get('text', value.get('content', value.get('message', ''))))
                from_number = data.get('from_phone', value.get('from', value.get('sender', 'Unknown')))
                sent_at = data.get('sent_at', value.get('created_at', 'Unknown'))
                event_type = value.get('event_type', 'unknown')
                
                # Find recipient
                chat_handles = data.get('chat_handles', [])
                recipient = sender_number
                for handle in chat_handles:
                    if handle.get('identifier') == sender_number:
                        recipient = sender_number
                        break
                
                print(f"\n📨 Event: {event_type}")
                print(f"📱 From: {from_number}")
                print(f"📱 To: {recipient}")
                print(f"🕐 Time: {sent_at}")
                print(f"💬 Text: {text}")
                
                # Show full message structure
                print(f"\n📋 Full Message Data:")
                print(json.dumps(value, indent=2))
                print("=" * 60)
                print()
    except KeyboardInterrupt:
        print(f"\n\nStopped monitoring. Total messages received: {message_count}")
    finally:
        consumer.close()

def monitor_outbound():
    """Monitor messages being produced to the outbound topic."""
    broker = os.getenv('KAFKA_BROKER')
    topic_out = os.getenv('KAFKA_TOPIC_OUT')
    
    print(f"Monitoring outbound topic: {topic_out}")
    print("Press Ctrl+C to stop...\n")
    
    consumer = KafkaConsumer(
        broker,
        topic_out,
        group_id=f"{os.getenv('KAFKA_GROUP_ID', 'series-swarm-group')}-monitor-out",
        sasl_username=os.getenv('KAFKA_SASL_USERNAME'),
        sasl_password=os.getenv('KAFKA_SASL_PASSWORD'),
        client_id=os.getenv('KAFKA_CLIENT_ID')
    )
    
    try:
        while True:
            message = consumer.consume(timeout=1.0)
            if message:
                print("=" * 60)
                print(f"Received message from partition {message.get('partition')}, offset {message.get('offset')}")
                print(f"Key: {message.get('key')}")
                print(f"Headers: {message.get('headers')}")
                # Outbound messages might be binary, try to decode
                value = message.get('value', {})
                if isinstance(value, dict):
                    print(f"Value: {json.dumps(value, indent=2)}")
                else:
                    print(f"Value (binary): {len(value) if hasattr(value, '__len__') else 'N/A'} bytes")
                print("=" * 60)
                print()
    except KeyboardInterrupt:
        print("\nStopping monitor...")
    finally:
        consumer.close()

def send_test_message():
    """Send a test message to the inbound topic."""
    broker = os.getenv('KAFKA_BROKER')
    topic_in = os.getenv('KAFKA_TOPIC_IN')
    
    producer = KafkaProducer(
        broker,
        topic_in,
        sasl_username=os.getenv('KAFKA_SASL_USERNAME'),
        sasl_password=os.getenv('KAFKA_SASL_PASSWORD'),
        client_id=os.getenv('KAFKA_CLIENT_ID')
    )
    
    # Example test messages for different intents
    test_messages = [
        {
            'name': 'Ticket Request',
            'data': {
                'text': 'I need a ticket for the concert on Friday night',
                'data': {
                    'title': 'Friday Night Concert',
                    'date': '2024-12-27',
                    'time': '8:00 PM',
                    'location': 'Madison Square Garden',
                    'ticket_number': 'TICKET-001',
                    'attendee_name': 'Test User'
                }
            }
        },
        {
            'name': 'Calendar Request',
            'data': {
                'text': 'Add this meeting to my calendar',
                'data': {
                    'title': 'Team Meeting',
                    'description': 'Weekly team sync',
                    'start_date': '2024-12-26T14:00:00',
                    'end_date': '2024-12-26T15:00:00',
                    'location': 'Conference Room A',
                    'organizer': 'organizer@example.com',
                    'attendee': 'attendee@example.com'
                }
            }
        },
        {
            'name': 'Contact Request',
            'data': {
                'text': 'Send me John\'s contact information',
                'data': {
                    'name': 'John Doe',
                    'email': 'john@example.com',
                    'phone': '+1234567890',
                    'organization': 'Acme Corp',
                    'title': 'Software Engineer'
                }
            }
        },
        {
            'name': 'Audio Request',
            'data': {
                'text': 'Give me an audio briefing of today\'s schedule',
                'data': {
                    'text': 'Today you have three meetings: Team standup at 10 AM, Client call at 2 PM, and Project review at 4 PM.'
                }
            }
        }
    ]
    
    print("Available test messages:")
    for i, msg in enumerate(test_messages, 1):
        print(f"{i}. {msg['name']}")
    
    choice = input("\nSelect a test message (1-4) or 'all' to send all: ").strip()
    
    if choice.lower() == 'all':
        messages_to_send = test_messages
    else:
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(test_messages):
                messages_to_send = [test_messages[idx]]
            else:
                print("Invalid choice")
                return
        except ValueError:
            print("Invalid choice")
            return
    
    for msg in messages_to_send:
        print(f"\nSending {msg['name']}...")
        success = producer.produce(msg['data'])
        if success:
            print(f"✓ {msg['name']} sent successfully")
        else:
            print(f"✗ Failed to send {msg['name']}")
    
    producer.flush()
    producer.close()
    print("\nAll messages sent!")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python monitor_messages.py inbound   - Monitor inbound topic")
        print("  python monitor_messages.py outbound    - Monitor outbound topic")
        print("  python monitor_messages.py send        - Send test message")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    if command == 'inbound':
        monitor_inbound()
    elif command == 'outbound':
        monitor_outbound()
    elif command == 'send':
        send_test_message()
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


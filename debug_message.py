"""Debug message processing to see what's happening."""

import os
import json
from dotenv import load_dotenv
from core import KafkaConsumer, Router, SeriesAPI
from agents.secretary import generate_calendar_invite

load_dotenv()

def debug_latest_message():
    """Debug the latest message to see structure."""
    broker = os.getenv('KAFKA_BROKER')
    topic_in = os.getenv('KAFKA_TOPIC_IN')
    
    consumer = KafkaConsumer(
        broker,
        topic_in,
        group_id=f"debug-{os.getpid()}",
        sasl_username=os.getenv('KAFKA_SASL_USERNAME'),
        sasl_password=os.getenv('KAFKA_SASL_PASSWORD'),
        client_id=os.getenv('KAFKA_CLIENT_ID')
    )
    
    print("Waiting for a message...")
    message = consumer.consume(timeout=10.0)
    
    if message:
        value = message.get('value', {})
        data = value.get('data', {})
        
        print("\n" + "="*60)
        print("MESSAGE STRUCTURE:")
        print("="*60)
        print(json.dumps(value, indent=2))
        print("\n" + "="*60)
        print("EXTRACTED DATA:")
        print("="*60)
        print(f"Text: {data.get('text')}")
        print(f"From Phone: {data.get('from_phone')}")
        print(f"Chat ID: {data.get('chat_id')} (type: {type(data.get('chat_id'))})")
        print(f"Chat Handles: {data.get('chat_handles', [])}")
        
        # Test extraction
        api = SeriesAPI(os.getenv('SERIES_API_KEY'), os.getenv('SERIES_API_URL', 'https://api.series.im'))
        chat_id = api.get_chat_id_from_message(value)
        recipient = api.get_recipient_phone(value)
        
        print("\n" + "="*60)
        print("EXTRACTION RESULTS:")
        print("="*60)
        print(f"Extracted Chat ID: {chat_id} (type: {type(chat_id)})")
        print(f"Extracted Recipient: {recipient}")
        
        # Test routing
        router = Router(os.getenv('OPENAI_API_KEY'))
        routing_msg = {
            'text': data.get('text', ''),
            'data': data
        }
        agent_func = router.route(routing_msg)
        print(f"\nRouted to agent: {agent_func.__name__ if agent_func else 'None'}")
        
    else:
        print("No message received within timeout")
    
    consumer.close()

if __name__ == '__main__':
    debug_latest_message()


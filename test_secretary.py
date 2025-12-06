"""Test the secretary agent with AI parsing."""

import os
import sys
from dotenv import load_dotenv
from agents.secretary import generate_calendar_invite

load_dotenv()

def test_secretary_with_text():
    """Test secretary agent with natural language text."""
    print("Testing Secretary Agent with Natural Language Text\n")
    print("=" * 60)
    
    # Test cases
    test_cases = [
        {
            'name': 'Simple meeting',
            'data': {
                'text': 'Meeting tomorrow at 2pm in the conference room',
                'openai_api_key': os.getenv('OPENAI_API_KEY')
            }
        },
        {
            'name': 'Event with details',
            'data': {
                'text': 'Team standup meeting on December 26th at 10:00 AM in the main office',
                'openai_api_key': os.getenv('OPENAI_API_KEY')
            }
        },
        {
            'name': 'Structured data (no parsing needed)',
            'data': {
                'title': 'Pre-scheduled Event',
                'description': 'This is a pre-structured event',
                'start_date': '2024-12-26T14:00:00',
                'end_date': '2024-12-26T15:00:00',
                'location': 'Virtual Meeting',
                'openai_api_key': os.getenv('OPENAI_API_KEY')
            }
        }
    ]
    
    for i, test in enumerate(test_cases, 1):
        print(f"\nTest {i}: {test['name']}")
        print(f"Input: {test['data'].get('text', test['data'].get('title', 'N/A'))}")
        print("-" * 60)
        
        try:
            result = generate_calendar_invite(test['data'])
            
            # Save to file
            filename = f"test_calendar_{i}.ics"
            with open(filename, 'wb') as f:
                f.write(result.getvalue())
            
            print(f"✓ Success! Generated {len(result.getvalue())} bytes")
            print(f"✓ Saved to: {filename}")
            
        except Exception as e:
            print(f"✗ Error: {e}")
            import traceback
            traceback.print_exc()
    
    print("\n" + "=" * 60)
    print("Testing complete!")

if __name__ == '__main__':
    if not os.getenv('OPENAI_API_KEY'):
        print("ERROR: OPENAI_API_KEY not found in environment variables")
        print("Please set it in your .env file")
        sys.exit(1)
    
    test_secretary_with_text()


#!/bin/bash
# Watch bot logs in real-time
tail -f /tmp/bot_output.log 2>/dev/null || echo "Bot not running or no log file yet"


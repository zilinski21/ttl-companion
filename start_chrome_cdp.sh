#!/bin/bash
# Launch Chrome with remote debugging enabled for TTL Monitor
# Run this ONCE before starting recording

echo "Starting Chrome with CDP on port 9222..."
echo "After Chrome opens, navigate to your TikTok Shop streamer dashboard."
echo ""

/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --remote-debugging-port=9222 &

echo "Chrome launched! You can now start recording from the TTL dashboard."

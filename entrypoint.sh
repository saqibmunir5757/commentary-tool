#!/bin/bash
# Start virtual display
Xvfb :99 -screen 0 1920x1080x24 &
sleep 1
export DISPLAY=:99

# Start VNC server on display :99, no password
x11vnc -display :99 -forever -nopw -shared -rfbport 5900 &

# Start noVNC web client (proxies VNC on port 6080)
websockify --web /usr/share/novnc/ 6080 localhost:5900 &

# Run the app
exec python server.py

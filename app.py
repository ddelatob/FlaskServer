import asyncio
import threading
from flask import Flask, render_template
from flask_socketio import SocketIO
from azure.eventhub.aio import EventHubConsumerClient
import json
import os
from dotenv import load_dotenv



# 1. Azure & Flask Configuration
load_dotenv()
CONNECTION_STR = os.environ.get("EVENT_HUB_CONNECTION_STRING")

CONSUMER_GROUP = "$Default"

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app, cors_allowed_origins="*")

# Store the latest stats in memory
latest_stats = {
    "hashrate": 0.0,
    "shares": 0,
    "timestamp": "Waiting for data..."
}

# 2. Azure Event Hub Background Listener
async def on_event(partition_context, event):
    global latest_stats
    try:
        # Parse the raw payload string coming from Azure
        payload_data = json.loads(event.body_as_str())
        
        # Check if the payload contains our expected fields
        if "hashrate" in payload_data and "shares" in payload_data:
            latest_stats["hashrate"] = payload_data["hashrate"]
            latest_stats["shares"] = payload_data["shares"]
            
            print(f"[Received] Hashrate: {latest_stats['hashrate']} KH/s | Shares: {latest_stats['shares']}")
            
            # Broadcast the updated analytics immediately to all connected browsers
            socketio.emit('telemetry_update', latest_stats)

        await partition_context.update_checkpoint(event)
    except Exception as e:
        print(f"Error parsing event: {e}")

async def start_event_hub_listener():
    client = EventHubConsumerClient.from_connection_string(
        CONNECTION_STR, 
        consumer_group=CONSUMER_GROUP
    )
    async with client:
        print(" Connected to Azure Event Hub! Listening for ESP32 telemetry...")
        await client.receive(on_event=on_event, starting_position="@latest")

def run_async_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(start_event_hub_listener())

# 3. Web Routes
@app.route('/')
def index():
    return render_template('index.html')

# 4. Server Entry Point
if __name__ == '__main__':
    # Run Azure listener in a background thread
    listener_thread = threading.Thread(target=run_async_loop, daemon=True)
    listener_thread.start()

    # Start Flask Web Server
    print("Starting Web Dashboard on http://localhost:5000")
    socketio.run(app, host='0.0.0.0', port=5000, debug=False)
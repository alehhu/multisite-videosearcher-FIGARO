from flask import Flask, request, jsonify
import paho.mqtt.client as mqtt
import json
import time
import threading
import uuid
from datetime import datetime
import os

app = Flask(__name__)

# MQTT Configuration
MQTT_BROKER = "mosquitto-service"
MQTT_PORT = 1883
MQTT_TRIGGER_TOPIC = "pipeline/A"
MQTT_RESULT_TOPIC = "pipeline/done"
MQTT_DLQ_TOPIC = "pipeline/dlq"

# Store for tracking in-flight requests
pending_requests = {}
results_lock = threading.Lock()

# MQTT Client setup
mqtt_client = mqtt.Client(
    protocol=mqtt.MQTTv5,
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2
)

def on_connect(client, userdata, flags, rc, props):
    """Callback when connected to MQTT broker"""
    print(f"[Gateway] Connected to MQTT broker with result code: {rc}")
    # Subscribe to result and DLQ topics to track completions
    client.subscribe(MQTT_RESULT_TOPIC, qos=1)
    client.subscribe(MQTT_DLQ_TOPIC, qos=1)
    print(f"[Gateway] Subscribed to {MQTT_RESULT_TOPIC} and {MQTT_DLQ_TOPIC}")

def on_message(client, userdata, msg, props=None):
    """Callback when a message is received"""
    try:
        payload = json.loads(msg.payload.decode())
        job_dir = payload.get("dir", "")
        
        with results_lock:
            if job_dir in pending_requests:
                if msg.topic == MQTT_RESULT_TOPIC:
                    pending_requests[job_dir]['result'] = payload
                    pending_requests[job_dir]['status'] = 'completed'
                elif msg.topic == MQTT_DLQ_TOPIC:
                    pending_requests[job_dir]['error'] = payload
                    pending_requests[job_dir]['status'] = 'failed'
                
                print(f"[Gateway] Job {job_dir} {pending_requests[job_dir]['status']}")
    except Exception as e:
        print(f"[Gateway] Error processing message: {e}")

mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# Start MQTT client in background thread
def start_mqtt():
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT)
    mqtt_client.loop_start()

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "service": "gateway"}), 200

@app.route('/workflow/invoke/<workflow_name>', methods=['POST'])
def invoke_workflow(workflow_name):
    """
    HTTP endpoint that receives workflow invocation requests.
    """
    try:
        request_data = request.get_json()
        params = request_data.get('params', {})
        is_async = request_data.get('Async', False)
        
        # Generate unique job directory if not provided
        job_dir = params.get('dir')
        if not job_dir or job_dir == "example":
            job_dir = f"job-{uuid.uuid4()}"
        
        # Prepare MQTT message
        search_str = params.get('find_str', 'speech')
        mqtt_message = {
            "dir": job_dir,
            "find_str": search_str,
            "timestamps": {
                "gateway_received": time.time()
            }
        }
        
        # Track this request if synchronous
        if not is_async:
            with results_lock:
                pending_requests[job_dir] = {
                    'status': 'pending',
                    'result': None,
                    'error': None,
                    'created_at': time.time()
                }
        
        # Publish to MQTT
        print(f"[Gateway] Triggering workflow '{workflow_name}' with job: {job_dir}, find_str: {search_str}")
        mqtt_client.publish(
            MQTT_TRIGGER_TOPIC,
            json.dumps(mqtt_message),
            qos=1
        )
        
        # For async requests, return immediately
        if is_async:
            return jsonify({
                "status": "accepted",
                "job_id": job_dir,
                "message": "Workflow triggered asynchronously"
            }), 202
        
        # For sync requests, wait for result (with timeout)
        timeout = 7200  # 2 hours
        start_time = time.time()
        
        while (time.time() - start_time) < timeout:
            with results_lock:
                request_info = pending_requests.get(job_dir)
                
                if request_info and request_info['status'] == 'completed':
                    result = request_info['result']
                    # Clean up
                    del pending_requests[job_dir]
                    
                    # Calculate duration
                    gateway_end = time.time()
                    gateway_start = mqtt_message["timestamps"]["gateway_received"]
                    
                    return jsonify({
                        "status": "success",
                        "job_id": job_dir,
                        "result": result,
                        "Reports": {
                            "Gateway": {
                                "Duration": gateway_end - gateway_start
                            }
                        }
                    }), 200
                
                elif request_info and request_info['status'] == 'failed':
                    error = request_info['error']
                    # Clean up
                    del pending_requests[job_dir]
                    
                    return jsonify({
                        "status": "error",
                        "job_id": job_dir,
                        "error": error
                    }), 500
            
            # Wait a bit before checking again
            time.sleep(0.1)
        
        # Timeout occurred
        with results_lock:
            if job_dir in pending_requests:
                del pending_requests[job_dir]
        
        return jsonify({
            "status": "timeout",
            "job_id": job_dir,
            "message": f"Workflow execution timed out after {timeout:.0f}s"
        }), 504
        
    except Exception as e:
        print(f"[Gateway] Error: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

if __name__ == '__main__':
    print("[Gateway] Starting MQTT Gateway API")
    start_mqtt()
    # Give MQTT client time to connect
    time.sleep(2)
    print("[Gateway] Starting HTTP server on port 8080")
    app.run(host='0.0.0.0', port=8080, debug=False)

from flask import Flask, request, jsonify
from datetime import datetime
import requests
import schedule
import random
import string
import time
import threading

app = Flask(__name__)

BASE_URL = "http://127.0.0.1:8080"
EXECUTION_STATE_CONTAINER = "/cse-in/NoiseCancellationSystem/ExecutionState"

# X-M2M-RI value
def generate_random_string(length=10):
    characters = string.ascii_lowercase + string.digits
    return ''.join(random.choices(characters, k=length))

def update_execution_state(state):
    payload = {
        "m2m:cin": {
            "con": state
        }
    }
    url = f"{BASE_URL}{EXECUTION_STATE_CONTAINER}"
    HEADERS = {
        "Content-Type": "application/json;ty=4",
        "X-M2M-Origin": "CAdmin",
        "X-M2M-RVI": "3",
        "X-M2M-RI": generate_random_string()
    }

    response = requests.post(url, headers=HEADERS, json=payload)
    if response.status_code == 201:
        print(f"ExecutionState updated to {state}")
    else:
        print(f"Failed to update ExecutionState: {response.status_code} - {response.text}")

def schedule_task(start_time, end_time):
    def start_job():
        print(f"Starting task at {datetime.now()}")
        update_execution_state("On")

    def stop_job():
        print(f"Stopping task at {datetime.now()}")
        update_execution_state("Off")

    schedule.every().day.at(start_time).do(start_job)
    schedule.every().day.at(end_time).do(stop_job)

    print(f"Scheduled daily tasks: start at {start_time}, stop at {end_time}")

def run_scheduler():
    """스케줄러 실행 루프"""
    while True:
        schedule.run_pending()
        time.sleep(1)

scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
scheduler_thread.start()

@app.route('/callback', methods=['POST'])
def callback():
    data = request.json
    try:
        schedule_data = data["m2m:sgn"]["nev"]["rep"]["m2m:cin"]["con"]
        print(f"Received new schedule: {schedule_data}")

        start_time, end_time = map(lambda x: x.strip(), schedule_data.split("-"))
        datetime.strptime(start_time, "%H:%M")  # 포맷 검증
        datetime.strptime(end_time, "%H:%M")    # 포맷 검증

        # 기존 스케줄 초기화 후 새 작업 등록
        schedule.clear()
        schedule_task(start_time, end_time)

        return jsonify({"status": "success"}), 200
    except Exception as e:
        print(f"Error processing callback: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(port=3000)

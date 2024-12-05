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
NOISE_AVERAGE_CONTAINER = "/cse-in/NoiseCancellationSystem/NoiseAverage"
NOISE_AVERAGE_LATEST = "/~/NCS-mn-cse/cse-mn/NoiseCancellationSystem/NoiseAverage/la"

# X-M2M-RI value
def generate_random_string(length=10):
    characters = string.ascii_lowercase + string.digits
    return ''.join(random.choices(characters, k=length))

def get_noise_average():
    """Retrieve the latest NoiseAverage value from MN-CSE."""
    url = f"{BASE_URL}{NOISE_AVERAGE_LATEST}"
    HEADERS = {
        "Content-Type": "application/json",
        "X-M2M-Origin": "CAdmin",
        "X-M2M-RVI": "3",
        "X-M2M-RI": generate_random_string()
    }

    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        try:
            data = response.json()
            noise_value = data["m2m:cin"]["con"]  # Retrieve the `con` field
            print(f"Retrieved NoiseAverage: {noise_value}")
            return noise_value
        except KeyError as e:
            print(f"Error parsing response: {e}")
            return None
    else:
        print(f"Failed to retrieve NoiseAverage: {response.status_code} - {response.text}")
        return None

def save_to_in_cse(noise_value):
    """Save the retrieved NoiseAverage value to IN-CSE."""
    payload = {
        "m2m:cin": {
            "con": noise_value
        }
    }
    url = f"{BASE_URL}{NOISE_AVERAGE_CONTAINER}"
    HEADERS = {
        "Content-Type": "application/json;ty=4",
        "X-M2M-Origin": "CAdmin",
        "X-M2M-RVI": "3",
        "X-M2M-RI": generate_random_string()
    }

    response = requests.post(url, headers=HEADERS, json=payload)
    if response.status_code == 201:
        print(f"Successfully saved NoiseAverage to IN-CSE: {noise_value}")
    else:
        print(f"Failed to save NoiseAverage to IN-CSE: {response.status_code} - {response.text}")


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

def update_noise_average():
    print(f"Running scheduled task at {datetime.now()}")
    noise_value = get_noise_average()
    if noise_value is not None:
        save_to_in_cse(noise_value)

def notify_schedule(start_time, end_time):
    def start_job():
        print(f"Starting task at {datetime.now()}")
        update_execution_state("On")

    def stop_job():
        print(f"Stopping task at {datetime.now()}")
        update_execution_state("Off")

    schedule.every().day.at(start_time).do(start_job)
    schedule.every().day.at(end_time).do(stop_job)

    print(f"Scheduled daily tasks: start at {start_time}, stop at {end_time}")

def sync_schedule():
    """Task to retrieve NoiseAverage from MN-CSE and save to IN-CSE."""
    # time = "15:50"
    # schedule.every().day.at(time).do(update_noise_average)
    # schedule.every(1).hours.do(update_noise_average)
    schedule.every(3).minutes.do(update_noise_average) # for demo
    # print(f"Scheduled task to run daily at {time}.")

@app.route('/callback', methods=['POST'])
def callback():
    data = request.json
    try:
        schedule_data = data["m2m:sgn"]["nev"]["rep"]["m2m:cin"]["con"]
        print(f"Received new schedule: {schedule_data}")

        start_time, end_time = map(lambda x: x.strip(), schedule_data.split("-"))
        datetime.strptime(start_time, "%H:%M")  # 포맷 검증
        datetime.strptime(end_time, "%H:%M")    # 포맷 검증

        notify_schedule(start_time, end_time)

        return jsonify({"status": "success"}), 200
    except Exception as e:
        print(f"Error processing callback: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    sync_schedule()

    app.run(port=3000)

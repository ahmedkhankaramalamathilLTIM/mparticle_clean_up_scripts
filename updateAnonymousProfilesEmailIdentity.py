import csv
import requests
import base64
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

# === Configuration ===
CSV_FILE = "anonymousprofiles.csv"
API_KEY = os.getenv("MPARTICLE_STG_API_KEY")
API_SECRET = os.getenv("MPARTICLE_STG_API_SECRET")
ENVIRONMENT = "development"
MAX_RETRIES = 3
THREAD_POOL_SIZE = 10  # Adjust based on rate limits and network conditions

# === Setup Logging ===
logging.basicConfig(
    filename="anonymous_profile_identity_update.log",
    filemode="a",
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO
)

# === Auth Header ===
#auth_header = base64.b64encode(f"{API_KEY}:{API_SECRET}".encode()).decode()
headers = {
    "Content-Type": "application/json"
}

# === API Call with Retry ===
def update_email_with_retry(mpid, email):
    url = f"https://identity.mparticle.com/v1/{mpid}/modify"
    payload = {
        "environment": ENVIRONMENT,
        "identity_changes": [
            {
                "old_value": "",
                "new_value": email,
                "identity_type": "email"
            }
        ]
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(url, json=payload,auth=(API_KEY, API_SECRET), headers=headers, timeout=10)
      
            if response.status_code == 202 or response.status_code == 200:
                logging.info(f"Response: {response.status_code} | SUCCESS | MPID: {mpid} | Email: {email}")
                return
            else:
                raise Exception(f"Status {response.status_code}: {response.text}")
        except Exception as e:
            logging.error(f"Attempt {attempt} failed for MPID: {mpid} | Email: {email} | Error: {e}")
            if attempt == MAX_RETRIES:
                logging.error(f"FAILED | MPID: {mpid} | Email: {email} | Error: {e}")
            else:
                backoff = 2 ** attempt
                time.sleep(backoff)

# === Main Runner with Parallel Execution ===
def process_csv_in_parallel():
    with open(CSV_FILE, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        tasks = []
        with ThreadPoolExecutor(max_workers=THREAD_POOL_SIZE) as executor:
            for row in reader:
                mpid = row["mpid"]
                email = row["email"]
                tasks.append(executor.submit(update_email_with_retry, mpid, email))

            for future in as_completed(tasks):
                _ = future.result()  # just to raise exceptions if any uncaught

if __name__ == "__main__":
    process_csv_in_parallel()

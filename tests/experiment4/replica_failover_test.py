import json
import time
import urllib.request
import threading
from concurrent.futures import ThreadPoolExecutor

GATEWAY = "http://localhost:5000"
ACCOUNT_ID = "A0001"

DURATION_SECONDS = 60
WORKERS = 4
DELAY_SECONDS = 0.25

stop_event = threading.Event()
print_lock = threading.Lock()
request_count = 0


def get_balance(worker_id):
    global request_count

    url = f"{GATEWAY}/accounts/{ACCOUNT_ID}/balance"
    deadline = time.monotonic() + DURATION_SECONDS

    while not stop_event.is_set() and time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=5) as response:
                result = json.loads(response.read().decode("utf-8"))

            status = response.status
            replica = result.get("replica", "unknown")
            balance = result.get("balance")

        except Exception as error:
            status = "FAILED"
            replica = "none"
            balance = str(error)

        with print_lock:
            request_count += 1
            print(
                f"Request {request_count}: "
                f"worker={worker_id}, status={status}, "
                f"replica={replica}, balance={balance}",
                flush=True
            )

        time.sleep(DELAY_SECONDS)


print("Continuous Gateway load test started.")
print(f"Running for {DURATION_SECONDS} seconds with {WORKERS} workers.")
print("Stop bank-a1 from another CMD window while this test is running.")

with ThreadPoolExecutor(max_workers=WORKERS) as executor:
    futures = [
        executor.submit(get_balance, worker_id)
        for worker_id in range(1, WORKERS + 1)
    ]

    for future in futures:
        future.result()

print("\nLoad test completed.")

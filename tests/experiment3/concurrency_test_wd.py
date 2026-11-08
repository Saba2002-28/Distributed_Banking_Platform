import json
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

GATEWAY = "http://localhost:5000"
ACCOUNT_ID = "A0001"
AMOUNT = 1
NUMBER_OF_EACH_OPERATION = 20


def get_balance():
    url = f"{GATEWAY}/accounts/{ACCOUNT_ID}/balance"

    with urllib.request.urlopen(url) as response:
        result = json.loads(response.read().decode("utf-8"))

    return result


def make_operation(operation, number):
    url = f"{GATEWAY}/accounts/{ACCOUNT_ID}/{operation}"

    data = json.dumps({"amount": AMOUNT}).encode("utf-8")

    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    with urllib.request.urlopen(request) as response:
        result = json.loads(response.read().decode("utf-8"))

    return operation, result


print("Checking initial balance...")
initial_result = get_balance()
print("Initial balance response:", initial_result)

operations = (
    ["deposit"] * NUMBER_OF_EACH_OPERATION
    + ["withdraw"] * NUMBER_OF_EACH_OPERATION
)

successful = {"deposit": 0, "withdraw": 0}
failed = 0

print("\nStarting concurrent deposits and withdrawals...")
print("Requests:", len(operations))
print("Workers: 10")

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [
        executor.submit(make_operation, operation, number)
        for number, operation in enumerate(operations)
    ]

    for future in as_completed(futures):
        try:
            operation, result = future.result()
            successful[operation] += 1

            print(
                f"{operation} succeeded through "
                f"{result.get('replica', 'unknown replica')}: "
                f"new balance = {result.get('new_balance', 'not returned')}"
            )

        except Exception as error:
            failed += 1
            print("Request failed:", error)

print("\nChecking final balance...")
final_result = get_balance()
print("Final balance response:", final_result)

print("\n========== TEST RESULTS ==========")
print("Successful deposits:", successful["deposit"])
print("Successful withdrawals:", successful["withdraw"])
print("Failed requests:", failed)
print("Initial balance:", initial_result)
print("Final balance:", final_result)

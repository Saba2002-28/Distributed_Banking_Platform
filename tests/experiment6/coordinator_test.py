import json
import subprocess
import time
import urllib.request
import urllib.error

GATEWAY_URL = "http://localhost:5000"
COORDINATOR_URL = "http://localhost:5005"

FROM_ACCOUNT = "A0500"
TO_ACCOUNT = "A1500"
AMOUNT = 100.00

MARKER_PATH = "/tmp/experiment6-paused"


def get_json(url):
    with urllib.request.urlopen(url, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def get_balance(account_id):
    result = get_json(
        f"{GATEWAY_URL}/accounts/{account_id}/balance"
    )
    return float(result["balance"])


def marker_exists():
    result = subprocess.run(
        [
            "docker", "compose", "exec", "-T", "coordinator",
            "sh", "-c", f"test -f {MARKER_PATH}"
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    return result.returncode == 0


def read_marker():
    result = subprocess.run(
        [
            "docker", "compose", "exec", "-T", "coordinator",
            "sh", "-c", f"cat {MARKER_PATH}"
        ],
        capture_output=True,
        text=True,
        check=True
    )
    return result.stdout.strip()


def main():
    print("Checking starting balances...")
    from_before = get_balance(FROM_ACCOUNT)
    to_before = get_balance(TO_ACCOUNT)

    print(f"{FROM_ACCOUNT} before: {from_before:.2f}")
    print(f"{TO_ACCOUNT} before: {to_before:.2f}")

    if from_before < AMOUNT:
        raise RuntimeError("The source account does not have enough funds.")

    print("\nStarting cross-shard transfer in the background...")

    transfer_payload = json.dumps({
        "from_account_id": FROM_ACCOUNT,
        "to_account_id": TO_ACCOUNT,
        "amount": AMOUNT
    }).encode("utf-8")

    request = urllib.request.Request(
        f"{GATEWAY_URL}/accounts/transfer",
        data=transfer_payload,
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    # This request should remain waiting while the Coordinator is paused.
    import threading

    def send_transfer():
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                print("Transfer response:", response.read().decode("utf-8"))
        except Exception as exc:
            print("Transfer request ended:", exc)

    transfer_thread = threading.Thread(
        target=send_transfer,
        daemon=True
    )
    transfer_thread.start()

    print("Waiting for the Coordinator's pause marker...")
    deadline = time.time() + 45

    while time.time() < deadline:
        if marker_exists():
            transaction_id = read_marker()
            print("\nCoordinator paused after first COMMIT acknowledgement.")
            print("Transaction ID:", transaction_id)
            print("Now stop and restart the Coordinator using the next instructions.")
            return

        time.sleep(0.5)

    raise TimeoutError(
        "Pause marker was not found. Check the Coordinator logs and API route."
    )


if __name__ == "__main__":
    main()

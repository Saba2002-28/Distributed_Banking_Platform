
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

GATEWAY_URL = "http://localhost:5000"

# Eight concurrent cross-shard transfers.
# The accounts overlap, and transfers go in both directions.
TRANSFERS = [
    {"from_account_id": "A0500", "to_account_id": "A1500", "amount": 10.00},
    {"from_account_id": "A1500", "to_account_id": "A0500", "amount": 5.00},

    {"from_account_id": "A0500", "to_account_id": "A1500", "amount": 7.00},
    {"from_account_id": "A1500", "to_account_id": "A0500", "amount": 3.00},

    {"from_account_id": "A0500", "to_account_id": "A1500", "amount": 12.00},
    {"from_account_id": "A1500", "to_account_id": "A0500", "amount": 8.00},

    {"from_account_id": "A0500", "to_account_id": "A1500", "amount": 4.00},
    {"from_account_id": "A1500", "to_account_id": "A0500", "amount": 6.00},
]


def send_transfer(transfer):
    request = Request(
        f"{GATEWAY_URL}/accounts/transfer",
        data=json.dumps(transfer).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urlopen(request, timeout=60) as response:
            body = response.read().decode("utf-8")
            return response.status, json.loads(body), transfer

    except HTTPError as error:
        body = error.read().decode("utf-8")
        try:
            result = json.loads(body)
        except json.JSONDecodeError:
            result = {"error": body}
        return error.code, result, transfer

    except (URLError, TimeoutError, OSError) as error:
        return 0, {"error": str(error)}, transfer


if __name__ == "__main__":
    print(f"Sending {len(TRANSFERS)} cross-shard transfers concurrently...\n")

    results = []

    with ThreadPoolExecutor(max_workers=len(TRANSFERS)) as executor:
        futures = [
            executor.submit(send_transfer, transfer)
            for transfer in TRANSFERS
        ]

        for future in as_completed(futures):
            status, result, transfer = future.result()
            results.append((status, result, transfer))

            print(f"Transfer: {transfer}")
            print(f"HTTP status: {status}")
            print(json.dumps(result, indent=2))
            print("-" * 50)

    successful = sum(
        1
        for status, result, _ in results
        if status == 200 and result.get("success") is True
    )

    print("\n========== SUMMARY ==========")
    print(f"Total transfers: {len(TRANSFERS)}")
    print(f"Successful: {successful}")
    print(f"Not successful: {len(TRANSFERS) - successful}")

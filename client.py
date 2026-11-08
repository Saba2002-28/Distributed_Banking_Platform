
import json
from decimal import Decimal, InvalidOperation
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

GATEWAY_URL = "http://localhost:5000"

ACCOUNTS = {
    "Shard A": [
        "A0001",
        "A0002",
        "A0003",
        "A0004",
        "A0100",
        "A0250",
        "A0500",
        "A0750",
        "A0999",
        "A1000",
    ],
    "Shard B": [
        "A1001",
        "A1002",
        "A1003",
        "A1004",
        "A1100",
        "A1250",
        "A1500",
        "A1750",
        "A1999",
        "A2000",
    ],
}


def call_gateway(path, method="GET", body=None):
    """Send a request to the Gateway and return its JSON response."""
    url = f"{GATEWAY_URL}{path}"

    data = None
    headers = {}

    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"

    outgoing_request = Request(
        url,
        data=data,
        headers=headers,
        method=method,
    )

    try:
        with urlopen(outgoing_request, timeout=35) as response:
            response_body = response.read().decode("utf-8")
            return response.status, json.loads(response_body)

    except HTTPError as error:
        response_body = error.read().decode("utf-8")

        try:
            result = json.loads(response_body)
        except json.JSONDecodeError:
            result = {"error": response_body}

        return error.code, result

    except (URLError, TimeoutError, OSError) as error:
        return 0, {
            "error": f"Could not connect to the Gateway: {error}"
        }

    except json.JSONDecodeError:
        return 0, {
            "error": "The Gateway returned an invalid response."
        }


def show_accounts():
    """Display the available accounts."""
    print("\nAvailable accounts:")

    for shard, accounts in ACCOUNTS.items():
        print(f"\n{shard}:")

        for account_id in accounts:
            print(f"  {account_id}")


def choose_account(prompt):
    """Ask the user to select one of the available accounts."""
    all_accounts = [
        account
        for accounts in ACCOUNTS.values()
        for account in accounts
    ]

    while True:
        account_id = input(prompt).strip().upper()

        if account_id in all_accounts:
            return account_id

        print("Invalid account ID. Please choose an account from the list.")


def get_amount():
    """Read and validate a positive monetary amount."""
    while True:
        amount_text = input("Enter amount: ").strip()

        try:
            amount = Decimal(amount_text)

            if not amount.is_finite() or amount <= 0:
                print("Amount must be a positive number.")
                continue

            if amount.as_tuple().exponent < -2:
                print("Amount can have at most two decimal places.")
                continue

            return float(amount)

        except InvalidOperation:
            print("Invalid amount. Please enter a number, such as 25 or 25.50.")


def show_result(status, result):
    """Display the Gateway response."""
    print(f"\nHTTP status: {status}")

    if status == 0:
        print(result.get("error", "Request failed."))
    elif 200 <= status < 300:
        print("Operation successful!")
        print(json.dumps(result, indent=2))
    else:
        print("Operation failed.")
        print(json.dumps(result, indent=2))


def check_balance():
    account_id = choose_account("Enter account ID: ")

    status, result = call_gateway(
        f"/accounts/{account_id}/balance"
    )

    show_result(status, result)


def deposit():
    account_id = choose_account("Enter account ID: ")
    amount = get_amount()

    status, result = call_gateway(
        f"/accounts/{account_id}/deposit",
        method="POST",
        body={"amount": amount},
    )

    show_result(status, result)


def withdraw():
    account_id = choose_account("Enter account ID: ")
    amount = get_amount()

    status, result = call_gateway(
        f"/accounts/{account_id}/withdraw",
        method="POST",
        body={"amount": amount},
    )

    show_result(status, result)


def transfer():
    print("\nSelect the source and destination accounts.")
    show_accounts()

    from_account_id = choose_account("From account ID: ")
    to_account_id = choose_account("To account ID: ")

    if from_account_id == to_account_id:
        print("Source and destination accounts must be different.")
        return

    amount = get_amount()

    status, result = call_gateway(
        "/accounts/transfer",
        method="POST",
        body={
            "from_account_id": from_account_id,
            "to_account_id": to_account_id,
            "amount": amount,
        },
    )

    show_result(status, result)


def main():
    while True:
        print("\n" + "=" * 45)
        print("       DISTRIBUTED BANKING CLIENT")
        print("=" * 45)

        print("1. Check balance")
        print("2. Deposit")
        print("3. Withdraw")
        print("4. Transfer")
        print("5. Show accounts")
        print("6. Exit")

        choice = input("\nChoose an operation: ").strip()

        if choice == "1":
            check_balance()
        elif choice == "2":
            deposit()
        elif choice == "3":
            withdraw()
        elif choice == "4":
            transfer()
        elif choice == "5":
            show_accounts()
        elif choice == "6":
            print("Thank you for using the banking client!")
            break
        else:
            print("Invalid choice. Please enter a number from 1 to 6.")


if __name__ == "__main__":
    main()

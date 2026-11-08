from flask import Flask, jsonify, request
import os

from .operations import (
    get_balance,
    deposit,
    withdraw,
    transfer,
    prepare_transfer,
    decide_prepared_transfer,
    InvalidAccountID,
    InvalidAmount,
    AccountNotFound,
    InsufficientFunds
)

app = Flask(__name__)

# Identifies the current application replica (A1, A2, B1, or B2).
REPLICA_ID = os.getenv("REPLICA_ID", "UNKNOWN")


@app.get("/health")
def health():
    return jsonify({
        "status": "healthy",
        "replica": REPLICA_ID
    }), 200


@app.get("/accounts/<account_id>/balance")
def balance(account_id):
    try:
        current_balance = get_balance(account_id)

        return jsonify({
            "account_id": account_id,
            "balance": str(current_balance),
            "replica": REPLICA_ID
        }), 200

    except (InvalidAccountID, AccountNotFound) as error:
        return jsonify({"error": str(error)}), 404


@app.post("/accounts/<account_id>/deposit")
def make_deposit(account_id):
    try:
        data = request.get_json()

        if not isinstance(data, dict) or "amount" not in data:
            raise InvalidAmount("A valid amount is required.")

        new_balance = deposit(account_id, data["amount"])

        return jsonify({
            "message": "Deposit successful.",
            "account_id": account_id,
            "new_balance": str(new_balance),
            "replica": REPLICA_ID
        }), 200

    except InvalidAccountID as error:
        return jsonify({"error": str(error)}), 400

    except (InvalidAmount, AccountNotFound) as error:
        return jsonify({"error": str(error)}), 400


@app.post("/accounts/<account_id>/withdraw")
def make_withdrawal(account_id):
    try:
        data = request.get_json()

        if not isinstance(data, dict) or "amount" not in data:
            raise InvalidAmount("A valid amount is required.")

        new_balance = withdraw(account_id, data["amount"])

        return jsonify({
            "message": "Withdrawal successful.",
            "account_id": account_id,
            "new_balance": str(new_balance),
            "replica": REPLICA_ID
        }), 200

    except InvalidAccountID as error:
        return jsonify({"error": str(error)}), 400

    except (InvalidAmount, AccountNotFound, InsufficientFunds) as error:
        return jsonify({"error": str(error)}), 400

@app.post("/accounts/transfer")
def make_transfer():
    try:
        data = request.get_json()

        if (
            not isinstance(data, dict)
            or "from_account_id" not in data
            or "to_account_id" not in data
            or "amount" not in data
        ):
            raise InvalidAmount(
                "from_account_id, to_account_id, and amount are required."
            )

        result = transfer(
            data["from_account_id"],
            data["to_account_id"],
            data["amount"]
        )

        return jsonify({
            "message": "Transfer successful.",
            "from_account_id": data["from_account_id"],
            "to_account_id": data["to_account_id"],
            "amount": str(data["amount"]),
            "from_balance": str(result["from_balance"]),
            "to_balance": str(result["to_balance"]),
            "replica": REPLICA_ID
        }), 200

    except InvalidAccountID as error:
        return jsonify({"error": str(error)}), 400

    except (
        InvalidAmount,
        AccountNotFound,
        InsufficientFunds
    ) as error:
        return jsonify({"error": str(error)}), 400

@app.post("/transactions/prepare")
def prepare_transaction():
    try:
        data = request.get_json()

        required_fields = (
            "transaction_id",
            "from_account_id",
            "to_account_id",
            "amount",
            "participant_shard"
        )

        if not isinstance(data, dict) or any(
            field not in data for field in required_fields
        ):
            return jsonify({
                "error": "All transaction fields are required."
            }), 400

        result = prepare_transfer(
            data["transaction_id"],
            data["from_account_id"],
            data["to_account_id"],
            data["amount"],
            data["participant_shard"]
        )

        return jsonify(result), 200

    except InsufficientFunds as error:
        return jsonify({
            "vote": "NO",
            "error": str(error)
        }), 200

    except (
        InvalidAccountID,
        InvalidAmount,
        AccountNotFound,
        ValueError
    ) as error:
        return jsonify({
            "vote": "NO",
            "error": str(error)
        }), 200

    except Exception as error:
        app.logger.exception("Prepare operation failed")
        return jsonify({
            "vote": "NO",
            "error": "Prepare operation failed."
        }), 500



@app.post("/transactions/decision")
def decide_transaction():
    try:
        data = request.get_json()

        required_fields = (
            "transaction_id",
            "participant_shard",
            "decision"
        )

        if not isinstance(data, dict) or any(
            field not in data for field in required_fields
        ):
            return jsonify({
                "error": "transaction_id, participant_shard, and decision are required."
            }), 400

        result = decide_prepared_transfer(
            data["transaction_id"],
            data["participant_shard"],
            data["decision"]
        )

        return jsonify(result), 200

    except ValueError as error:
        return jsonify({"error": str(error)}), 400

    except Exception:
        app.logger.exception("Decision operation failed")
        return jsonify({
            "error": "Decision operation failed."
        }), 500


if __name__ == "__main__":

    app.run(host="0.0.0.0", port=5000)

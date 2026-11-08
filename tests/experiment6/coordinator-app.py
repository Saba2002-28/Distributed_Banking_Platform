from flask import Flask, jsonify, request
import os
import uuid
import requests
import psycopg
import threading
import time

app = Flask(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")

# Internal Docker Compose addresses.
SHARD_REPLICAS = {
    "shard_a": ["http://bank-a1:5000", "http://bank-a2:5000"],
    "shard_b": ["http://bank-b1:5000", "http://bank-b2:5000"],
}


def get_db_connection():
    return psycopg.connect(DATABASE_URL)


def get_shard(account_id):
    """Return the owning shard for a valid account ID."""
    if not isinstance(account_id, str):
        raise ValueError("Account ID must be a string.")

    if len(account_id) != 5 or not account_id.startswith("A"):
        raise ValueError("Account ID must look like A0001.")

    try:
        number = int(account_id[1:])
    except ValueError:
        raise ValueError("Account ID must contain digits after A.")

    if 1 <= number <= 1000:
        return "shard_a"

    if 1001 <= number <= 2000:
        return "shard_b"

    raise ValueError("Account ID is outside the supported range.")


def send_prepare(shard_id, payload):
    """Try the shard's replicas until one responds."""
    for replica_url in SHARD_REPLICAS[shard_id]:
        try:
            response = requests.post(
                f"{replica_url}/transactions/prepare",
                json=payload,
                timeout=20
            )

            if response.status_code == 200:
                return response.json()

        except requests.RequestException:
            app.logger.warning(
                "Could not reach replica %s",
                replica_url
            )

    return {
        "vote": "NO",
        "error": f"No available replica for {shard_id}."
    }


def send_decision(shard_id, transaction_id, decision):
    """Send a COMMIT or ABORT decision to a shard participant."""
    payload = {
        "transaction_id": transaction_id,
        "participant_shard": shard_id,
        "decision": decision
    }

    for replica_url in SHARD_REPLICAS[shard_id]:
        try:
            response = requests.post(
                f"{replica_url}/transactions/decision",
                json=payload,
                timeout=20
            )

            if response.status_code == 200:
                return response.json()

            app.logger.warning(
                "Replica %s rejected %s: %s",
                replica_url,
                decision,
                response.text
            )

        except requests.RequestException:
            app.logger.warning(
                "Could not reach replica %s to send %s",
                replica_url,
                decision
            )

    return {
        "decision": decision,
        "error": f"No available replica for {shard_id}."
    }


def recover_unresolved_transactions():
    """
    Retry durable COMMIT or ABORT decisions that are not yet completed.
    """

    ##################### EXPERIMENT 6 CHANGE #####################
    # Do not let the background recovery loop complete the transaction
    # while the experiment is deliberately paused after the first ACK.
    if (
        os.getenv("EXPERIMENT6_PAUSE_AFTER_FIRST_ACK") == "1"
        and os.path.exists("/tmp/experiment6-paused")
    ):
        app.logger.warning(
            "Experiment 6 pause marker exists; delaying recovery."
        )
        return
    ##################### END EXPERIMENT 6 CHANGE #####################

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    transaction_id,
                    status,
                    from_account_id,
                    to_account_id
                FROM distributed_transactions
                WHERE status IN ('COMMIT_DECIDED', 'ABORT_DECIDED')
                """
            )

            unresolved_transactions = cur.fetchall()

    for transaction in unresolved_transactions:
        transaction_id = str(transaction[0])
        status = transaction[1]
        from_account_id = transaction[2]
        to_account_id = transaction[3]

        decision = (
            "COMMIT" if status == "COMMIT_DECIDED" else "ABORT"
        )

        from_shard = get_shard(from_account_id)
        to_shard = get_shard(to_account_id)

        acknowledgements = {}

        for shard_id in (from_shard, to_shard):
            acknowledgements[shard_id] = send_decision(
                shard_id,
                transaction_id,
                decision
            )

        all_confirmed = all(
            acknowledgement.get("decision") == decision
            and "error" not in acknowledgement
            for acknowledgement in acknowledgements.values()
        )

        if all_confirmed:
            final_status = "COMPLETED"

            participant_status = (
                "COMMITTED" if decision == "COMMIT" else "ABORTED"
            )

            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE transaction_participants
                        SET status = %s, updated_at = NOW()
                        WHERE transaction_id = %s
                        """,
                        (participant_status, transaction_id)
                    )

                    cur.execute(
                        """
                        UPDATE distributed_transactions
                        SET status = %s, updated_at = NOW()
                        WHERE transaction_id = %s
                        """,
                        (final_status, transaction_id)
                    )

            app.logger.info(
                "Recovered transaction %s with decision %s",
                transaction_id,
                decision
            )


def recovery_loop():
    """
    Periodically retry unresolved durable decisions.
    """
    while True:
        try:
            recover_unresolved_transactions()

        except Exception:
            app.logger.exception(
                "An error occurred during transaction recovery."
            )

        time.sleep(5)


@app.get("/transactions/<transaction_id>")
def get_transaction_status(transaction_id):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    transaction_id,
                    status,
                    from_account_id,
                    to_account_id,
                    amount,
                    created_at,
                    updated_at
                FROM distributed_transactions
                WHERE transaction_id = %s
                """,
                (transaction_id,)
            )

            transaction = cur.fetchone()

    if transaction is None:
        return jsonify({
            "error": "Transaction not found",
            "transaction_id": transaction_id
        }), 404

    return jsonify({
        "transaction_id": str(transaction[0]),
        "status": transaction[1],
        "from_account_id": transaction[2],
        "to_account_id": transaction[3],
        "amount": str(transaction[4]),
        "created_at": transaction[5].isoformat(),
        "updated_at": transaction[6].isoformat()
    }), 200


@app.get("/health")
def health():
    return jsonify({
        "status": "healthy",
        "service": "coordinator"
    }), 200


@app.post("/transactions")
def create_transaction():
    data = request.get_json(silent=True)

    if not isinstance(data, dict):
        return jsonify({"error": "A JSON request body is required."}), 400

    required_fields = (
        "from_account_id",
        "to_account_id",
        "amount"
    )

    if any(field not in data for field in required_fields):
        return jsonify({
            "error": "from_account_id, to_account_id, and amount are required."
        }), 400

    from_account_id = data["from_account_id"]
    to_account_id = data["to_account_id"]
    amount = data["amount"]

    try:
        from_shard = get_shard(from_account_id)
        to_shard = get_shard(to_account_id)
    except ValueError as error:
        return jsonify({"error": str(error)}), 400

    if from_shard == to_shard:
        return jsonify({
            "error": "This endpoint is for cross-shard transfers only."
        }), 400

    transaction_id = str(uuid.uuid4())

    # Save the transaction and participant records before sending PREPARE.
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO distributed_transactions (
                    transaction_id,
                    status,
                    from_account_id,
                    to_account_id,
                    amount
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    transaction_id,
                    "PREPARING",
                    from_account_id,
                    to_account_id,
                    amount
                )
            )

            cur.execute(
                """
                INSERT INTO transaction_participants (
                    transaction_id,
                    shard_id,
                    status
                )
                VALUES
                    (%s, %s, %s),
                    (%s, %s, %s)
                """,
                (
                    transaction_id, from_shard, "PREPARING",
                    transaction_id, to_shard, "PREPARING"
                )
            )

    # Each participant prepares its local part of the transfer.
    votes = {}

    for shard_id in sorted((from_shard, to_shard)):
        payload = {
            "transaction_id": transaction_id,
            "from_account_id": from_account_id,
            "to_account_id": to_account_id,
            "amount": amount,
            "participant_shard": shard_id
        }

        votes[shard_id] = send_prepare(shard_id, payload)

    # Record the votes.
    all_yes = all(
        vote_result.get("vote") == "YES"
        for vote_result in votes.values()
    )

    # The decision must be durable before it is sent to participants.
    decision = "COMMIT" if all_yes else "ABORT"

    decision_status = (
        "COMMIT_DECIDED" if all_yes else "ABORT_DECIDED"
    )

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for shard_id, vote_result in votes.items():
                cur.execute(
                    """
                    UPDATE transaction_participants
                    SET status = %s, updated_at = NOW()
                    WHERE transaction_id = %s AND shard_id = %s
                    """,
                    (
                        vote_result.get("vote", "NO"),
                        transaction_id,
                        shard_id
                    )
                )

            cur.execute(
                """
                UPDATE distributed_transactions
                SET status = %s, updated_at = NOW()
                WHERE transaction_id = %s
                """,
                (decision_status, transaction_id)
            )

    # Only after the decision is saved do we send it to participants.
    acknowledgements = {}

    ##################### EXPERIMENT 6 CHANGE #####################
    # Enumerate the loop so we know whether this is the first participant.
    # Pause immediately after its successful COMMIT acknowledgement,
    # before sending the decision to the second participant.
    for index, shard_id in enumerate((from_shard, to_shard)):
        acknowledgements[shard_id] = send_decision(
            shard_id,
            transaction_id,
            decision
        )

        if (
            os.getenv("EXPERIMENT6_PAUSE_AFTER_FIRST_ACK") == "1"
            and decision == "COMMIT"
            and index == 0
            and acknowledgements[shard_id].get("decision") == "COMMIT"
            and "error" not in acknowledgements[shard_id]
        ):
            app.logger.warning(
                "EXPERIMENT6_PAUSED_AFTER_FIRST_ACK transaction_id=%s",
                transaction_id
            )

            with open("/tmp/experiment6-paused", "w") as marker:
                marker.write(transaction_id)

            while True:
                time.sleep(1)
    ##################### END EXPERIMENT 6 CHANGE #####################

    all_confirmed = all(
        acknowledgement.get("decision") == decision
        and "error" not in acknowledgement
        for acknowledgement in acknowledgements.values()
    )

    if all_confirmed:
        final_status = "COMPLETED"

        participant_status = (
            "COMMITTED" if decision == "COMMIT" else "ABORTED"
        )

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE transaction_participants
                    SET status = %s, updated_at = NOW()
                    WHERE transaction_id = %s
                    """,
                    (participant_status, transaction_id)
                )

                cur.execute(
                    """
                    UPDATE distributed_transactions
                    SET status = %s, updated_at = NOW()
                    WHERE transaction_id = %s
                    """,
                    (final_status, transaction_id)
                )
    else:
        # Keep the durable decision unresolved so recovery can retry it.
        final_status = decision_status

    http_status = 200 if all_confirmed and decision == "COMMIT" else (
        200 if all_confirmed else 202
    )

    return jsonify({
        "transaction_id": transaction_id,
        "decision": decision,
        "status": final_status,
        "votes": votes,
        "acknowledgements": acknowledgements,
        "success": all_confirmed and decision == "COMMIT"
    }), http_status


if __name__ == "__main__":

    recovery_thread = threading.Thread(
        target=recovery_loop,
        daemon=True
    )

    recovery_thread.start()

    app.run(host="0.0.0.0", port=5000)
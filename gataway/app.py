
from flask import Flask, jsonify, request
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from urllib.parse import urlencode
from threading import Lock
import itertools
import json
import logging
import os

from shard_router import get_shard_for_account, InvalidAccountID


app = Flask(__name__)

COORDINATOR_URL = os.getenv(
    "COORDINATOR_URL",
    "http://coordinator:5000"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

# The replica URLs are internal Docker Compose service names.
REPLICAS = {
    "A": ["http://bank-a1:5000", "http://bank-a2:5000"],
    "B": ["http://bank-b1:5000", "http://bank-b2:5000"],
}

# One round-robin counter per shard.
ROUND_ROBIN = {
    shard: itertools.count()
    for shard in REPLICAS
}

ROUND_ROBIN_LOCK = Lock()


def select_replica(shard):
    """Return the next healthy replica for the requested shard."""
    replicas = REPLICAS[shard]

    with ROUND_ROBIN_LOCK:
        start_index = next(ROUND_ROBIN[shard]) % len(replicas)

    # Try every replica, starting from the round-robin position.
    for offset in range(len(replicas)):
        index = (start_index + offset) % len(replicas)
        replica_url = replicas[index]

        try:
            with urlopen(
                f"{replica_url}/health",
                timeout=2
            ) as response:
                if response.status == 200:
                    return replica_url

        except (URLError, HTTPError, TimeoutError, OSError):
            app.logger.warning(
                "Replica health check failed: %s",
                replica_url
            )

    return None


def forward_request(account_id, operation):
    """Route an account operation to a healthy replica."""
    try:
        shard = get_shard_for_account(account_id)
    except InvalidAccountID as error:
        return jsonify({"error": str(error)}), 400

    replica_url = select_replica(shard)

    if replica_url is None:
        app.logger.error("No healthy replicas available for shard %s", shard)
        return jsonify({
            "error": f"No healthy replicas available for shard {shard}"
        }), 503

    app.logger.info(
        "Routing account=%s shard=%s replica=%s operation=%s",
        account_id,
        shard,
        replica_url,
        operation
    )

    target_url = f"{replica_url}/accounts/{account_id}/{operation}"

    try:
        if operation == "balance":
            outgoing_request = Request(target_url, method="GET")

        else:
            body = request.get_json(silent=True)

            if not isinstance(body, dict):
                return jsonify({
                    "error": "A JSON object is required."
                }), 400

            outgoing_request = Request(
                target_url,
                data=json.dumps(body).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST"
            )

        with urlopen(outgoing_request, timeout=5) as response:
            response_body = response.read().decode("utf-8")
            return app.response_class(
                response=response_body,
                status=response.status,
                content_type="application/json"
            )

    except HTTPError as error:
        response_body = error.read().decode("utf-8")
        return app.response_class(
            response=response_body,
            status=error.code,
            content_type="application/json"
        )

    except (URLError, TimeoutError, OSError) as error:
        app.logger.error("Request to replica failed: %s", error)
        return jsonify({
            "error": "The selected replica could not process the request."
        }), 502


def forward_json_request(target_url, body):
    """Forward a JSON POST request and preserve the response."""
    outgoing_request = Request(
        target_url,
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    try:
        with urlopen(outgoing_request, timeout=30) as response:
            response_body = response.read().decode("utf-8")

            return app.response_class(
                response=response_body,
                status=response.status,
                content_type="application/json"
            )

    except HTTPError as error:
        response_body = error.read().decode("utf-8")

        return app.response_class(
            response=response_body,
            status=error.code,
            content_type="application/json"
        )

    except (URLError, TimeoutError, OSError) as error:
        app.logger.error(
            "Transfer request failed: %s",
            error
        )

        return jsonify({
            "error": "The transfer service could not be reached."
        }), 502


@app.post("/accounts/transfer")
def transfer():
    """Route local transfers to a shard and cross-shard to the Coordinator."""
    body = request.get_json(silent=True)

    if not isinstance(body, dict):
        return jsonify({
            "error": "A JSON object is required."
        }), 400

    required_fields = [
        "from_account_id",
        "to_account_id",
        "amount"
    ]

    if any(field not in body for field in required_fields):
        return jsonify({
            "error": "from_account_id, to_account_id, and amount are required."
        }), 400

    from_account_id = body["from_account_id"]
    to_account_id = body["to_account_id"]



    if from_account_id == to_account_id:
        return jsonify({
            "error": "Source and destination accounts must be different."
        }), 400

    try:
        from_shard = get_shard_for_account(from_account_id)
        to_shard = get_shard_for_account(to_account_id)

    except InvalidAccountID as error:
        return jsonify({"error": str(error)}), 400

    # Same-shard transfer: one atomic local database transaction.
    if from_shard == to_shard:
        replica_url = select_replica(from_shard)

        if replica_url is None:
            return jsonify({
                "error": f"No healthy replicas available for shard {from_shard}"
            }), 503

        app.logger.info(
            "Routing LOCAL transfer from=%s to=%s shard=%s replica=%s",
            from_account_id,
            to_account_id,
            from_shard,
            replica_url
        )

        return forward_json_request(
            f"{replica_url}/accounts/transfer",
            body
        )

    # Cross-shard transfer: Coordinator runs Two-Phase Commit.
    app.logger.info(
        "Routing CROSS-SHARD transfer from=%s shard=%s to=%s shard=%s coordinator=%s",
        from_account_id,
        from_shard,
        to_account_id,
        to_shard,
        COORDINATOR_URL
    )

    return forward_json_request(
        f"{COORDINATOR_URL}/transactions",
        body
    )



@app.get("/health")
def health():
    return jsonify({
        "status": "healthy",
        "service": "gateway"
    }), 200


@app.get("/accounts/<account_id>/balance")
def balance(account_id):
    return forward_request(account_id, "balance")


@app.post("/accounts/<account_id>/deposit")
def deposit(account_id):
    return forward_request(account_id, "deposit")


@app.post("/accounts/<account_id>/withdraw")
def withdraw(account_id):
    return forward_request(account_id, "withdraw")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

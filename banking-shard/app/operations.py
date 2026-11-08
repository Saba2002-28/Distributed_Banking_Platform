import re
from decimal import Decimal, InvalidOperation

from .database import get_db_connection, get_autocommit_connection
import uuid
from psycopg import sql


class InvalidAccountID(ValueError):
    pass


class InvalidAmount(ValueError):
    pass


class AccountNotFound(Exception):
    pass


class InsufficientFunds(Exception):
    pass


CENT = Decimal("0.01")

################## Validating accounts ###############################

def validate_account_id(account_id):
    if not isinstance(account_id, str):
        raise InvalidAccountID("Account ID must be a string.")

    if not re.fullmatch(r"A[0-9]{4}", account_id):
        raise InvalidAccountID("Invalid account ID format.")

    if not "A0001" <= account_id <= "A2000":
        raise InvalidAccountID("Account ID is outside the valid range.")

################ Checking the input #############################

def validate_amount(amount):
    if isinstance(amount, bool):
        raise InvalidAmount("Amount must be a positive number.")

    try:
        amount = Decimal(str(amount))
    except (InvalidOperation, ValueError):
        raise InvalidAmount("Invalid amount.")

    if not amount.is_finite() or amount <= 0:
        raise InvalidAmount("Amount must be greater than zero.")

    if amount != amount.quantize(CENT):
        raise InvalidAmount("Amount cannot have more than two decimal places.")

    return amount

######################## Getting balance ###############################

def get_balance(account_id):
    validate_account_id(account_id)

    with get_db_connection() as conn:
        result = conn.execute(
            """
            SELECT balance
            FROM accounts
            WHERE account_id = %s
            """,
            (account_id,)
        ).fetchone()

        if result is None:
            raise AccountNotFound("Account does not exist.")

        return result[0]

############################### Deposit ################################

def deposit(account_id, amount):
    validate_account_id(account_id)
    amount = validate_amount(amount)

    with get_db_connection() as conn:
        result = conn.execute(
            """
            SELECT balance
            FROM accounts
            WHERE account_id = %s
            FOR UPDATE
            """,
            (account_id,)
        ).fetchone()

        if result is None:
            raise AccountNotFound("Account does not exist.")

        conn.execute(
            """
            UPDATE accounts
            SET balance = balance + %s
            WHERE account_id = %s
            """,
            (amount, account_id)
        )

        return result[0] + amount

############################## Withdrawal #########################################

def withdraw(account_id, amount):
    validate_account_id(account_id)
    amount = validate_amount(amount)

    with get_db_connection() as conn:
        result = conn.execute(
            """
            SELECT balance
            FROM accounts
            WHERE account_id = %s
            FOR UPDATE
            """,
            (account_id,)
        ).fetchone()

        if result is None:
            raise AccountNotFound("Account does not exist.")

        current_balance = result[0]

        if current_balance < amount:
            raise InsufficientFunds("Insufficient account balance.")

        conn.execute(
            """
            UPDATE accounts
            SET balance = balance - %s
            WHERE account_id = %s
            """,
            (amount, account_id)
        )

        return current_balance - amount

########################### Same-shard transfer ####################################

def transfer(from_account_id, to_account_id, amount):
    validate_account_id(from_account_id)
    validate_account_id(to_account_id)
    amount = validate_amount(amount)

    if from_account_id == to_account_id:
        raise InvalidAccountID(
            "Source and destination accounts must be different."
        )

    # Both accounts must belong to the same shard.
    if (from_account_id <= "A1000") != (to_account_id <= "A1000"):
        raise InvalidAccountID(
            "A local transfer must use accounts from the same shard."
        )

    # Lock accounts in a consistent order to reduce deadlock risk.
    account_ids = sorted([from_account_id, to_account_id])

    with get_db_connection() as conn:
        rows = conn.execute(
            """
            SELECT account_id, balance
            FROM accounts
            WHERE account_id = ANY(%s)
            ORDER BY account_id
            FOR UPDATE
            """,
            (account_ids,)
        ).fetchall()

        if len(rows) != 2:
            raise AccountNotFound("One or both accounts do not exist.")

        balances = {
            account_id: balance
            for account_id, balance in rows
        }

        sender_balance = balances[from_account_id]

        if sender_balance < amount:
            raise InsufficientFunds("Insufficient account balance.")

        conn.execute(
            """
            UPDATE accounts
            SET balance = balance - %s
            WHERE account_id = %s
            """,
            (amount, from_account_id)
        )

        conn.execute(
            """
            UPDATE accounts
            SET balance = balance + %s
            WHERE account_id = %s
            """,
            (amount, to_account_id)
        )

        return {
            "from_balance": sender_balance - amount,
            "to_balance": balances[to_account_id] + amount
        }


############################## Cross-shard transfer ################################

def prepare_transfer(
    transaction_id,
    from_account_id,
    to_account_id,
    amount,
    participant_shard
):
    validate_account_id(from_account_id)
    validate_account_id(to_account_id)
    amount = validate_amount(amount)

    if from_account_id == to_account_id:
        raise InvalidAccountID(
            "Source and destination accounts must be different."
        )

    if participant_shard not in ("shard_a", "shard_b"):
        raise ValueError("Invalid participant shard.")

    transaction_uuid = str(uuid.UUID(str(transaction_id)))
    gid = f"{transaction_uuid}-{participant_shard}"

    # Determine ownership using the project's actual shard ranges.
    from_shard = (
        "shard_a" if from_account_id <= "A1000" else "shard_b"
    )
    to_shard = (
        "shard_a" if to_account_id <= "A1000" else "shard_b"
    )

    if from_shard == to_shard:
        raise ValueError("This is not a cross-shard transfer.")

    if participant_shard not in (from_shard, to_shard):
        raise ValueError(
            "Participant shard does not own either transfer account."
        )

    # Each participant handles only its own account.
    if participant_shard == from_shard:
        participant_account = from_account_id
        delta = -amount
    else:
        participant_account = to_account_id
        delta = amount

    lock_conn = get_autocommit_connection()
    conn = None
    prepared = False

    try:
        # Serialize operations for this transaction across replicas.
        lock_conn.execute(
            "SELECT pg_advisory_lock(hashtext(%s))",
            (transaction_uuid,)
        )

        # A final decision prevents a delayed PREPARE.
        decision_row = lock_conn.execute(
            """
            SELECT decision
            FROM participant_decisions
            WHERE transaction_id = %s
            """,
            (transaction_uuid,)
        ).fetchone()

        if decision_row is not None:
            raise ValueError(
                f"Transaction already has decision: {decision_row[0]}"
            )

        # Idempotent retry if this participant already prepared.
        prepared_row = lock_conn.execute(
            """
            SELECT gid
            FROM pg_prepared_xacts
            WHERE gid = %s
            """,
            (gid,)
        ).fetchone()

        if prepared_row is not None:
            return {
                "vote": "YES",
                "transaction_id": transaction_uuid,
                "participant_shard": participant_shard,
                "message": "Already prepared"
            }

        # Do not reuse a transaction ID that was previously resolved.
        existing_row = lock_conn.execute(
            """
            SELECT transaction_id
            FROM prepared_transfers
            WHERE transaction_id = %s
            """,
            (transaction_uuid,)
        ).fetchone()

        if existing_row is not None:
            raise ValueError("Transaction ID has already been used.")

        conn = get_db_connection()

        account_row = conn.execute(
            """
            SELECT balance
            FROM accounts
            WHERE account_id = %s
            FOR UPDATE
            """,
            (participant_account,)
        ).fetchone()

        if account_row is None:
            raise AccountNotFound(
                f"Account {participant_account} does not exist."
            )

        balance = account_row[0]

        if delta < 0 and balance < amount:
            raise InsufficientFunds("Insufficient account balance.")

        conn.execute(
            """
            UPDATE accounts
            SET balance = balance + %s
            WHERE account_id = %s
            """,
            (delta, participant_account)
        )

        conn.execute(
            """
            INSERT INTO prepared_transfers (
                transaction_id,
                from_account_id,
                to_account_id,
                amount,
                status
            )
            VALUES (%s, %s, %s, %s, 'PREPARED')
            """,
            (
                transaction_uuid,
                from_account_id,
                to_account_id,
                amount
            )
        )

        # Persist the participant's prepared state.
        conn.execute(
            sql.SQL("PREPARE TRANSACTION {}").format(
                sql.Literal(gid)
            )
        )

        prepared = True

        return {
            "vote": "YES",
            "transaction_id": transaction_uuid,
            "participant_shard": participant_shard,
            "message": "Prepared successfully"
        }

    except Exception:
        if conn is not None and not prepared:
            conn.rollback()
        raise

    finally:
        if conn is not None:
            conn.close()

        try:
            lock_conn.execute(
                "SELECT pg_advisory_unlock(hashtext(%s))",
                (transaction_uuid,)
            )
        finally:
            lock_conn.close()

################################ Cross-shard transfer prepare ####################################

def decide_prepared_transfer(
    transaction_id,
    participant_shard,
    decision
):
    if participant_shard not in ("shard_a", "shard_b"):
        raise ValueError("Invalid participant shard.")

    decision = decision.upper()

    if decision not in ("COMMIT", "ABORT"):
        raise ValueError("Decision must be COMMIT or ABORT.")

    transaction_uuid = str(uuid.UUID(str(transaction_id)))
    gid = f"{transaction_uuid}-{participant_shard}"

    conn = get_autocommit_connection()

    try:
        # Serialize decisions and PREPARE retries for this transaction.
        conn.execute(
            "SELECT pg_advisory_lock(hashtext(%s))",
            (transaction_uuid,)
        )

        existing_decision = conn.execute(
            """
            SELECT decision
            FROM participant_decisions
            WHERE transaction_id = %s
            """,
            (transaction_uuid,)
        ).fetchone()

        if existing_decision is not None:
            recorded_decision = existing_decision[0]

            if recorded_decision != decision:
                raise ValueError(
                    f"Conflicting decision: recorded "
                    f"{recorded_decision}, received {decision}."
                )

        prepared_row = conn.execute(
            """
            SELECT gid
            FROM pg_prepared_xacts
            WHERE gid = %s
            """,
            (gid,)
        ).fetchone()

        # COMMIT requires a prepared transaction, unless COMMIT
        # was already durably recorded by a previous request.
        if decision == "COMMIT":
            if existing_decision is None and prepared_row is None:
                raise ValueError(
                    "Cannot COMMIT: participant is not prepared."
                )

        # Record the decision durably before resolving the prepared TX.
        # For ABORT, this also prevents a delayed PREPARE.
        if existing_decision is None:
            conn.execute(
                """
                INSERT INTO participant_decisions (
                    transaction_id,
                    decision
                )
                VALUES (%s, %s)
                """,
                (transaction_uuid, decision)
            )

        if prepared_row is not None:
            if decision == "COMMIT":
                conn.execute(
                    sql.SQL("COMMIT PREPARED {}").format(
                        sql.Literal(gid)
                    )
                )
                message = "Committed successfully"

            else:
                conn.execute(
                    sql.SQL("ROLLBACK PREPARED {}").format(
                        sql.Literal(gid)
                    )
                )
                message = "Aborted successfully"

        else:
            # The decision is already durable, and no prepared
            # transaction remains to be resolved.
            if decision == "COMMIT":
                message = "Already committed"
            else:
                message = "Already aborted or never prepared"

        return {
            "transaction_id": transaction_uuid,
            "participant_shard": participant_shard,
            "decision": decision,
            "message": message
        }

    finally:
        try:
            conn.execute(
                "SELECT pg_advisory_unlock(hashtext(%s))",
                (transaction_uuid,)
            )
        finally:
            conn.close()


import json
import re
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
SHARD_MAP_PATH = PROJECT_ROOT / "config" / "shard-map.json"


class InvalidAccountID(ValueError):
    """Raised when an account ID is malformed or outside all shard ranges."""


def load_shard_map():
    with SHARD_MAP_PATH.open("r", encoding="utf-8") as file:
        return json.load(file)


def get_shard_for_account(account_id):
    if not isinstance(account_id, str):
        raise InvalidAccountID("Account ID must be a string.")

    if not re.fullmatch(r"A[0-9]{4}", account_id):
        raise InvalidAccountID(
            "Account ID must have the format A followed by four digits."
        )

    shard_map = load_shard_map()

    for shard_name, shard in shard_map["shards"].items():
        start, end = shard["account_range"]

        if start <= account_id <= end:
            return shard_name

    raise InvalidAccountID("Account ID is outside all configured shard ranges.")

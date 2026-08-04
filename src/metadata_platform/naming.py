"""Safe names for generated source code and deployment assets."""

import re

_SAFE_NAME = re.compile(r"^[a-z][a-z0-9_]{2,62}$")


def require_safe_name(value: str, field: str = "name") -> str:
    if not isinstance(value, str) or not _SAFE_NAME.fullmatch(value):
        raise ValueError(
            f"{field} must start with a lowercase letter and contain only "
            "lowercase letters, digits, or underscores (3-63 characters)"
        )
    return value


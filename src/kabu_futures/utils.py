from __future__ import annotations

from .models import Direction


def opposite(left: Direction, right: Direction) -> bool:
    """Return True if left and right are opposing trade directions."""
    return (left == "long" and right == "short") or (left == "short" and right == "long")

from dataclasses import dataclass
from typing import FrozenSet


@dataclass
class UserProfile:
    id: int
    first_name: str
    last_name: str
    generated_aliases: FrozenSet[str]
    aliases: FrozenSet[str]
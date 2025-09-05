from dataclasses import dataclass
from datetime import datetime
from typing import FrozenSet


@dataclass(frozen=True)
class ProcessedEmail:
    email_hash: str
    date: datetime
    norm_date: datetime
    subject: str
    aliases: FrozenSet[str]
    sender: FrozenSet[str]
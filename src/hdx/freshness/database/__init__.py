from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import DateTime, TypeDecorator


class CustomDateTime(TypeDecorator):
    impl = DateTime
    cache_ok = True

    def process_bind_param(self, value: Optional[datetime], dialect):
        if value is None:
            return value
        if value.tzinfo:
            value = value.astimezone(timezone.utc)

        return value.replace(tzinfo=None)

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)

        return value.astimezone(timezone.utc)

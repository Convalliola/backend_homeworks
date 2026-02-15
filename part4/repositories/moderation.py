from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import asyncpg


@dataclass(frozen=True, slots=True)
class ModerationResult:
    id: int
    item_id: int
    status: str
    is_violation: Optional[bool]
    probability: Optional[float]
    error_message: Optional[str]
    created_at: datetime
    processed_at: Optional[datetime]


def _row_to_moderation(row: asyncpg.Record) -> ModerationResult:
    return ModerationResult(
        id=int(row["id"]),
        item_id=int(row["item_id"]),
        status=str(row["status"]),
        is_violation=row["is_violation"],
        probability=float(row["probability"]) if row["probability"] is not None else None,
        error_message=row["error_message"],
        created_at=row["created_at"],
        processed_at=row["processed_at"],
    )


async def create_moderation_request(
    conn: asyncpg.Connection, *, item_id: int
) -> ModerationResult:
    """создание записи модерации со статусом pending"""
    row = await conn.fetchrow(
        """
        INSERT INTO public.moderation_results (item_id, status)
        VALUES ($1, 'pending')
        RETURNING id, item_id, status, is_violation, probability,
                  error_message, created_at, processed_at
        """,
        int(item_id),
    )
    assert row is not None
    return _row_to_moderation(row)


async def update_moderation_completed(
    conn: asyncpg.Connection,
    *,
    moderation_id: int,
    is_violation: bool,
    probability: float,
) -> ModerationResult | None:
    """обновление записи модерации с успешным результатом """
    row = await conn.fetchrow(
        """
        UPDATE public.moderation_results
        SET status = 'completed',
            is_violation = $2,
            probability = $3,
            processed_at = NOW()
        WHERE id = $1
        RETURNING id, item_id, status, is_violation, probability,
                  error_message, created_at, processed_at
        """,
        int(moderation_id),
        bool(is_violation),
        float(probability),
    )
    return _row_to_moderation(row) if row else None


async def update_moderation_failed(
    conn: asyncpg.Connection,
    *,
    moderation_id: int,
    error_message: str,
) -> ModerationResult | None:
    """обновление записи модрации с ошибкой"""
    row = await conn.fetchrow(
        """
        UPDATE public.moderation_results
        SET status = 'failed',
            error_message = $2,
            processed_at = NOW()
        WHERE id = $1
        RETURNING id, item_id, status, is_violation, probability,
                  error_message, created_at, processed_at
        """,
        int(moderation_id),
        error_message,
    )
    return _row_to_moderation(row) if row else None


async def get_moderation_by_id(
    conn: asyncpg.Connection, moderation_id: int
) -> ModerationResult | None:
    """получение результата модерации по id """
    row = await conn.fetchrow(
        """
        SELECT id, item_id, status, is_violation, probability,
               error_message, created_at, processed_at
        FROM public.moderation_results
        WHERE id = $1
        """,
        int(moderation_id),
    )
    return _row_to_moderation(row) if row else None

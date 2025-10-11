from __future__ import annotations

from datetime import datetime, timedelta, date
from typing import Any, Tuple

def now_seoul_str() -> str:
    return (datetime.utcnow() + timedelta(hours=9)).strftime("%Y-%m-%d %H:%M:%S")

def parse_datetime_flexible(s: Any) -> datetime | None:
    if not s:
        return None
    if isinstance(s, datetime):
        return s
    try:
        # 기본 ISO, 'YYYY-MM-DD', 'YYYY-MM-DDTHH:MM:SS' 등
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        try:
            # 아주 유연한 포맷 파서가 필요하면 dateutil 사용 가능(이미지에 없으면 제외)
            from dateutil import parser
            return parser.parse(str(s))
        except Exception:
            return None

def calc_period(report_type: str, start_date_input: str) -> Tuple[date, date]:
    """
    reportType 별 기간 계산.
    - DAILY: [start, start+1]
    - WEEKLY: [start, start+7]
    - MONTHLY: [start의 월 1일, 말일]
    """
    start = datetime.strptime(start_date_input, "%Y-%m-%d").date()

    if report_type.upper() == "DAILY":
        end = start + timedelta(days=1)
    elif report_type.upper() == "WEEKLY":
        end = start + timedelta(days=7)
    elif report_type.upper() == "MONTHLY":
        first = start.replace(day=1)
        # 다음달 1일 - 1일
        if first.month == 12:
            next_month_first = first.replace(year=first.year + 1, month=1, day=1)
        else:
            next_month_first = first.replace(month=first.month + 1, day=1)
        end = next_month_first - timedelta(days=1)
        start = first
    else:
        # 알 수 없는 타입이면 DAILY로 처리
        end = start
    return start, end

def to_instant_range(start: date, end: date) -> Tuple[datetime, datetime]:
    """
    날짜 범위를 [start 00:00:00, end+1 00:00:00) 로 반환 (UTC/naive)
    """
    start_dt = datetime.combine(start, datetime.min.time())
    end_dt_exclusive = datetime.combine(end + timedelta(days=1), datetime.min.time())
    return start_dt, end_dt_exclusive
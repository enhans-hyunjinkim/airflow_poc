# plugins/util.py
import json
from typing import Any, Dict, Optional, List

from airflow.models import Variable


def get_var_raw(key: str, default: Optional[str] = None) -> Optional[str]:
    """
    Airflow Variable.get() 을 안전하게 감싼 함수.
    존재하지 않으면 default 를 반환.
    """
    try:
        return Variable.get(key, default_var=default)
    except Exception:
        return default


def get_scoped(key: str, scope: Optional[str], default: Optional[str] = None) -> Optional[str]:
    """
    Variable 을 scope prefix 에 따라 우선 검색.
    ex) scope="naver_review" 이고 key="http_conn_id" 이면,
        naver_review__http_conn_id → http_conn_id 순서로 찾는다.
    """
    if scope:
        if isinstance(scope, (list, tuple)):
            scopes: List[str] = [str(s).strip() for s in scope if str(s).strip()]
        else:
            scopes: List[str] = [s.strip() for s in str(scope).split("|") if s.strip()]

        for sc in scopes:
            v = get_var_raw(f"{sc}__{key}", None)
            if v is not None:
                return v
    return get_var_raw(key, default)


def load_json(scoped_key: str, scope: Optional[str], default: Any) -> Any:
    """
    Variable 값을 JSON 으로 로드.
    잘못된 값이거나 없으면 default 반환.
    """
    raw = get_scoped(scoped_key, scope, None)
    if raw is None:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def deep_get(data: Dict[str, Any], dotted_path: str, default: Any = None) -> Any:
    """
    dict 에서 "a.b.c" 같은 dotted path 로 안전하게 값 꺼내오기.
    """
    if not dotted_path:
        return default
    cur: Any = data
    for part in dotted_path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return default
    return cur
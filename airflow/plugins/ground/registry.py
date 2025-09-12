# plugins/registry.py
from __future__ import annotations

from typing import Callable, Dict, Optional, Any

# 빌더 시그니처: (dag=DAG, params=dict) -> BaseOperator | TaskGroup
BuilderFn = Callable[..., Any]

# 내부 레지스트리
_REGISTRY: Dict[str, BuilderFn] = {}


def register_builder(stage_type: str, builder: BuilderFn) -> None:
    """
    주어진 타입에 대한 빌더 함수 등록.
    이미 존재하면 덮어쓴다(의도적으로 override 허용).
    """
    if not isinstance(stage_type, str) or not stage_type:
        raise ValueError("stage_type must be a non-empty string")
    if not callable(builder):
        raise ValueError("builder must be callable")
    _REGISTRY[stage_type] = builder


def get_builder(stage_type: Optional[str]) -> Optional[BuilderFn]:
    """
    스테이지 타입에 매칭되는 빌더를 반환. 없으면 None.
    """
    if not stage_type:
        return None
    return _REGISTRY.get(stage_type)


def available_types() -> Dict[str, BuilderFn]:
    """
    등록된 전체 타입 사전 반환(디버그/점검용).
    """
    return dict(_REGISTRY)


# -----------------------------------------------------
# 기본 빌더들 등록 (지연 import로 순환참조 회피)
# -----------------------------------------------------
def _register_defaults() -> None:
    # 지연 import: plugins.pipeline 이 registry 를 import 하지 않도록
    from plugins.ground.pipeline import build_http_async_pipeline

    register_builder("http_async", build_http_async_pipeline)

    # 필요 시, 다른 타입도 여기서 바로 매핑 가능:
    # from plugins.glue import build_glue_job_stage
    # register_builder("glue_job", build_glue_job_stage)
    #
    # from plugins.ops import build_python_callable_stage
    # register_builder("python_callable", build_python_callable_stage)


# 모듈 import 시점에 기본 매핑 주입
_register_defaults()
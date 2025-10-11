from typing import List

def camel_to_snake(s: str) -> str:
    out = []
    for ch in s:
        if ch.isupper():
            out.append("_")
            out.append(ch.lower())
        else:
            out.append(ch)
    res = "".join(out)
    return res[1:] if res.startswith("_") else res

def pick(d: dict, keys: List[str]) -> dict:
    return {k: d.get(k) for k in keys}
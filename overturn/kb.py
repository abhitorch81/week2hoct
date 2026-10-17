import json, re
from . import config

_KB = json.loads((config.DATA / "carc.json").read_text())


class _Safe(dict):
    def __missing__(self, k):
        return "[" + k.replace("_", " ") + "]"


def get(code: str) -> dict | None:
    return _KB.get(str(code).lstrip("0"))


def fill(template: str, **vals) -> str:
    return template.format_map(_Safe(**vals))


def all_codes():
    return list(_KB)


# Appeal windows are payer/plan specific. These are common defaults; the letter always
# tells staff to confirm against the notice. (Commercial plans governed by ERISA: 180 days;
# Medicare Part B redetermination: 120 days.)
WINDOWS = {"Medicare": 120, "Medicaid": 90}
DEFAULT_WINDOW = 180


def window_days(payer: str) -> int:
    return WINDOWS.get(payer, DEFAULT_WINDOW)

"""Deterministic parser for markdown notes dropped into `palinor-inbox/`.

Template (every section optional):

    # Title of the note
    Free-text summary ...

    ## Findings
    - claim one
    - claim two
    ## Agreements          (also: Decisions, Agreed)
    - we agreed to ...
    ## Questions           (also: Open questions)
    - is X better than Y?
    ## Tasks               (also: TODO, Next steps)
    - implement Z

Anything that is not under a recognised heading stays in the summary.
"""
from __future__ import annotations

import re

SECTIONS = {
    "findings": "findings", "finding": "findings", "facts": "findings", "insights": "findings",
    "agreements": "agreements", "agreed": "agreements", "decisions": "agreements", "decided": "agreements",
    "questions": "open_questions", "open questions": "open_questions", "open_questions": "open_questions",
    "tasks": "tasks", "todo": "tasks", "to do": "tasks", "next steps": "tasks", "action items": "tasks",
}
_BULLET = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+(?:\[[ xX]\]\s+)?(.*\S)\s*$")
_HEADING = re.compile(r"^\s*(#{1,6})\s+(.*\S)\s*$")


def parse_markdown(text: str, default_title: str = "note") -> dict:
    out = {"title": default_title, "summary": "", "findings": [], "agreements": [],
           "open_questions": [], "tasks": []}
    summary: list[str] = []
    current: str | None = None
    title_set = False
    for line in text.lstrip("﻿").splitlines():  # notes saved "UTF-8 with BOM" on Windows
        h = _HEADING.match(line)
        if h:
            name = h.group(2).strip().rstrip(":").lower()
            if len(h.group(1)) == 1 and not title_set:
                out["title"], title_set, current = h.group(2).strip(), True, None
                continue
            current = SECTIONS.get(name)
            if current is None:
                summary.append(line)
            continue
        b = _BULLET.match(line)
        if current and b:
            out[current].append(b.group(1).strip())
        elif current and line.strip() and out[current]:
            out[current][-1] += " " + line.strip()  # wrapped bullet continuation
        elif not current:
            summary.append(line)
    out["summary"] = "\n".join(summary).strip()[:4000]
    return out

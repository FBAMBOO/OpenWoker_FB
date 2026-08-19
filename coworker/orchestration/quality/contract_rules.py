"""Deterministic Prompt Contract Compiler rules and precedence metadata."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Pattern

from .models import Archetype


CONTRACT_RULESET_VERSION = "repo-analysis-rules@1"

PRECEDENCE = (
    "security_policy",
    "explicit_ui",
    "explicit_prompt",
    "user_custom",
    "archetype",
    "model_inference",
    "generic_default",
)


@dataclass(frozen=True, slots=True)
class PromptRule:
    id: str
    pattern: Pattern[str]

    def search(self, prompt: str) -> re.Match[str] | None:
        return self.pattern.search(prompt)


READ_ONLY_RULE = PromptRule(
    "read_only",
    re.compile(r"(?i)(?:\bread[- ]?only\b|do not modify|don't modify|只读|不要修改|不得修改)"),
)
CURRENT_CHECKOUT_RULE = PromptRule(
    "current_checkout",
    re.compile(
        r"(?i)(?:current (?:checkout|working tree|branch)|checked[- ]out branch|"
        r"当前(?:检出|工作树|工作区内容|分支)|现有工作树)"
    ),
)
EXPLICIT_REF_RULE = PromptRule(
    "explicit_ref",
    re.compile(
        r"(?i)(?<![\w/])((?:origin|upstream)/[A-Za-z0-9._/-]+|"
        r"(?:ref|branch|分支)\s*[:=]?\s*([A-Za-z0-9._/-]+)|[0-9a-f]{40,64})(?!\w)"
    ),
)
MARKDOWN_RULE = PromptRule(
    "markdown",
    re.compile(r"(?i)(?:markdown|\.md\b|架构报告|architecture report|analysis report)"),
)
EVIDENCE_RULE = PromptRule(
    "evidence",
    re.compile(r"(?i)(?:file evidence|citations?|line numbers?|文件证据|引用|行号|可追踪)"),
)
RELATIONSHIP_RULE = PromptRule(
    "relationship",
    re.compile(r"(?i)(?:relationships?|lineage|dependencies|之间的关系|依赖|血缘|控制链)"),
)
LIMITATION_RULE = PromptRule(
    "limitation",
    re.compile(r"(?i)(?:limitations?|assumptions?|限制|假设|未验证)"),
)
REPO_ANALYSIS_RULE = PromptRule(
    "repo_analysis",
    re.compile(
        r"(?i)(?:repository|repo|codebase|architecture|audit|dbt|fabric|"
        r"仓库|代码库|架构|审计|项目入口|models|macros|snapshots)"
    ),
)


def classify_archetype(prompt: str) -> Archetype:
    normalized = str(prompt)
    if REPO_ANALYSIS_RULE.search(normalized):
        return Archetype.REPO_ANALYSIS
    if re.search(
        r"(?i)(?:\bimplement\b|\bfix\b|\bchange\s+code\b|实现|修复|开发)",
        normalized,
    ):
        return Archetype.CODE_CHANGE
    if re.search(
        r"(?i)(?:\bincident\b|\boutage\b|\berror\s+log\b|事故|故障|日志)",
        normalized,
    ):
        return Archetype.INCIDENT_TRIAGE
    if re.search(r"(?i)(?:\bdocument\b|\breport\b|文档|报告)", normalized):
        return Archetype.DOCUMENT_GENERATION
    return Archetype.FOCUSED_QUESTION

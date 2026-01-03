# -*- coding: utf-8 -*-
"""
AI 分析服务

作为 telegram-service 的子模块集成，提供 AI 深度分析功能。
"""
from src.bot import (
    AIAnalysisHandler,
    get_ai_handler,
    register_ai_handlers,
)
from src.pipeline import run_analysis
from src.prompt import PromptRegistry

__all__ = [
    "AIAnalysisHandler",
    "get_ai_handler",
    "register_ai_handlers",
    "run_analysis",
    "PromptRegistry",
]

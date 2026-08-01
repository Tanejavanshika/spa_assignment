"""Standardized logging configuration for UrbanPulse components."""

from __future__ import annotations

import logging
import os
from typing import Final

DEFAULT_LOG_FORMAT: Final[str] = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def setup_logging(name: str, level: str | None = None) -> logging.Logger:
    """Configure and return a module logger with consistent formatting."""
    log_level = (level or os.getenv("LOG_LEVEL", "INFO")).upper()
    logging.basicConfig(level=getattr(logging, log_level, logging.INFO), format=DEFAULT_LOG_FORMAT)
    return logging.getLogger(name)

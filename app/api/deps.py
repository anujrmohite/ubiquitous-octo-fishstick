import logging
from typing import Generator

from sqlalchemy.orm import Session
from app.core.security import get_api_key, get_current_user

logger = logging.getLogger(__name__)
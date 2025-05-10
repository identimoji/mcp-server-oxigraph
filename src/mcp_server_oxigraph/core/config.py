"""
Configuration functions for PyOxigraph MCP.

This module provides functions for getting configuration settings.
"""

import logging
import os
from typing import Optional

# Configure logging
logger = logging.getLogger(__name__)

def get_system_default_store_path() -> str:
    """
    Get the system default store path.
    
    Returns:
        Path to the system default store
    """
    return os.path.expanduser("~/.mcp-server-oxigraph/default.oxigraph")

def get_default_store_path() -> Optional[str]:
    """
    Get the user default store path from environment variables.
    
    Returns:
        Path to the user default store, or None if not configured
    """
    env_path = os.environ.get("OXIGRAPH_DEFAULT_STORE")
    if env_path:
        # Expand user directory if needed
        if env_path.startswith("~"):
            env_path = os.path.expanduser(env_path)
        return env_path
    return None

def has_user_default_store() -> bool:
    """
    Check if a user default store is configured.
    
    Returns:
        True if a user default store is configured, False otherwise
    """
    return get_default_store_path() is not None

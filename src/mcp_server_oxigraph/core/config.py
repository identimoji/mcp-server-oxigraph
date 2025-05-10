"""
Configuration management for the Oxigraph MCP server.

This module provides functionality for loading and managing configuration.
"""

import os
import logging
from pathlib import Path
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# Default configuration values
DEFAULT_CONFIG = {
    "system_default_store_path": os.path.expanduser("~/.mcp-server-oxigraph/default.oxigraph"),
    "user_default_store_path": None
}

class ConfigManager:
    """
    Manages configuration for the Oxigraph MCP server.
    """
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._instance.config = DEFAULT_CONFIG.copy()
            cls._instance._load_from_env()
        return cls._instance
    
    def _load_from_env(self):
        """
        Load configuration from environment variables.
        """
        # Check for OXIGRAPH_DEFAULT_STORE env var
        user_default_store_path = os.environ.get("OXIGRAPH_DEFAULT_STORE")
        if user_default_store_path:
            self.config["user_default_store_path"] = os.path.expanduser(user_default_store_path)
            logger.info(f"Found user-defined default store path from environment: {self.config['user_default_store_path']}")
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value.
        
        Args:
            key: The configuration key
            default: Default value if the key is not found
            
        Returns:
            The configuration value
        """
        return self.config.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        """
        Set a configuration value.
        
        Args:
            key: The configuration key
            value: The value to set
        """
        self.config[key] = value
    
    def get_default_store_path(self) -> str:
        """
        Get the default store path.
        Returns the user-defined path if set, otherwise the system default.
        
        Returns:
            The default store path to use
        """
        # Prefer user-defined path if available
        if self.config["user_default_store_path"]:
            return self.config["user_default_store_path"]
        # Fall back to system default
        return self.config["system_default_store_path"]
        
    def get_system_default_store_path(self) -> str:
        """
        Get the system default store path.
        
        Returns:
            The system default store path
        """
        return self.config["system_default_store_path"]
        
    def has_user_default_store(self) -> bool:
        """
        Check if a user-defined default store path is set.
        
        Returns:
            True if a user-defined path is set, False otherwise
        """
        return self.config["user_default_store_path"] is not None

# Initialize the global config manager
_config_manager = ConfigManager()

def get_config(key: str, default: Any = None) -> Any:
    """
    Get a configuration value.
    
    Args:
        key: The configuration key
        default: Default value if the key is not found
        
    Returns:
        The configuration value
    """
    return _config_manager.get(key, default)

def set_config(key: str, value: Any) -> None:
    """
    Set a configuration value.
    
    Args:
        key: The configuration key
        value: The value to set
    """
    _config_manager.set(key, value)

def get_default_store_path() -> str:
    """
    Get the default store path.
    Returns the user-defined path if set, otherwise the system default.
    
    Returns:
        The default store path to use
    """
    return _config_manager.get_default_store_path()

def get_system_default_store_path() -> str:
    """
    Get the system default store path.
    
    Returns:
        The system default store path
    """
    return _config_manager.get_system_default_store_path()

def has_user_default_store() -> bool:
    """
    Check if a user-defined default store path is set.
    
    Returns:
        True if a user-defined path is set, False otherwise
    """
    return _config_manager.has_user_default_store()

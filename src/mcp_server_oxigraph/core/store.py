"""
Store management functions for PyOxigraph.

This module provides functions for creating, opening, closing, and managing PyOxigraph stores.
"""

import logging
import os
import sys
import json
import time
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
import pyoxigraph

# Import configuration
from .config import get_default_store_path, get_system_default_store_path, has_user_default_store

logger = logging.getLogger(__name__)

# Helper function to normalize paths
def normalize_path(path: str) -> str:
    """
    Normalize a file path to an absolute path.
    
    Args:
        path: File path to normalize
        
    Returns:
        Absolute file path with user directory expanded
    """
    if path is None:
        return None
        
    # Expand user directory
    if path.startswith("~"):
        path = os.path.expanduser(path)
    
    # Make absolute
    if not os.path.isabs(path):
        path = os.path.abspath(path)
    
    # Normalize path separators and resolve any symbolic links
    path = os.path.normpath(path)
    
    # Remove trailing slashes that might cause comparison issues
    path = path.rstrip(os.path.sep)
    
    # On case-insensitive file systems, normalize case
    if sys.platform.startswith("win") or sys.platform == "darwin":
        path = path.lower()
    
    logger.debug(f"Normalized path: '{path}'")
    return path

# Default location will be loaded from config

# Global store registry - singleton pattern with file persistence
class StoreManager:
    """
    Manages PyOxigraph stores, keeping track of open stores.
    Uses file-based persistence to maintain state across API calls.
    """
    
    _instance = None
    _registry_file = os.path.expanduser("~/.mcp-server-oxigraph/registry.json")
    _last_loaded = 0
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(StoreManager, cls).__new__(cls)
            cls._instance.stores = {}  # Dictionary to keep track of opened stores
            cls._instance.system_store_path = normalize_path(get_system_default_store_path())
            cls._instance.user_store_path = normalize_path(get_default_store_path()) if has_user_default_store() else None
            cls._instance.active_default_path = None
            
            # Ensure the directory for the registry file exists
            registry_dir = os.path.dirname(cls._registry_file)
            os.makedirs(registry_dir, exist_ok=True)
            
            # Load the registry from file
            cls._instance._load_registry()
        return cls._instance
    
    def __init__(self):
        # Initialization already handled in __new__
        pass
        
    def _load_registry(self):
        """
        Load the store registry from file.
        """
        try:
            # Check if registry file exists
            if os.path.exists(self._registry_file):
                # Check if file was modified since last load
                file_mtime = os.path.getmtime(self._registry_file)
                if file_mtime > self._last_loaded:
                    with open(self._registry_file, 'r') as f:
                        registry_data = json.load(f)
                    
                    # Update the registry data
                    store_paths = registry_data.get('store_paths', [])
                    active_default = registry_data.get('active_default', None)
                    
                    # Reset the stores dictionary
                    self.stores = {}
                    
                    # Open all registered stores
                    for store_path in store_paths:
                        try:
                            if os.path.exists(store_path):
                                logger.debug(f"Opening registered store: {store_path}")
                                self.stores[store_path] = pyoxigraph.Store(store_path)
                            else:
                                logger.warning(f"Registered store path does not exist: {store_path}")
                        except Exception as e:
                            logger.error(f"Failed to open registered store {store_path}: {e}")
                    
                    # Set the active default path if it exists in the registry
                    if active_default and active_default in self.stores:
                        self.active_default_path = active_default
                    elif len(self.stores) > 0:
                        # Fall back to using the first available store
                        self.active_default_path = next(iter(self.stores.keys()))
                    else:
                        self.active_default_path = None
                    
                    # Update the last loaded timestamp
                    self._last_loaded = time.time()
                    
                    logger.info(f"Loaded store registry with {len(self.stores)} stores")
                    logger.debug(f"Registered stores: {list(self.stores.keys())}")
                    logger.debug(f"Active default: {self.active_default_path}")
        except Exception as e:
            logger.error(f"Failed to load registry: {e}")
    
    def _save_registry(self):
        """
        Save the store registry to file.
        """
        try:
            registry_data = {
                'store_paths': list(self.stores.keys()),
                'active_default': self.active_default_path
            }
            
            with open(self._registry_file, 'w') as f:
                json.dump(registry_data, f)
            
            # Update the last loaded timestamp
            self._last_loaded = time.time()
            
            logger.debug(f"Saved store registry with {len(self.stores)} stores")
        except Exception as e:
            logger.error(f"Failed to save registry: {e}")
        
    def initialize_stores(self) -> None:
        """
        Initialize both system and user default stores.
        This method is called at server startup.
        """
        logger.info("Initializing Oxigraph stores...")
        
        # First, initialize the system default store
        try:
            self._initialize_store(self.system_store_path)
            logger.info(f"Initialized system default store at {self.system_store_path}")
        except Exception as e:
            logger.error(f"Failed to initialize system default store: {e}")
        
        # Then, if user default is specified, initialize it too
        if self.user_store_path:
            try:
                self._initialize_store(self.user_store_path)
                # Set user store as the active default
                self.active_default_path = self.user_store_path
                logger.info(f"Initialized user default store at {self.user_store_path}")
                logger.info(f"Using user default store as active default")
            except Exception as e:
                logger.error(f"Failed to initialize user default store: {e}")
                # Fall back to system default if available
                if self.system_store_path in self.stores:
                    self.active_default_path = self.system_store_path
                    logger.info(f"Falling back to system default store")
        else:
            # If no user store, use system store as active default
            if self.system_store_path in self.stores:
                self.active_default_path = self.system_store_path
                logger.info(f"Using system default store as active default")
        
        if not self.active_default_path:
            logger.warning("No default store could be initialized!")
    
    def _initialize_store(self, store_path: str) -> None:
        """
        Initialize a store at the given path.
        
        Args:
            store_path: Path to the store
            
        Raises:
            ValueError: If the store cannot be initialized
        """
        # Normalize the path to ensure consistency
        normalized_path = normalize_path(store_path)
        
        # Create store directory if it doesn't exist
        store_dir = os.path.dirname(normalized_path)
        try:
            os.makedirs(store_dir, exist_ok=True)
        except Exception as e:
            logger.error(f"Failed to create store directory: {e}")
            raise ValueError(f"Could not create store directory at {store_dir}: {e}")
        
        # If the store already exists on disk, open it
        if os.path.exists(normalized_path):
            try:
                self.open_store(normalized_path)
                logger.info(f"Opened existing store at {normalized_path}")
                return
            except Exception as e:
                logger.error(f"Failed to open existing store: {e}")
                # Fall through to create a new one
        
        # Create a new store
        try:
            self.create_store(normalized_path)
            logger.info(f"Created new store at {normalized_path}")
            return
        except Exception as e:
            logger.error(f"Failed to create store: {e}")
            raise ValueError(f"Could not create store at {normalized_path}: {e}")
    
    def ensure_default_store_exists(self) -> Optional[str]:
        """
        Ensures the default store exists and returns its path.
        If no stores have been initialized yet, it initializes them.
        
        Returns:
            The path of the active default store or None if unavailable
        """
        # If we haven't initialized stores yet, do it now
        if self.active_default_path is None or len(self.stores) == 0:
            logger.info("No active default store found, initializing stores")
            self.initialize_stores()
        
        # Check if we have an active default store
        if self.active_default_path and self.active_default_path in self.stores:
            return self.active_default_path
        
        # If we still don't have an active default, something went wrong
        # Try again with system default path as a last resort
        logger.warning("No active default store found, trying system default path")
        try:
            logger.info(f"Creating store at system path: {self.system_store_path}")
            self.create_store(self.system_store_path)
            self.active_default_path = self.system_store_path
            return self.active_default_path
        except Exception as e:
            logger.error(f"Failed to create system default store: {e}")
            raise ValueError("Could not create any store for operation")
    
    def create_store(self, store_path: str) -> Dict[str, Any]:
        """
        Create a new PyOxigraph store on disk.
        
        Args:
            store_path: Path for the store.
            
        Returns:
            A dictionary with the result
        """
        try:
            # Always load the latest registry state first
            self._load_registry()
            
            # First normalize the path to check for existing stores
            normalized_path = normalize_path(store_path)
            
            # Check if store already exists using normalized path
            if normalized_path in self.stores:
                raise ValueError(f"Store with path '{normalized_path}' already exists")
            
            # Also check for case-insensitive matches on macOS and Windows
            if sys.platform.startswith("win") or sys.platform == "darwin":
                for existing_path in self.stores.keys():
                    if existing_path.lower() == normalized_path.lower():
                        logger.warning(f"Case-insensitive match found: '{existing_path}' vs '{normalized_path}'")
                        raise ValueError(f"Store with similar path '{existing_path}' already exists (case-insensitive match)")
            
            # Create a persistent store
            os.makedirs(os.path.dirname(normalized_path), exist_ok=True)
            store = pyoxigraph.Store(normalized_path)
            store_type = "persistent"
            
            # Log the exact path keys for debugging
            logger.info(f"Created store at normalized path: '{normalized_path}'")
            logger.debug(f"Store registry before adding: {list(self.stores.keys())}")
            
            # Store it in our dictionary using the normalized path
            self.stores[normalized_path] = store
            
            # Save the updated registry to file
            self._save_registry()
            
            logger.debug(f"Store registry after adding: {list(self.stores.keys())}")
            
            # Set as default if no active default exists
            if self.active_default_path is None:
                self.active_default_path = normalized_path
                self._save_registry()
                
            return {
                "store": normalized_path
            }
        except Exception as e:
            logger.error(f"Error creating store: {e}")
            raise
    
    def open_store(self, store_path: str, read_only: bool = False) -> Dict[str, Any]:
        """
        Open an existing PyOxigraph store from disk.
        
        Args:
            store_path: Path to the store on disk
            read_only: Whether to open in read-only mode
            
        Returns:
            A dictionary with the result
        """
        try:
            # Always load the latest registry state first
            self._load_registry()
            
            # Normalize the path to ensure consistency
            normalized_path = normalize_path(store_path)
            logger.debug(f"Opening store with normalized path: '{normalized_path}'")
            
            # Check if store already exists using normalized path
            if normalized_path in self.stores:
                logger.warning(f"Store with path '{normalized_path}' already exists in registry")
                raise ValueError(f"Store with path '{normalized_path}' already exists")
            
            # Also check for case-insensitive matches on macOS and Windows
            if sys.platform.startswith("win") or sys.platform == "darwin":
                for existing_path in self.stores.keys():
                    if existing_path.lower() == normalized_path.lower():
                        logger.warning(f"Case-insensitive match found: '{existing_path}' vs '{normalized_path}'")
                        raise ValueError(f"Store with similar path '{existing_path}' already exists (case-insensitive match)")
                        
            if not os.path.exists(normalized_path):
                raise FileNotFoundError(f"Store path does not exist: {normalized_path}")
            
            if read_only:
                # Use the read_only method if available
                if hasattr(pyoxigraph.Store, 'read_only'):
                    store = pyoxigraph.Store.read_only(normalized_path)
                    mode = "read-only"
                else:
                    # Fallback to regular open if read_only not available
                    store = pyoxigraph.Store(normalized_path)
                    mode = "read-write (read_only not supported)"
            else:
                store = pyoxigraph.Store(normalized_path)
                mode = "read-write"
            
            logger.info(f"Opened store at '{normalized_path}' in {mode} mode")
            logger.debug(f"Store registry before adding: {list(self.stores.keys())}")
            
            # Store it in our dictionary with the normalized path as the key
            self.stores[normalized_path] = store
            
            # Save the updated registry to file
            self._save_registry()
            
            logger.debug(f"Store registry after adding: {list(self.stores.keys())}")
            
            # Set as default if no active default exists
            if self.active_default_path is None:
                self.active_default_path = normalized_path
                logger.info(f"Set '{normalized_path}' as active default store")
                self._save_registry()
                
            return {
                "store": normalized_path
            }
        except Exception as e:
            logger.error(f"Error opening store: {e}")
            raise
    
    def close_store(self, store_path: str) -> Dict[str, Any]:
        """
        Close a PyOxigraph store.
        
        Args:
            store_path: The path of the store to close
            
        Returns:
            A dictionary with the result
        """
        try:
            # Always load the latest registry state first
            self._load_registry()
            
            # Normalize path
            store_path = normalize_path(store_path)
                
            if store_path not in self.stores:
                raise ValueError(f"Store with path '{store_path}' not found")
            
            # Get the store
            store = self.stores[store_path]
            
            # Close the store - note there's no explicit close method in PyOxigraph,
            # but we'll remove it from our dictionary
            del self.stores[store_path]
            
            # Save the updated registry to file
            self._save_registry()
            
            # Update default store if needed
            if self.active_default_path == store_path:
                self.active_default_path = next(iter(self.stores.keys())) if self.stores else None
                self._save_registry()
                
            return {
                "success": True,
                "message": f"Closed store at '{store_path}'"
            }
        except Exception as e:
            logger.error(f"Error closing store: {e}")
            raise
    
    def backup_store(self, store_path: str, backup_path: str) -> Dict[str, Any]:
        """
        Create a backup of a PyOxigraph store.
        
        Args:
            store_path: Path of the store to backup
            backup_path: Path where to save the backup
            
        Returns:
            A dictionary with the result
        """
        try:
            # Normalize path
            store_path = normalize_path(store_path)
            
            if store_path not in self.stores:
                raise ValueError(f"Store with path '{store_path}' not found")
            
            # Get the store
            store = self.stores[store_path]
            
            # Create backup directory
            backup_path = os.path.expanduser(backup_path)
            os.makedirs(os.path.dirname(backup_path), exist_ok=True)
            
            # Check if backup method is available
            if hasattr(store, 'backup'):
                # Backup the store
                store.backup(backup_path)
            else:
                # Alternative implementation if backup method is not available
                import shutil
                
                if os.path.isdir(store_path):
                    shutil.copytree(store_path, backup_path)
                else:
                    shutil.copy2(store_path, backup_path)
            
            return {
                "success": True,
                "message": f"Created backup at '{backup_path}'"
            }
        except Exception as e:
            logger.error(f"Error backing up store: {e}")
            raise
    
    def restore_store(self, backup_path: str, restore_path: str) -> Dict[str, Any]:
        """
        Restore a PyOxigraph store from a backup.
        
        Args:
            backup_path: Path of the backup to restore
            restore_path: Path where to restore the store
            
        Returns:
            A dictionary with the result
        """
        try:
            backup_path = os.path.expanduser(backup_path)
            restore_path = normalize_path(restore_path)
            
            if restore_path in self.stores:
                raise ValueError(f"Store with path '{restore_path}' already exists")
                
            if not os.path.exists(backup_path):
                raise FileNotFoundError(f"Backup path does not exist: {backup_path}")
            
            # Create restore directory if needed
            restore_dir = os.path.dirname(restore_path)
            os.makedirs(restore_dir, exist_ok=True)
            
            # Check if from_backup method is available
            if hasattr(pyoxigraph.Store, 'from_backup'):
                # Create a new store from the backup
                store = pyoxigraph.Store.from_backup(backup_path, restore_path)
            else:
                # Alternative approach - copy files then open as regular store
                import shutil
                if os.path.isdir(backup_path):
                    shutil.copytree(backup_path, restore_path)
                else:
                    shutil.copy2(backup_path, restore_path)
                store = pyoxigraph.Store(restore_path)
            
            # Store it in our dictionary
            self.stores[restore_path] = store
            
            # Set as default if no active default exists
            if self.active_default_path is None:
                self.active_default_path = restore_path
                
            return {
                "store": restore_path
            }
        except Exception as e:
            logger.error(f"Error restoring store: {e}")
            raise
    
    def optimize_store(self, store_path: str) -> Dict[str, Any]:
        """
        Optimize a PyOxigraph store for performance.
        
        Args:
            store_path: The path of the store to optimize
            
        Returns:
            A dictionary with the result
        """
        try:
            # Normalize path
            store_path = normalize_path(store_path)
                
            if store_path not in self.stores:
                raise ValueError(f"Store with path '{store_path}' not found")
            
            # Get the store
            store = self.stores[store_path]
            
            # Check if optimize method is available
            if hasattr(store, 'optimize'):
                # Optimize the store
                store.optimize()
                return {
                    "success": True,
                    "message": f"Optimized store at '{store_path}'"
                }
            else:
                return {
                    "success": True,
                    "message": "Store optimization is not available in this version of PyOxigraph"
                }
        except Exception as e:
            logger.error(f"Error optimizing store: {e}")
            raise
    
    def get_store(self, store_path: Optional[str] = None) -> Optional[pyoxigraph.Store]:
        """
        Get a store by path or the default store.
        
        Args:
            store_path: The path of the store to get, or None for the default
            
        Returns:
            The PyOxigraph Store object or None if not found
        """
        # Always load the latest registry state first
        self._load_registry()
        
        if not store_path:
            # If no store_path provided, ensure default store exists and use it
            store_path = self.ensure_default_store_exists()
            logger.debug(f"Using default store path: '{store_path}'")
            
        if not store_path:
            logger.error("No default store available")
            return None
            
        # Normalize the path
        normalized_path = normalize_path(store_path)
        logger.debug(f"Looking up store with normalized path: '{normalized_path}'")
        
        # If the requested store exists directly, return it
        if normalized_path in self.stores:
            return self.stores[normalized_path]
        
        # If not found directly, try additional diagnostics
        logger.warning(f"Requested store '{normalized_path}' not found.")
        logger.warning(f"Available stores: {list(self.stores.keys())}")
        
        # Try to find close matches to help diagnose normalization issues
        close_matches = []
        for key in self.stores.keys():
            # Check for paths that might be the same except for case or trailing slashes
            if key.lower() == normalized_path.lower() or key.rstrip('/\\') == normalized_path.rstrip('/\\'):
                close_matches.append(key)
                
        if close_matches:
            logger.warning(f"Found potential close matches: {close_matches}")
            
            # For MacOS, try to match case-insensitively as a fallback
            if sys.platform == "darwin" and any(key.lower() == normalized_path.lower() for key in close_matches):
                for key in close_matches:
                    if key.lower() == normalized_path.lower():
                        logger.info(f"Using case-insensitive match on macOS: {key}")
                        return self.stores[key]
        
        # If the store isn't found in memory, try opening it from disk
        try:
            if os.path.exists(store_path):
                logger.info(f"Store not in registry but exists on disk, opening: {store_path}")
                store = pyoxigraph.Store(store_path)
                self.stores[normalized_path] = store
                self._save_registry()
                return store
        except Exception as e:
            logger.error(f"Failed to open store from disk: {e}")
        
        return None
    
    def list_stores(self) -> Dict[str, Any]:
        """
        List all open stores.
        
        Returns:
            A dictionary with the list of stores
        """
        # Always load the latest registry state first
        self._load_registry()
        
        return {
            "stores": list(self.stores.keys()),
            "default": self.active_default_path
        }
    
    def set_default_store(self, store_path: str) -> Dict[str, Any]:
        """
        Set the default store.
        
        Args:
            store_path: The path of the store to set as default
            
        Returns:
            A dictionary with the result
        """
        # Normalize path
        store_path = normalize_path(store_path)
            
        if store_path not in self.stores:
            raise ValueError(f"Store with path '{store_path}' not found")
        
        # Set as default
        self.active_default_path = store_path
        
        return {
            "success": True,
            "message": f"Set store at '{store_path}' as default"
        }

# Initialize the global store manager
_store_manager = StoreManager()

# Exposed API functions

def oxigraph_create_store(store_path: str) -> Dict[str, Any]:
    """
    Create a new PyOxigraph store on disk.
    
    Args:
        store_path: Path for the store.
        
    Returns:
        A dictionary with the result
    """
    return _store_manager.create_store(store_path)


def oxigraph_open_store(store_path: str, read_only: bool = False) -> Dict[str, Any]:
    """
    Open an existing file-based store.
    
    Args:
        store_path: Path to the file-based store
        read_only: Whether to open in read-only mode
        
    Returns:
        Store operation result
    """
    return _store_manager.open_store(store_path, read_only)


def oxigraph_close_store(store_path: str) -> Dict[str, Any]:
    """
    Close a store and remove it from the manager.
    
    Args:
        store_path: Path to the store
    
    Returns:
        Operation result
    """
    return _store_manager.close_store(store_path)


def oxigraph_backup_store(store_path: str, backup_path: str) -> Dict[str, Any]:
    """
    Create a backup of a store.
    
    Args:
        store_path: Path to the store
        backup_path: Path where to save the backup
    
    Returns:
        Operation result
    """
    return _store_manager.backup_store(store_path, backup_path)


def oxigraph_restore_store(backup_path: str, restore_path: str) -> Dict[str, Any]:
    """
    Restore a store from a backup.
    
    Args:
        backup_path: Path to the backup file
        restore_path: Path where to restore the store
    
    Returns:
        Operation result
    """
    return _store_manager.restore_store(backup_path, restore_path)


def oxigraph_optimize_store(store_path: str) -> Dict[str, Any]:
    """
    Optimize a store for better performance.
    
    Args:
        store_path: Path to the store
    
    Returns:
        Operation result
    """
    return _store_manager.optimize_store(store_path)


def oxigraph_get_store(store_path: Optional[str] = None) -> Optional[pyoxigraph.Store]:
    """
    Get a store by path.
    
    Args:
        store_path: Path of the store to retrieve (defaults to the default store)
    
    Returns:
        The store instance
    """
    return _store_manager.get_store(store_path)


def oxigraph_list_stores() -> Dict[str, Any]:
    """
    List all managed stores.
    
    Returns:
        Dictionary with list of store paths and the default store
    """
    return _store_manager.list_stores()

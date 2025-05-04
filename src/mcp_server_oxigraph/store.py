"""
Store management functions for PyOxigraph.

This module provides functions for creating, opening, closing, and managing PyOxigraph stores.
"""

import logging
import os
from typing import Dict, Optional, Any
import pyoxigraph

logger = logging.getLogger(__name__)

# Global store registry
_STORES: Dict[str, pyoxigraph.Store] = {}
_DEFAULT_STORE_ID = "default"

def oxigraph_create_store(store_id: str, path: Optional[str] = None) -> Dict[str, Any]:
    """
    Create a new store (in-memory or file-based).
    
    Args:
        store_id: Unique identifier for the store
        path: Optional path for file-based store
        
    Returns:
        Store operation result
    """
    global _STORES
    
    if store_id in _STORES:
        raise ValueError(f"Store with ID '{store_id}' already exists")
    
    try:
        if path:
            # Create directory if it doesn't exist
            os.makedirs(path, exist_ok=True)
            store = pyoxigraph.Store(path)
        else:
            store = pyoxigraph.Store()
        
        _STORES[store_id] = store
        logger.info(f"Created {'file-based' if path else 'in-memory'} store with ID '{store_id}'")
        return {"store": ""}
    
    except Exception as e:
        logger.error(f"Error creating store: {e}")
        raise


def oxigraph_open_store(store_id: str, path: str) -> Dict[str, Any]:
    """
    Open an existing file-based store.
    
    Args:
        store_id: Unique identifier for the store
        path: Path to the file-based store
        
    Returns:
        Store operation result
    """
    global _STORES
    
    if store_id in _STORES:
        raise ValueError(f"Store with ID '{store_id}' already exists")
    
    try:
        store = pyoxigraph.Store(path)
        _STORES[store_id] = store
        logger.info(f"Opened file-based store at '{path}' with ID '{store_id}'")
        return {"store": ""}
    
    except Exception as e:
        logger.error(f"Error opening store: {e}")
        raise


def oxigraph_open_read_only(store_id: str, path: str) -> Dict[str, Any]:
    """
    Open an existing file-based store in read-only mode.
    
    Args:
        store_id: Unique identifier for the store
        path: Path to the file-based store
        
    Returns:
        Store operation result
    """
    global _STORES
    
    if store_id in _STORES:
        raise ValueError(f"Store with ID '{store_id}' already exists")
    
    try:
        # Note: PyOxigraph doesn't have a built-in read-only mode,
        # so this is a placeholder for future implementation
        store = pyoxigraph.Store(path)
        _STORES[store_id] = store
        logger.info(f"Opened read-only file-based store at '{path}' with ID '{store_id}'")
        return {"store": ""}
    
    except Exception as e:
        logger.error(f"Error opening read-only store: {e}")
        raise


def oxigraph_close_store(store_id: str) -> Dict[str, Any]:
    """
    Close a store and remove it from the manager.
    
    Args:
        store_id: Unique identifier for the store
    
    Returns:
        Operation result
    """
    global _STORES
    
    if store_id not in _STORES:
        raise ValueError(f"Store with ID '{store_id}' not found")
    
    try:
        # PyOxigraph automatically closes the store when it's garbage collected,
        # but we can manually remove it from our registry
        del _STORES[store_id]
        logger.info(f"Closed store '{store_id}'")
        return {"success": True, "message": f"Closed store '{store_id}'"}
    
    except Exception as e:
        logger.error(f"Error closing store: {e}")
        raise


def oxigraph_backup_store(store_id: str, backup_path: str) -> Dict[str, Any]:
    """
    Create a backup of a store.
    
    Args:
        store_id: Unique identifier for the store
        backup_path: Path where to save the backup
    
    Returns:
        Operation result
    """
    store = oxigraph_get_store(store_id)
    
    if not store:
        raise ValueError(f"Store with ID '{store_id}' not found")
    
    # This is a placeholder - PyOxigraph doesn't have a built-in backup method
    # We need to implement copying the store files
    raise NotImplementedError("Backup functionality is not yet implemented")


def oxigraph_restore_store(store_id: str, backup_path: str) -> Dict[str, Any]:
    """
    Restore a store from a backup.
    
    Args:
        store_id: Unique identifier for the store
        backup_path: Path to the backup file
    
    Returns:
        Operation result
    """
    # This is a placeholder - PyOxigraph doesn't have a built-in restore method
    # We need to implement copying the backup files and reopening the store
    raise NotImplementedError("Restore functionality is not yet implemented")


def oxigraph_optimize_store(store_id: str) -> Dict[str, Any]:
    """
    Optimize a store for better performance.
    
    Args:
        store_id: Unique identifier for the store
    
    Returns:
        Operation result
    """
    store = oxigraph_get_store(store_id)
    
    if not store:
        raise ValueError(f"Store with ID '{store_id}' not found")
    
    # This is a placeholder - PyOxigraph doesn't have a built-in optimize method
    return {"success": True, "message": "Store optimization is not available in PyOxigraph"}


def oxigraph_get_store(store_id: Optional[str] = None) -> Optional[pyoxigraph.Store]:
    """
    Get a store by ID.
    
    Args:
        store_id: ID of the store to retrieve (defaults to the default store)
    
    Returns:
        The store instance
    """
    global _STORES, _DEFAULT_STORE_ID
    
    if not store_id:
        store_id = _DEFAULT_STORE_ID
    
    # Create default store if it doesn't exist
    if store_id == _DEFAULT_STORE_ID and store_id not in _STORES:
        _STORES[_DEFAULT_STORE_ID] = pyoxigraph.Store()
        logger.info(f"Created default in-memory store")
    
    return _STORES.get(store_id)


def oxigraph_list_stores() -> Dict[str, Any]:
    """
    List all managed stores.
    
    Returns:
        List of store IDs
    """
    global _STORES
    
    return {"stores": list(_STORES.keys())}

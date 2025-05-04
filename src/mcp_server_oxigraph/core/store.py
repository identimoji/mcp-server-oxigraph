"""
Store management functions for PyOxigraph.

This module provides functions for creating, opening, closing, and managing PyOxigraph stores.
"""

import logging
import os
import sys
from typing import Dict, List, Any, Optional, Union
import pyoxigraph

logger = logging.getLogger(__name__)

# Global store registry - singleton pattern
class StoreManager:
    """
    Manages PyOxigraph stores, keeping track of open stores.
    """
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(StoreManager, cls).__new__(cls)
            cls._instance.stores = {}  # Dictionary to keep track of opened stores
            cls._instance.default_store = None
        return cls._instance
    
    def __init__(self):
        # Initialization already handled in __new__
        pass
    
    def create_store(self, store_id: str, path: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a new PyOxigraph store, either in-memory or on disk.
        
        Args:
            store_id: A unique identifier for this store
            path: Optional path for a persistent store. If None, creates an in-memory store.
            
        Returns:
            A dictionary with the result
        """
        try:
            if store_id in self.stores:
                raise ValueError(f"Store with ID '{store_id}' already exists")
                
            if path:
                # Create a persistent store
                path = os.path.expanduser(path)
                os.makedirs(os.path.dirname(path), exist_ok=True)
                store = pyoxigraph.Store(path)
                store_type = "persistent"
            else:
                # Create an in-memory store
                store = pyoxigraph.Store()
                store_type = "in-memory"
            
            # Store it in our dictionary
            self.stores[store_id] = store
            
            # Set as default if it's the first store
            if self.default_store is None:
                self.default_store = store_id
                
            return {
                "store": store_id
            }
        except Exception as e:
            logger.error(f"Error creating store: {e}")
            raise
    
    def open_store(self, store_id: str, path: str, read_only: bool = False) -> Dict[str, Any]:
        """
        Open an existing PyOxigraph store from disk.
        
        Args:
            store_id: A unique identifier for this store
            path: Path to the store on disk
            read_only: Whether to open in read-only mode
            
        Returns:
            A dictionary with the result
        """
        try:
            if store_id in self.stores:
                raise ValueError(f"Store with ID '{store_id}' already exists")
                
            path = os.path.expanduser(path)
            
            if not os.path.exists(path):
                raise FileNotFoundError(f"Store path does not exist: {path}")
            
            if read_only:
                # Use the read_only method if available
                if hasattr(pyoxigraph.Store, 'read_only'):
                    store = pyoxigraph.Store.read_only(path)
                else:
                    # Fallback to regular open if read_only not available
                    store = pyoxigraph.Store(path)
                mode = "read-only"
            else:
                store = pyoxigraph.Store(path)
                mode = "read-write"
            
            # Store it in our dictionary
            self.stores[store_id] = store
            
            # Set as default if it's the first store
            if self.default_store is None:
                self.default_store = store_id
                
            return {
                "store": store_id
            }
        except Exception as e:
            logger.error(f"Error opening store: {e}")
            raise
    
    def close_store(self, store_id: str) -> Dict[str, Any]:
        """
        Close a PyOxigraph store.
        
        Args:
            store_id: The identifier of the store to close
            
        Returns:
            A dictionary with the result
        """
        try:
            if store_id not in self.stores:
                raise ValueError(f"Store with ID '{store_id}' not found")
            
            # Get the store
            store = self.stores[store_id]
            
            # Close the store - note there's no explicit close method in PyOxigraph,
            # but we'll remove it from our dictionary
            del self.stores[store_id]
            
            # Update default store if needed
            if self.default_store == store_id:
                self.default_store = next(iter(self.stores.keys())) if self.stores else None
                
            return {
                "success": True,
                "message": f"Closed store '{store_id}'"
            }
        except Exception as e:
            logger.error(f"Error closing store: {e}")
            raise
    
    def backup_store(self, store_id: str, backup_path: str) -> Dict[str, Any]:
        """
        Create a backup of a PyOxigraph store.
        
        Args:
            store_id: The identifier of the store to backup
            backup_path: Path where to save the backup
            
        Returns:
            A dictionary with the result
        """
        try:
            if store_id not in self.stores:
                raise ValueError(f"Store with ID '{store_id}' not found")
            
            # Get the store
            store = self.stores[store_id]
            
            # Check if it's an in-memory store
            # This is a heuristic - there's no direct way to check
            if not hasattr(store, 'path') or not store.path:
                raise ValueError("Cannot backup in-memory store")
            
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
                store_path = store.path
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
    
    def restore_store(self, store_id: str, backup_path: str) -> Dict[str, Any]:
        """
        Restore a PyOxigraph store from a backup.
        
        Args:
            store_id: A unique identifier for the restored store
            backup_path: Path of the backup to restore
            
        Returns:
            A dictionary with the result
        """
        try:
            if store_id in self.stores:
                raise ValueError(f"Store with ID '{store_id}' already exists")
                
            backup_path = os.path.expanduser(backup_path)
            
            if not os.path.exists(backup_path):
                raise FileNotFoundError(f"Backup path does not exist: {backup_path}")
            
            # Check if from_backup method is available
            if hasattr(pyoxigraph.Store, 'from_backup'):
                # Create a new store from the backup
                store = pyoxigraph.Store.from_backup(backup_path)
            else:
                # Alternative approach - open as regular store
                store = pyoxigraph.Store(backup_path)
            
            # Store it in our dictionary
            self.stores[store_id] = store
            
            # Set as default if it's the first store
            if self.default_store is None:
                self.default_store = store_id
                
            return {
                "store": store_id
            }
        except Exception as e:
            logger.error(f"Error restoring store: {e}")
            raise
    
    def optimize_store(self, store_id: str) -> Dict[str, Any]:
        """
        Optimize a PyOxigraph store for performance.
        
        Args:
            store_id: The identifier of the store to optimize
            
        Returns:
            A dictionary with the result
        """
        try:
            if store_id not in self.stores:
                raise ValueError(f"Store with ID '{store_id}' not found")
            
            # Get the store
            store = self.stores[store_id]
            
            # Check if optimize method is available
            if hasattr(store, 'optimize'):
                # Optimize the store
                store.optimize()
                return {
                    "success": True,
                    "message": f"Optimized store with ID '{store_id}'"
                }
            else:
                return {
                    "success": True,
                    "message": "Store optimization is not available in this version of PyOxigraph"
                }
        except Exception as e:
            logger.error(f"Error optimizing store: {e}")
            raise
    
    def get_store(self, store_id: Optional[str] = None) -> Optional[pyoxigraph.Store]:
        """
        Get a store by ID or the default store.
        
        Args:
            store_id: The identifier of the store to get, or None for the default
            
        Returns:
            The PyOxigraph Store object or None if not found
        """
        store_id = store_id or self.default_store
        
        if not store_id or store_id not in self.stores:
            # If no store exists, create a default one
            if not self.stores:
                self.create_store("default")
                return self.stores["default"]
            return None
            
        return self.stores[store_id]
    
    def list_stores(self) -> Dict[str, Any]:
        """
        List all open stores.
        
        Returns:
            A dictionary with the list of stores
        """
        return {
            "stores": list(self.stores.keys())
        }
    
    def set_default_store(self, store_id: str) -> Dict[str, Any]:
        """
        Set the default store.
        
        Args:
            store_id: The identifier of the store to set as default
            
        Returns:
            A dictionary with the result
        """
        if store_id not in self.stores:
            raise ValueError(f"Store with ID '{store_id}' not found")
        
        # Set as default
        self.default_store = store_id
        
        return {
            "success": True,
            "message": f"Set store with ID '{store_id}' as default"
        }

# Initialize the global store manager
_store_manager = StoreManager()

# Exposed API functions

def oxigraph_create_store(store_id: str, path: Optional[str] = None) -> Dict[str, Any]:
    """
    Create a new store (in-memory or file-based).
    
    Args:
        store_id: Unique identifier for the store
        path: Optional path for file-based store
        
    Returns:
        Store operation result
    """
    return _store_manager.create_store(store_id, path)


def oxigraph_open_store(store_id: str, path: str) -> Dict[str, Any]:
    """
    Open an existing file-based store.
    
    Args:
        store_id: Unique identifier for the store
        path: Path to the file-based store
        
    Returns:
        Store operation result
    """
    return _store_manager.open_store(store_id, path)


def oxigraph_open_read_only(store_id: str, path: str) -> Dict[str, Any]:
    """
    Open an existing file-based store in read-only mode.
    
    Args:
        store_id: Unique identifier for the store
        path: Path to the file-based store
        
    Returns:
        Store operation result
    """
    return _store_manager.open_store(store_id, path, read_only=True)


def oxigraph_close_store(store_id: str) -> Dict[str, Any]:
    """
    Close a store and remove it from the manager.
    
    Args:
        store_id: Unique identifier for the store
    
    Returns:
        Operation result
    """
    return _store_manager.close_store(store_id)


def oxigraph_backup_store(store_id: str, backup_path: str) -> Dict[str, Any]:
    """
    Create a backup of a store.
    
    Args:
        store_id: Unique identifier for the store
        backup_path: Path where to save the backup
    
    Returns:
        Operation result
    """
    return _store_manager.backup_store(store_id, backup_path)


def oxigraph_restore_store(store_id: str, backup_path: str) -> Dict[str, Any]:
    """
    Restore a store from a backup.
    
    Args:
        store_id: Unique identifier for the store
        backup_path: Path to the backup file
    
    Returns:
        Operation result
    """
    return _store_manager.restore_store(store_id, backup_path)


def oxigraph_optimize_store(store_id: str) -> Dict[str, Any]:
    """
    Optimize a store for better performance.
    
    Args:
        store_id: Unique identifier for the store
    
    Returns:
        Operation result
    """
    return _store_manager.optimize_store(store_id)


def oxigraph_get_store(store_id: Optional[str] = None) -> Optional[pyoxigraph.Store]:
    """
    Get a store by ID.
    
    Args:
        store_id: ID of the store to retrieve (defaults to the default store)
    
    Returns:
        The store instance
    """
    return _store_manager.get_store(store_id)


def oxigraph_list_stores() -> Dict[str, Any]:
    """
    List all managed stores.
    
    Returns:
        List of store IDs
    """
    return _store_manager.list_stores()

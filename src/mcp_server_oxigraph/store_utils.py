#!/usr/bin/env python3
"""
Utility functions for PyOxigraph store management.
These functions wrap around PyOxigraph's Store operations.
"""

import os
import logging
from typing import Dict, List, Any, Optional, Union
from pathlib import Path

try:
    # Import pyoxigraph
    import pyoxigraph as ox
    logger = logging.getLogger(__name__)
    logger.info("Successfully imported pyoxigraph")
except ImportError as e:
    logger.error(f"Failed to import pyoxigraph: {e}")
    print(f"Failed to import pyoxigraph: {e}", file=sys.stderr)


class StoreManager:
    """
    Manages PyOxigraph stores, keeping track of open stores.
    """
    
    def __init__(self):
        self.stores = {}  # Dictionary to keep track of opened stores
        self.default_store = None
    
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
            if path:
                # Create a persistent store
                path = os.path.expanduser(path)
                os.makedirs(os.path.dirname(path), exist_ok=True)
                store = ox.Store(path)
                store_type = "persistent"
            else:
                # Create an in-memory store
                store = ox.Store()
                store_type = "in-memory"
            
            # Store it in our dictionary
            self.stores[store_id] = store
            
            # Set as default if it's the first store
            if self.default_store is None:
                self.default_store = store_id
                
            return {
                "success": True,
                "message": f"Created {store_type} store with ID '{store_id}'",
                "store_id": store_id,
                "store_type": store_type,
                "is_default": store_id == self.default_store
            }
        except Exception as e:
            logger.error(f"Error creating store: {e}")
            return {
                "success": False,
                "message": f"Failed to create store: {str(e)}"
            }
    
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
            path = os.path.expanduser(path)
            
            if not os.path.exists(path):
                return {
                    "success": False,
                    "message": f"Store path does not exist: {path}"
                }
            
            if read_only:
                store = ox.Store.read_only(path)
                mode = "read-only"
            else:
                store = ox.Store(path)
                mode = "read-write"
            
            # Store it in our dictionary
            self.stores[store_id] = store
            
            # Set as default if it's the first store
            if self.default_store is None:
                self.default_store = store_id
                
            return {
                "success": True,
                "message": f"Opened store in {mode} mode with ID '{store_id}'",
                "store_id": store_id,
                "mode": mode,
                "is_default": store_id == self.default_store
            }
        except Exception as e:
            logger.error(f"Error opening store: {e}")
            return {
                "success": False,
                "message": f"Failed to open store: {str(e)}"
            }
    
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
                return {
                    "success": False,
                    "message": f"Store with ID '{store_id}' not found"
                }
            
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
                "message": f"Closed store with ID '{store_id}'",
                "new_default_store": self.default_store
            }
        except Exception as e:
            logger.error(f"Error closing store: {e}")
            return {
                "success": False,
                "message": f"Failed to close store: {str(e)}"
            }
    
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
                return {
                    "success": False,
                    "message": f"Store with ID '{store_id}' not found"
                }
            
            # Get the store
            store = self.stores[store_id]
            
            # Create backup directory
            backup_path = os.path.expanduser(backup_path)
            os.makedirs(os.path.dirname(backup_path), exist_ok=True)
            
            # Backup the store
            store.backup(backup_path)
            
            return {
                "success": True,
                "message": f"Created backup at '{backup_path}'"
            }
        except Exception as e:
            logger.error(f"Error backing up store: {e}")
            return {
                "success": False,
                "message": f"Failed to backup store: {str(e)}"
            }
    
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
            backup_path = os.path.expanduser(backup_path)
            
            if not os.path.exists(backup_path):
                return {
                    "success": False,
                    "message": f"Backup path does not exist: {backup_path}"
                }
            
            # Create a new store from the backup
            store = ox.Store.from_backup(backup_path)
            
            # Store it in our dictionary
            self.stores[store_id] = store
            
            # Set as default if it's the first store
            if self.default_store is None:
                self.default_store = store_id
                
            return {
                "success": True,
                "message": f"Restored store from backup with ID '{store_id}'",
                "store_id": store_id,
                "is_default": store_id == self.default_store
            }
        except Exception as e:
            logger.error(f"Error restoring store: {e}")
            return {
                "success": False,
                "message": f"Failed to restore store: {str(e)}"
            }
    
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
                return {
                    "success": False,
                    "message": f"Store with ID '{store_id}' not found"
                }
            
            # Get the store
            store = self.stores[store_id]
            
            # Optimize the store
            store.optimize()
            
            return {
                "success": True,
                "message": f"Optimized store with ID '{store_id}'"
            }
        except Exception as e:
            logger.error(f"Error optimizing store: {e}")
            return {
                "success": False,
                "message": f"Failed to optimize store: {str(e)}"
            }
    
    def get_store(self, store_id: Optional[str] = None) -> Optional[ox.Store]:
        """
        Get a store by ID or the default store.
        
        Args:
            store_id: The identifier of the store to get, or None for the default
            
        Returns:
            The PyOxigraph Store object or None if not found
        """
        store_id = store_id or self.default_store
        
        if not store_id or store_id not in self.stores:
            return None
            
        return self.stores[store_id]
    
    def list_stores(self) -> Dict[str, Any]:
        """
        List all open stores.
        
        Returns:
            A dictionary with the result
        """
        store_list = [
            {
                "store_id": store_id,
                "is_default": store_id == self.default_store
            }
            for store_id in self.stores
        ]
        
        return {
            "success": True,
            "stores": store_list,
            "default_store": self.default_store,
            "count": len(store_list)
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
            return {
                "success": False,
                "message": f"Store with ID '{store_id}' not found"
            }
        
        # Set as default
        self.default_store = store_id
        
        return {
            "success": True,
            "message": f"Set store with ID '{store_id}' as default"
        }

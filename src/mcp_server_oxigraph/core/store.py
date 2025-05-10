"""
Store management functions for PyOxigraph.

This module provides functions for creating, opening, closing, and managing PyOxigraph stores.
Designed for stateless operation where each function call reopens required stores.
"""

import logging
import os
import sys
import json
import re
from typing import Dict, List, Any, Optional, Union
import pyoxigraph

# Import configuration
from .config import get_default_store_path, get_system_default_store_path, has_user_default_store

logger = logging.getLogger(__name__)

# Constants
REGISTRY_FILE = os.path.expanduser("~/.mcp-server-oxigraph/registry.json")
REGISTRY_DIR = os.path.dirname(REGISTRY_FILE)

# Ensure registry directory exists
os.makedirs(REGISTRY_DIR, exist_ok=True)

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
    if isinstance(path, str) and path.startswith("~"):
        path = os.path.expanduser(path)
    
    # Make absolute
    if isinstance(path, str) and not os.path.isabs(path):
        path = os.path.abspath(path)
    
    # Normalize path separators
    if isinstance(path, str):
        path = os.path.normpath(path)
    
        # Remove trailing slashes that might cause comparison issues
        path = path.rstrip(os.path.sep)
    
        # On case-insensitive file systems, normalize case
        if sys.platform.startswith("win") or sys.platform == "darwin":
            path = path.lower()
    
    return path

# Simple registry functions - read/write a list of store paths
def read_registry() -> Dict[str, Any]:
    """
    Read the registry file.
    
    Returns:
        Dictionary with store_paths and default_store
    """
    try:
        if os.path.exists(REGISTRY_FILE):
            with open(REGISTRY_FILE, 'r') as f:
                registry = json.load(f)
                # Ensure the registry has the right format
                if not isinstance(registry, dict):
                    registry = {'store_paths': [], 'default_store': None}
                if 'store_paths' not in registry:
                    registry['store_paths'] = []
                if 'default_store' not in registry:
                    registry['default_store'] = None
                return registry
    except Exception as e:
        logger.error(f"Failed to read registry: {e}")
    
    # Default if registry doesn't exist or can't be read
    return {
        'store_paths': [],
        'default_store': None
    }

def write_registry(registry: Dict[str, Any]) -> None:
    """
    Write to the registry file.
    
    Args:
        registry: Dictionary with store_paths and default_store
    """
    try:
        # Ensure registry has the right format
        if 'store_paths' not in registry:
            registry['store_paths'] = []
        if 'default_store' not in registry:
            registry['default_store'] = None
            
        with open(REGISTRY_FILE, 'w') as f:
            json.dump(registry, f)
    except Exception as e:
        logger.error(f"Failed to write registry: {e}")

# Get default store path with fallbacks
def get_default_store() -> str:
    """
    Get the default store path with fallbacks.
    
    Returns:
        Path to the default store
    """
    registry = read_registry()
    default_store = registry.get('default_store')
    
    # If a default is set and exists, use it
    if default_store and os.path.exists(default_store):
        return default_store
    
    # Look for user-configured store
    user_path = normalize_path(get_default_store_path())
    if user_path and os.path.exists(user_path):
        return user_path
    
    # Fall back to system default
    system_path = normalize_path(get_system_default_store_path())
    if system_path and os.path.exists(system_path):
        return system_path
    
    # If system path doesn't exist, create it
    try:
        os.makedirs(os.path.dirname(system_path), exist_ok=True)
        store = pyoxigraph.Store(system_path)
        # Just to make sure it's created
        store.add(pyoxigraph.Quad(
            pyoxigraph.NamedNode("http://example.org/subject"),
            pyoxigraph.NamedNode("http://example.org/predicate"),
            pyoxigraph.Literal("initialization")
        ))
        
        # Add to registry
        registry['store_paths'].append(system_path)
        registry['default_store'] = system_path
        write_registry(registry)
        
        return system_path
    except Exception as e:
        logger.error(f"Could not create system store: {e}")
    
    # Last resort - in-memory (though this won't persist)
    return None

# Open a store by path - used by many functions
def open_store(store_path: Optional[str] = None) -> pyoxigraph.Store:
    """
    Open a store by path, with fallback to default.
    
    Args:
        store_path: Path to the store, or None for default
    
    Returns:
        PyOxigraph Store instance
    
    Raises:
        ValueError: If store doesn't exist and can't be created
    """
    # If no specific path, use default
    if store_path is None:
        store_path = get_default_store()
        if not store_path:
            raise ValueError("No default store configured or available")
    
    # Normalize the path
    store_path = normalize_path(store_path)
    
    # Check if store exists and is valid
    try:
        # Try opening the store directly
        store = pyoxigraph.Store(store_path)
        
        # Also add to registry if not already there
        registry = read_registry()
        if store_path not in registry['store_paths']:
            registry['store_paths'].append(store_path)
            
            # If no default store, set this as default
            if not registry['default_store']:
                registry['default_store'] = store_path
                
            write_registry(registry)
            
        return store
    except Exception as e:
        logger.error(f"Failed to open store {store_path}: {e}")
        
        # Try to create the directory structure
        try:
            os.makedirs(os.path.dirname(store_path), exist_ok=True)
            
            # Try again
            store = pyoxigraph.Store(store_path)
            
            # Add to registry
            registry = read_registry()
            if store_path not in registry['store_paths']:
                registry['store_paths'].append(store_path)
                if not registry['default_store']:
                    registry['default_store'] = store_path
                write_registry(registry)
                
            return store
        except Exception as e2:
            logger.error(f"Failed to create and open store {store_path}: {e2}")
            raise ValueError(f"Could not open or create store at {store_path}: {e2}")

# Exposed API functions

def oxigraph_create_store(store_path: str) -> Dict[str, Any]:
    """
    Create a new PyOxigraph store on disk.
    
    Args:
        store_path: Path for the store.
        
    Returns:
        A dictionary with the result
    """
    try:
        # Normalize the path
        store_path = normalize_path(store_path)
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(store_path), exist_ok=True)
        
        # Create the store
        store = pyoxigraph.Store(store_path)
        
        # Add to registry
        registry = read_registry()
        if store_path not in registry['store_paths']:
            registry['store_paths'].append(store_path)
            
            # If no default store, set this as default
            if not registry['default_store']:
                registry['default_store'] = store_path
                
            write_registry(registry)
        
        # Initialize with a test triple to ensure creation
        store.add(pyoxigraph.Quad(
            pyoxigraph.NamedNode("http://example.org/subject"),
            pyoxigraph.NamedNode("http://example.org/predicate"),
            pyoxigraph.Literal("initialization")
        ))
        
        return {
            "message": f"Store created at {store_path}",
            "store": store_path
        }
    except Exception as e:
        logger.error(f"Error creating store: {e}")
        raise ValueError(f"Failed to create store: {e}")

def oxigraph_open_store(store_path: str, read_only: bool = False) -> Dict[str, Any]:
    """
    Open an existing file-based store.
    
    Args:
        store_path: Path to the file-based store
        read_only: Whether to open in read-only mode
        
    Returns:
        Store operation result
    """
    try:
        # Normalize the path
        store_path = normalize_path(store_path)
        
        # Try opening the store
        store = open_store(store_path)
            
        # Add to registry
        registry = read_registry()
        if store_path not in registry['store_paths']:
            registry['store_paths'].append(store_path)
            write_registry(registry)
        
        return {
            "message": f"Store opened at {store_path}",
            "store": store_path
        }
    except Exception as e:
        logger.error(f"Error opening store: {e}")
        raise ValueError(f"Failed to open store: {e}")

def oxigraph_close_store(store_path: str) -> Dict[str, Any]:
    """
    Close a store and remove it from the registry.
    
    Args:
        store_path: Path to the store
    
    Returns:
        Operation result
    """
    try:
        # Normalize the path
        store_path = normalize_path(store_path)
        
        # Update registry
        registry = read_registry()
        if store_path in registry['store_paths']:
            registry['store_paths'].remove(store_path)
            
            # If this was the default, clear the default
            if registry['default_store'] == store_path:
                if registry['store_paths']:
                    registry['default_store'] = registry['store_paths'][0]
                else:
                    registry['default_store'] = None
                    
            write_registry(registry)
        
        return {
            "success": True,
            "message": f"Store at {store_path} removed from registry"
        }
    except Exception as e:
        logger.error(f"Error closing store: {e}")
        raise ValueError(f"Failed to close store: {e}")

def oxigraph_backup_store(store_path: str, backup_path: str) -> Dict[str, Any]:
    """
    Create a backup of a store.
    
    Args:
        store_path: Path to the store
        backup_path: Path where to save the backup
    
    Returns:
        Operation result
    """
    try:
        # Normalize paths
        store_path = normalize_path(store_path)
        backup_path = os.path.expanduser(backup_path)
        
        # Open the store
        store = open_store(store_path)
        
        # Create backup directory if needed
        os.makedirs(os.path.dirname(backup_path), exist_ok=True)
        
        # Manual backup by copying files
        import shutil
        if os.path.isdir(store_path):
            shutil.copytree(store_path, backup_path, dirs_exist_ok=True)
        else:
            shutil.copy2(store_path, backup_path)
        
        return {
            "success": True,
            "message": f"Created backup at {backup_path}"
        }
    except Exception as e:
        logger.error(f"Error backing up store: {e}")
        raise ValueError(f"Failed to backup store: {e}")

def oxigraph_restore_store(backup_path: str, restore_path: str) -> Dict[str, Any]:
    """
    Restore a store from a backup.
    
    Args:
        backup_path: Path to the backup file
        restore_path: Path where to restore the store
    
    Returns:
        Operation result
    """
    try:
        # Normalize paths
        backup_path = os.path.expanduser(backup_path)
        restore_path = normalize_path(restore_path)
        
        # Check if backup exists
        if not os.path.exists(backup_path):
            raise ValueError(f"Backup does not exist at path: {backup_path}")
            
        # Create restore directory if needed
        os.makedirs(os.path.dirname(restore_path), exist_ok=True)
        
        # Manual restore by copying files
        import shutil
        if os.path.isdir(backup_path):
            shutil.copytree(backup_path, restore_path, dirs_exist_ok=True)
        else:
            shutil.copy2(backup_path, restore_path)
                
        # Open to verify
        store = pyoxigraph.Store(restore_path)
            
        # Add to registry
        registry = read_registry()
        if restore_path not in registry['store_paths']:
            registry['store_paths'].append(restore_path)
            write_registry(registry)
        
        return {
            "success": True,
            "message": f"Restored store to {restore_path}",
            "store": restore_path
        }
    except Exception as e:
        logger.error(f"Error restoring store: {e}")
        raise ValueError(f"Failed to restore store: {e}")

def oxigraph_optimize_store(store_path: str) -> Dict[str, Any]:
    """
    Optimize a store for better performance.
    
    Args:
        store_path: Path to the store
    
    Returns:
        Operation result
    """
    try:
        # Open the store
        store = open_store(store_path)
        
        # For now, just return success since optimize isn't implemented
        return {
            "success": True, 
            "message": "Store optimization is not available in this version of PyOxigraph"
        }
    except Exception as e:
        logger.error(f"Error optimizing store: {e}")
        raise ValueError(f"Failed to optimize store: {e}")

def oxigraph_list_stores() -> Dict[str, Any]:
    """
    List all managed stores.
    
    Returns:
        Dictionary with list of store paths and the default store
    """
    try:
        registry = read_registry()
        
        # Filter to only include stores that actually exist
        existing_stores = [
            path for path in registry['store_paths'] 
            if os.path.exists(path)
        ]
        
        # Update registry to remove non-existent stores
        if len(existing_stores) != len(registry['store_paths']):
            registry['store_paths'] = existing_stores
            
            # Check if default store still exists
            if registry['default_store'] not in existing_stores:
                registry['default_store'] = existing_stores[0] if existing_stores else None
                
            write_registry(registry)
        
        return {
            "stores": existing_stores,
            "default": registry['default_store']
        }
    except Exception as e:
        logger.error(f"Error listing stores: {e}")
        raise ValueError(f"Failed to list stores: {e}")

def oxigraph_get_store(store_path: Optional[str] = None) -> pyoxigraph.Store:
    """
    Get a store by path.
    
    Args:
        store_path: Path of the store to retrieve (defaults to the default store)
    
    Returns:
        The store instance
    """
    return open_store(store_path)

# The following RDF functions need to be updated to work with the stateless model

def oxigraph_add(quad: Dict[str, Any], store_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Add a quad to the store.
    
    Args:
        quad: Dictionary representation of the quad to add
        store_path: Path to the store (optional)
    
    Returns:
        Success dictionary
    """
    try:
        # Open the store
        store = open_store(store_path)
        
        # Convert Dict to Quad
        subject = quad['subject']
        predicate = quad['predicate']
        object = quad['object']
        graph_name = quad.get('graph_name')
        
        if subject['type'] == 'NamedNode':
            subject = pyoxigraph.NamedNode(subject['value'])
        elif subject['type'] == 'BlankNode':
            subject = pyoxigraph.BlankNode(subject.get('value'))
            
        if predicate['type'] == 'NamedNode':
            predicate = pyoxigraph.NamedNode(predicate['value'])
            
        if object['type'] == 'NamedNode':
            object = pyoxigraph.NamedNode(object['value'])
        elif object['type'] == 'BlankNode':
            object = pyoxigraph.BlankNode(object.get('value'))
        elif object['type'] == 'Literal':
            datatype = object.get('datatype')
            language = object.get('language')
            object = pyoxigraph.Literal(
                object['value'], 
                datatype=None if not datatype else pyoxigraph.NamedNode(datatype),
                language=language
            )
            
        if graph_name:
            if graph_name['type'] == 'NamedNode':
                graph_name = pyoxigraph.NamedNode(graph_name['value'])
            elif graph_name['type'] == 'BlankNode':
                graph_name = pyoxigraph.BlankNode(graph_name.get('value'))
                
        quad_obj = pyoxigraph.Quad(
            subject,
            predicate,
            object,
            graph_name
        )
        
        # Add quad to store
        store.add(quad_obj)
        
        return {
            "success": True,
            "message": "Quad added successfully"
        }
    except Exception as e:
        logger.error(f"Error adding quad: {e}")
        raise ValueError(f"Failed to add quad: {e}")

def oxigraph_add_many(quads: List[Dict[str, Any]], store_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Add multiple quads to the store.
    
    Args:
        quads: List of quad dictionaries
        store_path: Path to the store (optional)
    
    Returns:
        Success dictionary
    """
    try:
        # Open the store
        store = open_store(store_path)
        
        # Process each quad
        count = 0
        for quad in quads:
            try:
                # Convert Dict to Quad (same logic as oxigraph_add)
                subject = quad['subject']
                predicate = quad['predicate']
                object = quad['object']
                graph_name = quad.get('graph_name')
                
                if subject['type'] == 'NamedNode':
                    subject = pyoxigraph.NamedNode(subject['value'])
                elif subject['type'] == 'BlankNode':
                    subject = pyoxigraph.BlankNode(subject.get('value'))
                    
                if predicate['type'] == 'NamedNode':
                    predicate = pyoxigraph.NamedNode(predicate['value'])
                    
                if object['type'] == 'NamedNode':
                    object = pyoxigraph.NamedNode(object['value'])
                elif object['type'] == 'BlankNode':
                    object = pyoxigraph.BlankNode(object.get('value'))
                elif object['type'] == 'Literal':
                    datatype = object.get('datatype')
                    language = object.get('language')
                    object = pyoxigraph.Literal(
                        object['value'], 
                        datatype=None if not datatype else pyoxigraph.NamedNode(datatype),
                        language=language
                    )
                    
                if graph_name:
                    if graph_name['type'] == 'NamedNode':
                        graph_name = pyoxigraph.NamedNode(graph_name['value'])
                    elif graph_name['type'] == 'BlankNode':
                        graph_name = pyoxigraph.BlankNode(graph_name.get('value'))
                        
                quad_obj = pyoxigraph.Quad(
                    subject,
                    predicate,
                    object,
                    graph_name
                )
                
                # Add quad to store
                store.add(quad_obj)
                count += 1
            except Exception as e:
                logger.error(f"Error adding quad: {e}")
                # Continue with other quads
        
        return {
            "success": True,
            "message": f"Added {count} quads",
            "count": count
        }
    except Exception as e:
        logger.error(f"Error adding quads: {e}")
        raise ValueError(f"Failed to add quads: {e}")

def oxigraph_remove(quad: Dict[str, Any], store_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Remove a quad from the store.
    
    Args:
        quad: Dictionary representation of the quad to remove
        store_path: Path to the store (optional)
    
    Returns:
        Success dictionary
    """
    try:
        # Open the store
        store = open_store(store_path)
        
        # Convert Dict to Quad (same logic as oxigraph_add)
        subject = quad['subject']
        predicate = quad['predicate']
        object = quad['object']
        graph_name = quad.get('graph_name')
        
        if subject['type'] == 'NamedNode':
            subject = pyoxigraph.NamedNode(subject['value'])
        elif subject['type'] == 'BlankNode':
            subject = pyoxigraph.BlankNode(subject.get('value'))
            
        if predicate['type'] == 'NamedNode':
            predicate = pyoxigraph.NamedNode(predicate['value'])
            
        if object['type'] == 'NamedNode':
            object = pyoxigraph.NamedNode(object['value'])
        elif object['type'] == 'BlankNode':
            object = pyoxigraph.BlankNode(object.get('value'))
        elif object['type'] == 'Literal':
            datatype = object.get('datatype')
            language = object.get('language')
            object = pyoxigraph.Literal(
                object['value'], 
                datatype=None if not datatype else pyoxigraph.NamedNode(datatype),
                language=language
            )
            
        if graph_name:
            if graph_name['type'] == 'NamedNode':
                graph_name = pyoxigraph.NamedNode(graph_name['value'])
            elif graph_name['type'] == 'BlankNode':
                graph_name = pyoxigraph.BlankNode(graph_name.get('value'))
                
        quad_obj = pyoxigraph.Quad(
            subject,
            predicate,
            object,
            graph_name
        )
        
        # Remove quad from store
        store.remove(quad_obj)
        
        return {
            "success": True,
            "message": "Quad removed successfully"
        }
    except Exception as e:
        logger.error(f"Error removing quad: {e}")
        raise ValueError(f"Failed to remove quad: {e}")

def oxigraph_remove_many(quads: List[Dict[str, Any]], store_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Remove multiple quads from the store.
    
    Args:
        quads: List of quad dictionaries
        store_path: Path to the store (optional)
    
    Returns:
        Success dictionary
    """
    try:
        # Open the store
        store = open_store(store_path)
        
        # Process each quad
        count = 0
        for quad in quads:
            try:
                # Convert Dict to Quad (same logic as oxigraph_add)
                subject = quad['subject']
                predicate = quad['predicate']
                object = quad['object']
                graph_name = quad.get('graph_name')
                
                if subject['type'] == 'NamedNode':
                    subject = pyoxigraph.NamedNode(subject['value'])
                elif subject['type'] == 'BlankNode':
                    subject = pyoxigraph.BlankNode(subject.get('value'))
                    
                if predicate['type'] == 'NamedNode':
                    predicate = pyoxigraph.NamedNode(predicate['value'])
                    
                if object['type'] == 'NamedNode':
                    object = pyoxigraph.NamedNode(object['value'])
                elif object['type'] == 'BlankNode':
                    object = pyoxigraph.BlankNode(object.get('value'))
                elif object['type'] == 'Literal':
                    datatype = object.get('datatype')
                    language = object.get('language')
                    object = pyoxigraph.Literal(
                        object['value'], 
                        datatype=None if not datatype else pyoxigraph.NamedNode(datatype),
                        language=language
                    )
                    
                if graph_name:
                    if graph_name['type'] == 'NamedNode':
                        graph_name = pyoxigraph.NamedNode(graph_name['value'])
                    elif graph_name['type'] == 'BlankNode':
                        graph_name = pyoxigraph.BlankNode(graph_name.get('value'))
                        
                quad_obj = pyoxigraph.Quad(
                    subject,
                    predicate,
                    object,
                    graph_name
                )
                
                # Remove quad from store
                store.remove(quad_obj)
                count += 1
            except Exception as e:
                logger.error(f"Error removing quad: {e}")
                # Continue with other quads
        
        return {
            "success": True,
            "message": f"Removed {count} quads",
            "count": count
        }
    except Exception as e:
        logger.error(f"Error removing quads: {e}")
        raise ValueError(f"Failed to remove quads: {e}")

def oxigraph_clear(store_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Remove all quads from the store.
    
    Args:
        store_path: Path to the store (optional)
    
    Returns:
        Success dictionary
    """
    try:
        # Open the store
        store = open_store(store_path)
        
        # Get all quads
        quads = list(store.quads_for_pattern(None, None, None, None))
        
        # Remove each quad
        for quad in quads:
            store.remove(quad)
        
        return {
            "success": True,
            "message": f"Cleared {len(quads)} quads from store",
            "count": len(quads)
        }
    except Exception as e:
        logger.error(f"Error clearing store: {e}")
        raise ValueError(f"Failed to clear store: {e}")

def oxigraph_quads_for_pattern(
    subject: Optional[Dict[str, Any]] = None, 
    predicate: Optional[Dict[str, Any]] = None, 
    object: Optional[Dict[str, Any]] = None, 
    graph_name: Optional[Dict[str, Any]] = None,
    store_path: Optional[str] = None
) -> Dict[str, Any]:
    """
    Query for quads matching a pattern.
    
    Args:
        subject: Subject to match (optional)
        predicate: Predicate to match (optional)
        object: Object to match (optional)
        graph_name: Graph name to match (optional)
        store_path: Path to the store (optional)
    
    Returns:
        Dictionary with matching quads
    """
    try:
        # Open the store
        store = open_store(store_path)
        
        # Convert Dict objects to PyOxigraph objects if provided
        subj = None
        if subject:
            if subject['type'] == 'NamedNode':
                subj = pyoxigraph.NamedNode(subject['value'])
            elif subject['type'] == 'BlankNode':
                subj = pyoxigraph.BlankNode(subject.get('value'))
                
        pred = None
        if predicate:
            if predicate['type'] == 'NamedNode':
                pred = pyoxigraph.NamedNode(predicate['value'])
                
        obj = None
        if object:
            if object['type'] == 'NamedNode':
                obj = pyoxigraph.NamedNode(object['value'])
            elif object['type'] == 'BlankNode':
                obj = pyoxigraph.BlankNode(object.get('value'))
            elif object['type'] == 'Literal':
                datatype = object.get('datatype')
                language = object.get('language')
                obj = pyoxigraph.Literal(
                    object['value'], 
                    datatype=None if not datatype else pyoxigraph.NamedNode(datatype),
                    language=language
                )
                
        graph = None
        if graph_name:
            if graph_name['type'] == 'NamedNode':
                graph = pyoxigraph.NamedNode(graph_name['value'])
            elif graph_name['type'] == 'BlankNode':
                graph = pyoxigraph.BlankNode(graph_name.get('value'))
                
        # Query the store
        matching_quads = list(store.quads_for_pattern(
            subject=subj,
            predicate=pred,
            object=obj,
            graph_name=graph
        ))
        
        # Convert PyOxigraph Quads to dictionaries
        result_quads = []
        for quad in matching_quads:
            q_dict = {
                "type": "Quad",
                "subject": _node_to_dict(quad.subject),
                "predicate": _node_to_dict(quad.predicate),
                "object": _node_to_dict(quad.object)
            }
            if quad.graph_name:
                q_dict["graph_name"] = _node_to_dict(quad.graph_name)
                
            result_quads.append(q_dict)
        
        return {
            "quads": result_quads,
            "count": len(result_quads)
        }
    except Exception as e:
        logger.error(f"Error querying quads: {e}")
        raise ValueError(f"Failed to query quads: {e}")

def _node_to_dict(node):
    """Helper to convert PyOxigraph nodes to dictionaries."""
    if isinstance(node, pyoxigraph.NamedNode):
        return {
            "type": "NamedNode",
            "value": str(node.value)
        }
    elif isinstance(node, pyoxigraph.BlankNode):
        return {
            "type": "BlankNode",
            "value": str(node.value) if hasattr(node, 'value') else None
        }
    elif isinstance(node, pyoxigraph.Literal):
        result = {
            "type": "Literal",
            "value": str(node.value)
        }
        if node.datatype:
            result["datatype"] = str(node.datatype)
        if node.language:
            result["language"] = str(node.language)
        return result
    return None

def oxigraph_query(query: str, store_path: Optional[str] = None) -> Any:
    """
    Execute a SPARQL query against the store.
    
    Args:
        query: SPARQL query string
        store_path: Path to the store (optional)
    
    Returns:
        Query results
    """
    try:
        # Open the store
        store = open_store(store_path)
        
        # Execute the query
        results = store.query(query)
        
        # Handle different result types
        if isinstance(results, bool) or str(results).startswith("<QueryBoolean"):
            # ASK query
            if isinstance(results, bool):
                return {"result": results}
            else:
                # Extract boolean from string representation
                result_str = str(results).lower()
                if "true" in result_str:
                    return {"result": True}
                elif "false" in result_str:
                    return {"result": False}
                else:
                    return {"result": str(results)}
        else:
            # SELECT query
            # Results is an iterator of QuerySolution objects
            solutions = []
            
            for solution in results:
                # In PyOxigraph 0.4.9, QuerySolution objects act like dictionaries
                # with variable names as keys
                solution_dict = {}
                
                try:
                    # Try the dictionary access method (PyOxigraph 0.4.9)
                    if hasattr(solution, 'items'):
                        for var_name, term in solution.items():
                            solution_dict[var_name] = _node_to_dict(term)
                    # If no items() method, try direct key access for each variable
                    else:
                        # Extract variable names from query (simple approach)
                        import re
                        var_pattern = re.compile(r'\?([a-zA-Z0-9_]+)')
                        var_names = var_pattern.findall(query)
                        
                        for var_name in var_names:
                            try:
                                # Try dictionary-style access
                                term = solution[var_name]
                                solution_dict[var_name] = _node_to_dict(term)
                            except (KeyError, TypeError):
                                # If fails, try attribute access
                                if hasattr(solution, var_name):
                                    term = getattr(solution, var_name)
                                    solution_dict[var_name] = _node_to_dict(term)
                except Exception as e:
                    logger.debug(f"Error accessing variables: {e}")
                
                # Last resort fallback: parse the string representation
                if not solution_dict:
                    solution_str = str(solution)
                    logger.debug(f"Falling back to string parsing: {solution_str}")
                    
                    # Extract var=value pairs from string representation
                    pairs = solution_str.strip('<>').split(' ', 1)[1]  # Remove "QuerySolution" prefix
                    var_value_pairs = pairs.split(' ', 1)
                    
                    for pair in var_value_pairs:
                        if '=' in pair:
                            var_name, value_repr = pair.split('=', 1)
                            # Extract term type and value
                            if value_repr.startswith('<NamedNode'):
                                value = value_repr.split('value=', 1)[1].strip('>')
                                solution_dict[var_name] = {"type": "NamedNode", "value": value}
                            elif value_repr.startswith('<Literal'):
                                parts = value_repr.split('value=', 1)[1]
                                value = parts.split(' ', 1)[0] if ' ' in parts else parts.strip('>')
                                solution_dict[var_name] = {"type": "Literal", "value": value}
                                
                                # Check for datatype or language
                                if 'datatype=' in parts:
                                    datatype = parts.split('datatype=', 1)[1]
                                    datatype = datatype.split(' ', 1)[0] if ' ' in datatype else datatype.strip('>')
                                    solution_dict[var_name]["datatype"] = datatype
                                if 'language=' in parts:
                                    language = parts.split('language=', 1)[1]
                                    language = language.split(' ', 1)[0] if ' ' in language else language.strip('>')
                                    solution_dict[var_name]["language"] = language
                            elif value_repr.startswith('<BlankNode'):
                                if 'value=' in value_repr:
                                    value = value_repr.split('value=', 1)[1].strip('>')
                                    solution_dict[var_name] = {"type": "BlankNode", "value": value}
                                else:
                                    solution_dict[var_name] = {"type": "BlankNode", "value": None}
                
                # If all attempts fail, just store the raw string
                if not solution_dict:
                    solution_dict["result"] = str(solution)
                
                solutions.append(solution_dict)
            
            return solutions
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        raise ValueError(f"Failed to execute query: {e}")

def oxigraph_update(update: str, store_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Execute a SPARQL update against the store.
    
    Args:
        update: SPARQL update string
        store_path: Path to the store (optional)
    
    Returns:
        Success dictionary
    """
    try:
        # Open the store
        store = open_store(store_path)
        
        # Execute the update
        store.update(update)
        
        return {
            "success": True,
            "message": "Update executed successfully"
        }
    except Exception as e:
        logger.error(f"Error executing update: {e}")
        raise ValueError(f"Failed to execute update: {e}")

def oxigraph_run_query(query: str, store_path: Optional[str] = None) -> Any:
    """
    Run a SPARQL query or update against the store.
    
    Args:
        query: SPARQL query or update string
        store_path: Path to the store (optional)
        
    Returns:
        Query results or success dictionary
    """
    try:
        # Detect if it's a query or update based on the first token
        query = query.strip()
        first_token = query.split()[0].upper()
        
        if first_token in ['SELECT', 'ASK', 'CONSTRUCT', 'DESCRIBE']:
            # It's a query
            return oxigraph_query(query, store_path)
        else:
            # It's an update
            return oxigraph_update(query, store_path)
    except Exception as e:
        logger.error(f"Error running query: {e}")
        raise ValueError(f"Failed to run query: {e}")

def oxigraph_query_with_options(
    query: str, 
    default_graph_uris: Optional[List[str]] = None, 
    named_graph_uris: Optional[List[str]] = None, 
    use_default_graph_as_union: bool = False,
    store_path: Optional[str] = None
) -> Any:
    """
    Execute a SPARQL query with custom options.
    
    Args:
        query: SPARQL query string
        default_graph_uris: Optional list of IRIs to use as default graphs
        named_graph_uris: Optional list of IRIs to use as named graphs
        use_default_graph_as_union: Whether to use default graph as union
        store_path: Path to the store (optional)
    
    Returns:
        Query results
    """
    try:
        # This is a placeholder - PyOxigraph might not support all these options
        # Fallback to basic query if options not supported
        return oxigraph_query(query, store_path)
    except Exception as e:
        logger.error(f"Error executing query with options: {e}")
        raise ValueError(f"Failed to execute query with options: {e}")

def oxigraph_prepare_query(query_template: str) -> Dict[str, Any]:
    """
    Prepare a SPARQL query template.
    
    Args:
        query_template: SPARQL query template
    
    Returns:
        Dictionary with prepared query ID
    """
    # This is a placeholder - implementation depends on PyOxigraph capabilities
    return {
        "prepared_query_id": "query1",
        "message": "Query preparation not fully implemented"
    }

def oxigraph_execute_prepared_query(
    prepared_query_id: str,
    parameters: Dict[str, Any],
    store_path: Optional[str] = None
) -> Any:
    """
    Execute a prepared SPARQL query.
    
    Args:
        prepared_query_id: ID of the prepared query
        parameters: Query parameters
        store_path: Path to the store (optional)
    
    Returns:
        Query results
    """
    # This is a placeholder - implementation depends on PyOxigraph capabilities
    return {
        "message": "Prepared query execution not fully implemented"
    }

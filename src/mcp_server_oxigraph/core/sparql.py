"""
SPARQL query and update functions for PyOxigraph.

This module provides functions for executing SPARQL queries and updates
against PyOxigraph stores.
"""

import logging
from typing import Dict, List, Any, Optional, Union
import pyoxigraph

# Configure logging
logger = logging.getLogger(__name__)

# Export query functions from store.py
from .store import (
    oxigraph_query,
    oxigraph_update,
    oxigraph_query_with_options,
    oxigraph_prepare_query,
    oxigraph_execute_prepared_query,
    oxigraph_run_query
)

# Additional utility functions

def oxigraph_explain_query(query: str, store_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Get an explanation of a SPARQL query.
    
    Args:
        query: The SPARQL query string
        store_path: Optional path to the store
        
    Returns:
        Explanation of the query
    """
    try:
        # This is a placeholder - PyOxigraph might not provide query explanation
        # Simply return a basic analysis of the query
        
        # Detect query type
        query = query.strip()
        query_type = "UNKNOWN"
        
        if query.upper().startswith("SELECT"):
            query_type = "SELECT"
        elif query.upper().startswith("ASK"):
            query_type = "ASK"
        elif query.upper().startswith("CONSTRUCT"):
            query_type = "CONSTRUCT"
        elif query.upper().startswith("DESCRIBE"):
            query_type = "DESCRIBE"
        elif any(query.upper().startswith(p) for p in ["INSERT", "DELETE", "CLEAR", "LOAD", "CREATE", "DROP", "COPY"]):
            query_type = "UPDATE"
        
        # Count triple patterns (very basic)
        triple_patterns = query.count("{") + query.count(".")
        
        return {
            "query_type": query_type,
            "estimated_complexity": "MEDIUM" if triple_patterns > 5 else "LOW",
            "note": "Full query explanation not available in this version"
        }
    except Exception as e:
        logger.error(f"Error explaining query: {e}")
        raise ValueError(f"Failed to explain query: {e}")

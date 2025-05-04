"""
SPARQL Query functions for PyOxigraph.

This module provides functions for executing SPARQL queries and updates against a PyOxigraph store.
"""

import logging
from typing import Dict, List, Optional, Any, Union
import pyoxigraph
from .store import oxigraph_get_store

logger = logging.getLogger(__name__)

# Prepared queries storage
_PREPARED_QUERIES = {}


def oxigraph_query(query: str, store_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Execute a SPARQL query against the store.
    
    Args:
        query: SPARQL query string
        store_id: Optional ID of the store to use. If None, uses the default store.
    
    Returns:
        Query results dictionary
    """
    store = oxigraph_get_store(store_id)
    
    if not store:
        raise ValueError(f"Store with ID '{store_id}' not found")
    
    try:
        results = store.query(query)
        
        # Convert results to a JSON-serializable format
        result_list = []
        
        for result in results:
            if hasattr(result, "items"):
                # Handle solution bindings (SELECT queries)
                solution_dict = {}
                
                for var, value in result.items():
                    # Convert each value to a JSON-serializable format
                    solution_dict[var] = _term_to_dict(value)
                
                result_list.append(solution_dict)
            else:
                # Handle other result types (ASK, CONSTRUCT, DESCRIBE)
                # For now, return an error
                result_list.append({
                    "error": f"Unknown node type: {type(result)}"
                })
        
        return {
            "success": True,
            "results": result_list
        }
    
    except Exception as e:
        logger.error(f"Error executing SPARQL query: {e}")
        raise


def oxigraph_update(update: str, store_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Execute a SPARQL update against the store.
    
    Args:
        update: SPARQL update string
        store_id: Optional ID of the store to use. If None, uses the default store.
    
    Returns:
        Success dictionary
    """
    store = oxigraph_get_store(store_id)
    
    if not store:
        raise ValueError(f"Store with ID '{store_id}' not found")
    
    try:
        store.update(update)
        
        return {
            "success": True,
            "message": "Update executed successfully"
        }
    
    except Exception as e:
        logger.error(f"Error executing SPARQL update: {e}")
        raise


def oxigraph_query_with_options(query: str, 
                             default_graph_uris: Optional[List[str]] = None,
                             named_graph_uris: Optional[List[str]] = None,
                             use_default_graph_as_union: bool = False,
                             store_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Execute a SPARQL query with custom options.
    
    Args:
        query: SPARQL query string
        default_graph_uris: Optional list of IRIs to use as the default graph
        named_graph_uris: Optional list of IRIs to use as named graphs
        use_default_graph_as_union: Whether to use the default graph as the union of all graphs
        store_id: Optional ID of the store to use. If None, uses the default store.
    
    Returns:
        Query results dictionary
    """
    # Note: PyOxigraph's Python bindings don't support these options directly,
    # so we'll need to implement this functionality by modifying the query
    
    # For now, this is a placeholder that just forwards to the standard query function
    logger.warning("Query options not fully supported in PyOxigraph Python bindings")
    
    return oxigraph_query(query, store_id)


def oxigraph_prepare_query(query_template: str) -> Dict[str, Any]:
    """
    Prepare a SPARQL query template for later execution.
    
    Args:
        query_template: SPARQL query template with placeholders for parameters
    
    Returns:
        Dictionary with prepared query ID
    """
    global _PREPARED_QUERIES
    
    try:
        # Generate a unique ID for this prepared query
        query_id = f"query_{len(_PREPARED_QUERIES) + 1}"
        
        # Store the query template
        _PREPARED_QUERIES[query_id] = query_template
        
        return {
            "prepared_query_id": query_id,
            "message": "Query prepared successfully"
        }
    
    except Exception as e:
        logger.error(f"Error preparing query: {e}")
        raise


def oxigraph_execute_prepared_query(prepared_query_id: str, 
                                   parameters: Dict[str, Any],
                                   store_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Execute a previously prepared SPARQL query.
    
    Args:
        prepared_query_id: ID of the prepared query
        parameters: Dictionary of parameter names and values to substitute in the query
        store_id: Optional ID of the store to use. If None, uses the default store.
    
    Returns:
        Query results dictionary
    """
    global _PREPARED_QUERIES
    
    if prepared_query_id not in _PREPARED_QUERIES:
        raise ValueError(f"Prepared query with ID '{prepared_query_id}' not found")
    
    try:
        # Get the query template
        query_template = _PREPARED_QUERIES[prepared_query_id]
        
        # Substitute parameters
        query = query_template
        for name, value in parameters.items():
            placeholder = f"${{{name}}}"
            
            if isinstance(value, str):
                # For string values, wrap in quotes
                if value.startswith("http://") or value.startswith("https://"):
                    # IRIs should be wrapped in < >
                    query = query.replace(placeholder, f"<{value}>")
                else:
                    # Literals should be wrapped in quotes
                    query = query.replace(placeholder, f'"{value}"')
            elif isinstance(value, bool):
                # Boolean values
                query = query.replace(placeholder, str(value).lower())
            elif value is None:
                # NULL values
                query = query.replace(placeholder, "UNDEF")
            else:
                # Other values (numbers, etc.)
                query = query.replace(placeholder, str(value))
        
        # Execute the query
        return oxigraph_query(query, store_id)
    
    except Exception as e:
        logger.error(f"Error executing prepared query: {e}")
        raise


def oxigraph_run_query(query: str) -> Dict[str, Any]:
    """
    Run a query against Oxigraph.
    This can be any query type supported by Oxigraph (SPARQL, etc.)
    
    Args:
        query: The query string
        
    Returns:
        Query results
    """
    # For now, this is just an alias for oxigraph_query
    return oxigraph_query(query)


# Helper functions

def _term_to_dict(term):
    """
    Convert a PyOxigraph term to a dictionary representation.
    
    Args:
        term: PyOxigraph term (NamedNode, BlankNode, Literal, or DefaultGraph)
        
    Returns:
        Dictionary representation of the term
    """
    try:
        if isinstance(term, pyoxigraph.NamedNode):
            return {
                "type": "NamedNode",
                "value": str(term.value)
            }
        elif isinstance(term, pyoxigraph.BlankNode):
            return {
                "type": "BlankNode",
                "value": str(term.value) if term.value else ""
            }
        elif isinstance(term, pyoxigraph.Literal):
            result = {
                "type": "Literal",
                "value": str(term.value)
            }
            
            if term.datatype:
                result["datatype"] = str(term.datatype.value)
            
            if term.language:
                result["language"] = term.language
            
            return result
        else:
            # Handle the DefaultGraph case or any other types
            return {
                "error": f"Unknown node type: {type(term)}"
            }
    except Exception as e:
        logger.error(f"Error converting term to dict: {e}")
        return {
            "error": f"Error: {str(e)}"
        }

"""
RDF node creation functions for PyOxigraph.

This module provides functions for creating RDF nodes, which are the building blocks
of RDF statements.
"""

import logging
from typing import Dict, List, Any, Optional, Union
import pyoxigraph

# Configure logging
logger = logging.getLogger(__name__)

def oxigraph_create_named_node(iri: str) -> Dict[str, Any]:
    """
    Create a NamedNode (IRI) for use in RDF statements.
    
    Args:
        iri: The IRI string for the node
    
    Returns:
        Dictionary representing the Named Node
    """
    try:
        node = pyoxigraph.NamedNode(iri)
        return {
            "type": "NamedNode",
            "value": iri
        }
    except Exception as e:
        logger.error(f"Error creating named node: {e}")
        raise ValueError(f"Failed to create named node: {e}")

def oxigraph_create_blank_node(id: Optional[str] = None) -> Dict[str, Any]:
    """
    Create a BlankNode for use in RDF statements.
    
    Args:
        id: Optional identifier for the blank node
    
    Returns:
        Dictionary representing the Blank Node
    """
    try:
        if id:
            node = pyoxigraph.BlankNode(id)
            node_value = id
        else:
            node = pyoxigraph.BlankNode()
            node_value = str(node)
            # If PyOxigraph doesn't provide a way to get the value, 
            # we'll use the string representation
            
        return {
            "type": "BlankNode",
            "value": node_value
        }
    except Exception as e:
        logger.error(f"Error creating blank node: {e}")
        raise ValueError(f"Failed to create blank node: {e}")

def oxigraph_create_literal(
    value: str, 
    datatype: Optional[str] = None, 
    language: Optional[str] = None
) -> Dict[str, Any]:
    """
    Create a Literal for use in RDF statements.
    
    Args:
        value: The string value
        datatype: Optional datatype IRI
        language: Optional language tag
    
    Returns:
        Dictionary representing the Literal
    """
    try:
        # Create the datatype node if provided
        datatype_node = None
        if datatype:
            datatype_node = pyoxigraph.NamedNode(datatype)
            
        # Create the literal
        node = pyoxigraph.Literal(value, datatype=datatype_node, language=language)
        
        # Create response
        result = {
            "type": "Literal",
            "value": value
        }
        
        if datatype:
            result["datatype"] = datatype
            
        if language:
            result["language"] = language
            
        return result
    except Exception as e:
        logger.error(f"Error creating literal: {e}")
        raise ValueError(f"Failed to create literal: {e}")

def oxigraph_create_quad(
    subject: Dict[str, Any],
    predicate: Dict[str, Any],
    object: Dict[str, Any],
    graph_name: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Create a Quad (triple with optional graph) for use in RDF statements.
    
    Args:
        subject: Dictionary representing a NamedNode or BlankNode
        predicate: Dictionary representing a NamedNode
        object: Dictionary representing a NamedNode, BlankNode, or Literal
        graph_name: Optional dictionary representing the graph name (NamedNode or BlankNode)
    
    Returns:
        Dictionary representing the Quad
    """
    try:
        # Validate basic types
        if not isinstance(subject, dict) or subject.get('type') not in ['NamedNode', 'BlankNode']:
            raise ValueError("Subject must be a NamedNode or BlankNode")
            
        if not isinstance(predicate, dict) or predicate.get('type') != 'NamedNode':
            raise ValueError("Predicate must be a NamedNode")
            
        if not isinstance(object, dict) or object.get('type') not in ['NamedNode', 'BlankNode', 'Literal']:
            raise ValueError("Object must be a NamedNode, BlankNode, or Literal")
            
        if graph_name and (not isinstance(graph_name, dict) or graph_name.get('type') not in ['NamedNode', 'BlankNode']):
            raise ValueError("Graph name must be a NamedNode or BlankNode")
            
        # Create the quad dictionary
        quad = {
            "type": "Quad",
            "subject": subject,
            "predicate": predicate,
            "object": object
        }
        
        if graph_name:
            quad["graph_name"] = graph_name
            
        return quad
    except Exception as e:
        logger.error(f"Error creating quad: {e}")
        raise ValueError(f"Failed to create quad: {e}")

# Export additional functions from store.py that are RDF-related
from .store import (
    oxigraph_add,
    oxigraph_add_many,
    oxigraph_remove,
    oxigraph_remove_many,
    oxigraph_clear,
    oxigraph_quads_for_pattern
)

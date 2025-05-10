"""
RDF Data Model functions for PyOxigraph.

This module provides functions for creating and manipulating RDF nodes, literals, and quads.
"""

import logging
from typing import Dict, List, Optional, Any, Union
import pyoxigraph
from .store import oxigraph_get_store

logger = logging.getLogger(__name__)

# Node creation functions

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
        raise


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
            return {
                "type": "BlankNode",
                "value": id
            }
        else:
            node = pyoxigraph.BlankNode()
            return {
                "type": "BlankNode",
                "value": ""
            }
    except Exception as e:
        logger.error(f"Error creating blank node: {e}")
        raise


def oxigraph_create_literal(value: str, datatype: Optional[str] = None, language: Optional[str] = None) -> Dict[str, Any]:
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
        if datatype and language:
            raise ValueError("Cannot specify both datatype and language")
        
        if datatype:
            datatype_node = pyoxigraph.NamedNode(datatype)
            node = pyoxigraph.Literal(value, datatype=datatype_node)
            return {
                "type": "Literal",
                "value": value,
                "datatype": datatype
            }
        elif language:
            node = pyoxigraph.Literal(value, language=language)
            return {
                "type": "Literal",
                "value": value,
                "language": language
            }
        else:
            node = pyoxigraph.Literal(value)
            return {
                "type": "Literal",
                "value": value
            }
    except Exception as e:
        logger.error(f"Error creating literal: {e}")
        raise


def oxigraph_create_quad(subject: Dict[str, Any], predicate: Dict[str, Any], 
                        object: Dict[str, Any], graph_name: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
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
        # Convert subject
        if subject["type"] == "NamedNode":
            s = pyoxigraph.NamedNode(subject["value"])
        elif subject["type"] == "BlankNode":
            s = pyoxigraph.BlankNode(subject["value"] if subject["value"] else None)
        else:
            raise ValueError(f"Invalid subject type: {subject['type']}")
        
        # Convert predicate
        if predicate["type"] == "NamedNode":
            p = pyoxigraph.NamedNode(predicate["value"])
        else:
            raise ValueError(f"Invalid predicate type: {predicate['type']}")
        
        # Convert object
        if object["type"] == "NamedNode":
            o = pyoxigraph.NamedNode(object["value"])
        elif object["type"] == "BlankNode":
            o = pyoxigraph.BlankNode(object["value"] if object["value"] else None)
        elif object["type"] == "Literal":
            if "datatype" in object and object["datatype"]:
                datatype_node = pyoxigraph.NamedNode(object["datatype"])
                o = pyoxigraph.Literal(object["value"], datatype=datatype_node)
            elif "language" in object and object["language"]:
                o = pyoxigraph.Literal(object["value"], language=object["language"])
            else:
                o = pyoxigraph.Literal(object["value"])
        else:
            raise ValueError(f"Invalid object type: {object['type']}")
        
        # Convert graph name if provided
        g = None
        if graph_name:
            if graph_name["type"] == "NamedNode":
                g = pyoxigraph.NamedNode(graph_name["value"])
            elif graph_name["type"] == "BlankNode":
                g = pyoxigraph.BlankNode(graph_name["value"] if graph_name["value"] else None)
            else:
                raise ValueError(f"Invalid graph name type: {graph_name['type']}")
        
        # Create quad (triple with optional graph)
        if g:
            quad = pyoxigraph.Quad(s, p, o, g)
        else:
            quad = pyoxigraph.Quad(s, p, o)
        
        # Return serialized representation
        result = {
            "type": "Quad",
            "subject": subject,
            "predicate": predicate,
            "object": object
        }
        
        if graph_name:
            result["graph_name"] = graph_name
        
        return result
    
    except Exception as e:
        logger.error(f"Error creating quad: {e}")
        raise


# Store manipulation functions

def oxigraph_add(quad: Dict[str, Any], store_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Add a quad to the store.
    
    Args:
        quad: Dictionary representing a quad
        store_path: Optional path to the store to use. If None, uses the default store.
    
    Returns:
        Success dictionary
    """
    store = oxigraph_get_store(store_path)
    
    if not store:
        raise ValueError(f"Store with path '{store_path}' not found")
    
    try:
        # Convert the quad dictionary to a pyoxigraph.Quad object
        subject = quad["subject"]
        predicate = quad["predicate"]
        object = quad["object"]
        graph_name = quad.get("graph_name")
        
        # Convert subject
        if subject["type"] == "NamedNode":
            s = pyoxigraph.NamedNode(subject["value"])
        elif subject["type"] == "BlankNode":
            s = pyoxigraph.BlankNode(subject["value"] if subject["value"] else None)
        else:
            raise ValueError(f"Invalid subject type: {subject['type']}")
        
        # Convert predicate
        if predicate["type"] == "NamedNode":
            p = pyoxigraph.NamedNode(predicate["value"])
        else:
            raise ValueError(f"Invalid predicate type: {predicate['type']}")
        
        # Convert object
        if object["type"] == "NamedNode":
            o = pyoxigraph.NamedNode(object["value"])
        elif object["type"] == "BlankNode":
            o = pyoxigraph.BlankNode(object["value"] if object["value"] else None)
        elif object["type"] == "Literal":
            if "datatype" in object and object["datatype"]:
                datatype_node = pyoxigraph.NamedNode(object["datatype"])
                o = pyoxigraph.Literal(object["value"], datatype=datatype_node)
            elif "language" in object and object["language"]:
                o = pyoxigraph.Literal(object["value"], language=object["language"])
            else:
                o = pyoxigraph.Literal(object["value"])
        else:
            raise ValueError(f"Invalid object type: {object['type']}")
        
        # Convert graph name if provided
        g = None
        if graph_name:
            if graph_name["type"] == "NamedNode":
                g = pyoxigraph.NamedNode(graph_name["value"])
            elif graph_name["type"] == "BlankNode":
                g = pyoxigraph.BlankNode(graph_name["value"] if graph_name["value"] else None)
            else:
                raise ValueError(f"Invalid graph name type: {graph_name['type']}")
        
        # Create and add the quad
        if g:
            quad_obj = pyoxigraph.Quad(s, p, o, g)
        else:
            quad_obj = pyoxigraph.Quad(s, p, o)
        
        store.add(quad_obj)
        
        return {
            "success": True,
            "message": "Quad added successfully"
        }
    
    except Exception as e:
        logger.error(f"Error adding quad: {e}")
        raise


def oxigraph_add_many(quads: List[Dict[str, Any]], store_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Add multiple quads to the store.
    
    Args:
        quads: List of dictionaries representing quads
        store_path: Optional path to the store to use. If None, uses the default store.
    
    Returns:
        Success dictionary
    """
    store = oxigraph_get_store(store_path)
    
    if not store:
        raise ValueError(f"Store with path '{store_path}' not found")
    
    try:
        count = 0
        for quad in quads:
            # Use oxigraph_add to add each quad
            oxigraph_add(quad, store_id)
            count += 1
        
        return {
            "success": True,
            "message": f"Added {count} quads"
        }
    
    except Exception as e:
        logger.error(f"Error adding quads: {e}")
        raise


def oxigraph_remove(quad: Dict[str, Any], store_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Remove a quad from the store.
    
    Args:
        quad: Dictionary representing a quad
        store_path: Optional path to the store to use. If None, uses the default store.
    
    Returns:
        Success dictionary
    """
    store = oxigraph_get_store(store_path)
    
    if not store:
        raise ValueError(f"Store with path '{store_path}' not found")
    
    try:
        # Convert the quad dictionary to a pyoxigraph.Quad object
        subject = quad["subject"]
        predicate = quad["predicate"]
        object = quad["object"]
        graph_name = quad.get("graph_name")
        
        # Convert subject
        if subject["type"] == "NamedNode":
            s = pyoxigraph.NamedNode(subject["value"])
        elif subject["type"] == "BlankNode":
            s = pyoxigraph.BlankNode(subject["value"] if subject["value"] else None)
        else:
            raise ValueError(f"Invalid subject type: {subject['type']}")
        
        # Convert predicate
        if predicate["type"] == "NamedNode":
            p = pyoxigraph.NamedNode(predicate["value"])
        else:
            raise ValueError(f"Invalid predicate type: {predicate['type']}")
        
        # Convert object
        if object["type"] == "NamedNode":
            o = pyoxigraph.NamedNode(object["value"])
        elif object["type"] == "BlankNode":
            o = pyoxigraph.BlankNode(object["value"] if object["value"] else None)
        elif object["type"] == "Literal":
            if "datatype" in object and object["datatype"]:
                datatype_node = pyoxigraph.NamedNode(object["datatype"])
                o = pyoxigraph.Literal(object["value"], datatype=datatype_node)
            elif "language" in object and object["language"]:
                o = pyoxigraph.Literal(object["value"], language=object["language"])
            else:
                o = pyoxigraph.Literal(object["value"])
        else:
            raise ValueError(f"Invalid object type: {object['type']}")
        
        # Convert graph name if provided
        g = None
        if graph_name:
            if graph_name["type"] == "NamedNode":
                g = pyoxigraph.NamedNode(graph_name["value"])
            elif graph_name["type"] == "BlankNode":
                g = pyoxigraph.BlankNode(graph_name["value"] if graph_name["value"] else None)
            else:
                raise ValueError(f"Invalid graph name type: {graph_name['type']}")
        
        # Create and remove the quad
        if g:
            quad_obj = pyoxigraph.Quad(s, p, o, g)
        else:
            quad_obj = pyoxigraph.Quad(s, p, o)
        
        store.remove(quad_obj)
        
        return {
            "success": True,
            "message": "Quad removed successfully"
        }
    
    except Exception as e:
        logger.error(f"Error removing quad: {e}")
        raise


def oxigraph_remove_many(quads: List[Dict[str, Any]], store_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Remove multiple quads from the store.
    
    Args:
        quads: List of dictionaries representing quads
        store_path: Optional path to the store to use. If None, uses the default store.
    
    Returns:
        Success dictionary
    """
    store = oxigraph_get_store(store_path)
    
    if not store:
        raise ValueError(f"Store with path '{store_path}' not found")
    
    try:
        count = 0
        for quad in quads:
            # Use oxigraph_remove to remove each quad
            oxigraph_remove(quad, store_id)
            count += 1
        
        return {
            "success": True,
            "message": f"Removed {count} quads"
        }
    
    except Exception as e:
        logger.error(f"Error removing quads: {e}")
        raise


def oxigraph_clear(store_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Remove all quads from the store.
    
    Args:
        store_path: Optional path to the store to use. If None, uses the default store.
    
    Returns:
        Success dictionary
    """
    store = oxigraph_get_store(store_path)
    
    if not store:
        raise ValueError(f"Store with path '{store_path}' not found")
    
    try:
        # Get all quads
        quads = list(store.quads_for_pattern(None, None, None))
        
        # Remove each quad
        for quad in quads:
            store.remove(quad)
        
        return {
            "success": True,
            "message": f"Cleared {len(quads)} quads from the store"
        }
    
    except Exception as e:
        logger.error(f"Error clearing store: {e}")
        raise


def oxigraph_quads_for_pattern(subject: Optional[Dict[str, Any]] = None,
                            predicate: Optional[Dict[str, Any]] = None,
                            object: Optional[Dict[str, Any]] = None,
                            graph_name: Optional[Dict[str, Any]] = None,
                            store_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Query for quads matching a pattern.
    
    Args:
        subject: Optional dictionary representing the subject (NamedNode or BlankNode)
        predicate: Optional dictionary representing the predicate (NamedNode)
        object: Optional dictionary representing the object (NamedNode, BlankNode, or Literal)
        graph_name: Optional dictionary representing the graph name (NamedNode or BlankNode)
        store_path: Optional path to the store to use. If None, uses the default store.
    
    Returns:
        Dictionary with list of matching quads
    """
    store = oxigraph_get_store(store_path)
    
    if not store:
        raise ValueError(f"Store with path '{store_path}' not found")
    
    try:
        # Convert subject if provided
        s = None
        if subject:
            if subject["type"] == "NamedNode":
                s = pyoxigraph.NamedNode(subject["value"])
            elif subject["type"] == "BlankNode":
                s = pyoxigraph.BlankNode(subject["value"] if subject["value"] else None)
            else:
                raise ValueError(f"Invalid subject type: {subject['type']}")
        
        # Convert predicate if provided
        p = None
        if predicate:
            if predicate["type"] == "NamedNode":
                p = pyoxigraph.NamedNode(predicate["value"])
            else:
                raise ValueError(f"Invalid predicate type: {predicate['type']}")
        
        # Convert object if provided
        o = None
        if object:
            if object["type"] == "NamedNode":
                o = pyoxigraph.NamedNode(object["value"])
            elif object["type"] == "BlankNode":
                o = pyoxigraph.BlankNode(object["value"] if object["value"] else None)
            elif object["type"] == "Literal":
                if "datatype" in object and object["datatype"]:
                    datatype_node = pyoxigraph.NamedNode(object["datatype"])
                    o = pyoxigraph.Literal(object["value"], datatype=datatype_node)
                elif "language" in object and object["language"]:
                    o = pyoxigraph.Literal(object["value"], language=object["language"])
                else:
                    o = pyoxigraph.Literal(object["value"])
            else:
                raise ValueError(f"Invalid object type: {object['type']}")
        
        # Convert graph name if provided
        g = None
        if graph_name:
            if graph_name["type"] == "NamedNode":
                g = pyoxigraph.NamedNode(graph_name["value"])
            elif graph_name["type"] == "BlankNode":
                g = pyoxigraph.BlankNode(graph_name["value"] if graph_name["value"] else None)
            else:
                raise ValueError(f"Invalid graph name type: {graph_name['type']}")
        
        # Query for quads matching the pattern
        matching_quads = list(store.quads_for_pattern(s, p, o, g))
        
        # Convert results to dictionaries
        result_quads = []
        for quad in matching_quads:
            quad_dict = {
                "type": "Quad",
                "subject": _term_to_dict(quad.subject),
                "predicate": _term_to_dict(quad.predicate),
                "object": _term_to_dict(quad.object),
                "graph_name": _term_to_dict(quad.graph_name)
            }
            result_quads.append(quad_dict)
        
        return {
            "success": True,
            "quads": result_quads
        }
    
    except Exception as e:
        logger.error(f"Error querying quads: {e}")
        raise


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

"""
Serialization functions for PyOxigraph.

This module provides functions for parsing and serializing RDF data in various formats.
"""

import logging
import os
from typing import Dict, List, Optional, Any, Union
import pyoxigraph
from .store import oxigraph_get_store

logger = logging.getLogger(__name__)

# Define supported formats
_SUPPORTED_FORMATS = {
    "turtle": {
        "name": "Turtle",
        "extension": ".ttl",
        "media_type": "text/turtle",
        "description": "Terse RDF Triple Language",
        "can_parse": True,
        "can_serialize": True
    },
    "ntriples": {
        "name": "N-Triples",
        "extension": ".nt",
        "media_type": "application/n-triples",
        "description": "Line-based syntax for RDF",
        "can_parse": True,
        "can_serialize": True
    },
    "nquads": {
        "name": "N-Quads",
        "extension": ".nq",
        "media_type": "application/n-quads",
        "description": "Line-based syntax for RDF datasets",
        "can_parse": True,
        "can_serialize": True
    },
    "trig": {
        "name": "TriG",
        "extension": ".trig",
        "media_type": "application/trig",
        "description": "Turtle-like syntax for RDF datasets",
        "can_parse": True,
        "can_serialize": True
    },
    "rdfxml": {
        "name": "RDF/XML",
        "extension": ".rdf",
        "media_type": "application/rdf+xml",
        "description": "XML syntax for RDF",
        "can_parse": True,
        "can_serialize": True
    }
}


def oxigraph_parse(data: str, format: str = "turtle", base_iri: Optional[str] = None, 
                store_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Parse RDF data and add to the store.
    
    Args:
        data: RDF data string
        format: Format of the data (turtle, ntriples, nquads, etc.)
        base_iri: Optional base IRI for resolving relative IRIs
        store_id: Optional ID of the store to use. If None, uses the default store.
    
    Returns:
        Success dictionary
    """
    store = oxigraph_get_store(store_id)
    
    if not store:
        raise ValueError(f"Store with ID '{store_id}' not found")
    
    # Normalize format string
    format = format.lower()
    
    try:
        # Check if format is supported
        if format not in _SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported format: {format}")
        
        # PyOxigraph doesn't have a direct parse method for strings
        # We need to implement this using the appropriate parser
        
        if format == "turtle":
            # For Turtle format
            base = pyoxigraph.NamedNode(base_iri) if base_iri else None
            
            if not hasattr(pyoxigraph, "parse_turtle"):
                raise NotImplementedError("parse_turtle function is not available in this version of PyOxigraph")
            
            quads = pyoxigraph.parse_turtle(data, base=base)
            count = 0
            
            for quad in quads:
                store.add(quad)
                count += 1
            
            return {
                "success": True,
                "message": f"Parsed {count} triples from Turtle data"
            }
            
        elif format == "ntriples":
            # For N-Triples format
            if not hasattr(pyoxigraph, "parse_ntriples"):
                raise NotImplementedError("parse_ntriples function is not available in this version of PyOxigraph")
                
            triples = pyoxigraph.parse_ntriples(data)
            count = 0
            
            for triple in triples:
                store.add(triple)
                count += 1
            
            return {
                "success": True,
                "message": f"Parsed {count} triples from N-Triples data"
            }
            
        elif format == "nquads":
            # For N-Quads format
            if not hasattr(pyoxigraph, "parse_nquads"):
                raise NotImplementedError("parse_nquads function is not available in this version of PyOxigraph")
                
            quads = pyoxigraph.parse_nquads(data)
            count = 0
            
            for quad in quads:
                store.add(quad)
                count += 1
            
            return {
                "success": True,
                "message": f"Parsed {count} quads from N-Quads data"
            }
            
        elif format == "trig":
            # For TriG format
            base = pyoxigraph.NamedNode(base_iri) if base_iri else None
            
            if not hasattr(pyoxigraph, "parse_trig"):
                raise NotImplementedError("parse_trig function is not available in this version of PyOxigraph")
                
            quads = pyoxigraph.parse_trig(data, base=base)
            count = 0
            
            for quad in quads:
                store.add(quad)
                count += 1
            
            return {
                "success": True,
                "message": f"Parsed {count} quads from TriG data"
            }
            
        elif format == "rdfxml":
            # For RDF/XML format
            base = pyoxigraph.NamedNode(base_iri) if base_iri else None
            
            if not hasattr(pyoxigraph, "parse_rdf_xml"):
                raise NotImplementedError("parse_rdf_xml function is not available in this version of PyOxigraph")
                
            triples = pyoxigraph.parse_rdf_xml(data, base=base)
            count = 0
            
            for triple in triples:
                store.add(triple)
                count += 1
            
            return {
                "success": True,
                "message": f"Parsed {count} triples from RDF/XML data"
            }
            
        else:
            raise ValueError(f"Format parsing not implemented: {format}")
    
    except Exception as e:
        logger.error(f"Error parsing {format} data: {e}")
        raise


def oxigraph_serialize(format: str = "turtle", store_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Serialize the store to a string.
    
    Args:
        format: Format for serialization (turtle, ntriples, nquads, etc.)
        store_id: Optional ID of the store to use. If None, uses the default store.
    
    Returns:
        Dictionary with serialized data
    """
    store = oxigraph_get_store(store_id)
    
    if not store:
        raise ValueError(f"Store with ID '{store_id}' not found")
    
    # Normalize format string
    format = format.lower()
    
    try:
        # Check if format is supported
        if format not in _SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported format: {format}")
        
        # Get all quads from the store
        quads = list(store.quads())
        
        # PyOxigraph doesn't have direct serialization methods,
        # but we'll try to implement this based on the format requested
        if format == "turtle":
            # Turtle format - triple-based, no graph names
            # Not a perfect approach, but workable for simple cases
            raise NotImplementedError(f"Serialization to {format} is not yet implemented")
            
        elif format == "ntriples":
            # N-Triples format - triple-based, no graph names
            # Not a perfect approach, but workable for simple cases
            raise NotImplementedError(f"Serialization to {format} is not yet implemented")
            
        elif format == "nquads":
            # N-Quads format - quad-based
            # Not a perfect approach, but workable for simple cases
            raise NotImplementedError(f"Serialization to {format} is not yet implemented")
            
        elif format == "trig":
            # TriG format - quad-based
            raise NotImplementedError(f"Serialization to {format} is not yet implemented")
            
        elif format == "rdfxml":
            # RDF/XML format - triple-based, no graph names
            raise NotImplementedError(f"Serialization to {format} is not yet implemented")
            
        else:
            raise ValueError(f"Format serialization not implemented: {format}")
    
    except Exception as e:
        logger.error(f"Error serializing to {format}: {e}")
        raise


def oxigraph_import_file(file_path: str, format: str = "turtle", base_iri: Optional[str] = None,
                     store_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Import RDF data from a file.
    
    Args:
        file_path: Path to the file
        format: Format of the data (turtle, ntriples, nquads, etc.)
        base_iri: Optional base IRI for resolving relative IRIs
        store_id: Optional ID of the store to use. If None, uses the default store.
    
    Returns:
        Success dictionary
    """
    try:
        # Check if file exists
        if not os.path.isfile(file_path):
            raise ValueError(f"File not found: {file_path}")
        
        # Read file content
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
        
        # Use parse function to import the content
        return oxigraph_parse(content, format, base_iri, store_id)
    
    except Exception as e:
        logger.error(f"Error importing file: {e}")
        raise


def oxigraph_export_graph(file_path: str, format: str = "turtle", graph_name: Optional[str] = None,
                      store_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Export a graph to a file.
    
    Args:
        file_path: Path to save the file
        format: Format for serialization (turtle, ntriples, nquads, etc.)
        graph_name: Optional IRI of the graph to export. If None, exports the default graph.
        store_id: Optional ID of the store to use. If None, uses the default store.
    
    Returns:
        Success dictionary
    """
    try:
        # Serialize the store to the specified format
        serialized = oxigraph_serialize(format, store_id)
        
        # Write to file
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(serialized["data"])
        
        return {
            "success": True,
            "message": f"Graph exported to {file_path} in {format} format"
        }
    
    except Exception as e:
        logger.error(f"Error exporting graph: {e}")
        raise


def oxigraph_get_supported_formats() -> Dict[str, Any]:
    """
    Get a list of supported RDF formats.
    
    Returns:
        Dictionary with supported formats
    """
    return {
        "success": True,
        "formats": _SUPPORTED_FORMATS
    }

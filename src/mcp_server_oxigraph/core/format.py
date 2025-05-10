"""
Serialization functions for PyOxigraph.

This module provides functions for parsing and serializing RDF data
in various formats using PyOxigraph's RdfFormat enum.
"""

import logging
import os
from typing import Dict, List, Any, Optional, Union
import pyoxigraph
from pyoxigraph import RdfFormat

# Configure logging
logger = logging.getLogger(__name__)

# Import the open_store function for stateless operations
from .store import open_store

def _get_rdf_format(format_str: Optional[str] = None, file_path: Optional[str] = None) -> RdfFormat:
    """
    Convert a format string to a RdfFormat enum value or detect from file extension.
    
    Args:
        format_str: Format string (e.g., 'turtle', 'ntriples')
        file_path: File path to detect format from extension
    
    Returns:
        RdfFormat enum value
    """
    # Try to detect from file extension first
    if file_path:
        _, ext = os.path.splitext(file_path)
        if ext:
            try:
                # Remove the dot from extension
                return RdfFormat.from_extension(ext[1:])
            except ValueError:
                # If extension doesn't map to a format, continue to string matching
                pass
    
    # Handle string format specification
    if format_str:
        format_lower = format_str.lower()
        
        if format_lower in ['turtle', 'ttl']:
            return RdfFormat.TURTLE
        elif format_lower in ['ntriples', 'nt']:
            return RdfFormat.N_TRIPLES
        elif format_lower in ['nquads', 'nq']:
            return RdfFormat.N_QUADS
        elif format_lower in ['trig']:
            return RdfFormat.TRIG
        elif format_lower in ['rdfxml', 'rdf/xml', 'rdf', 'xml']:
            return RdfFormat.RDF_XML
        elif format_lower in ['n3']:
            return RdfFormat.N3
    
    # Default to Turtle if no format could be determined
    return RdfFormat.TURTLE

def oxigraph_parse(
    data: str, 
    format: str = "turtle", 
    base_iri: Optional[str] = None,
    store_path: Optional[str] = None
) -> Dict[str, Any]:
    """
    Parse RDF data and add to the store.
    
    Args:
        data: RDF data string
        format: Format of the data (turtle, ntriples, nquads, etc.)
        base_iri: Optional base IRI for resolving relative IRIs
        store_path: Optional path to the store to use
    
    Returns:
        Success dictionary
    """
    try:
        # Open the store
        store = open_store(store_path)
        
        # Convert string format to RdfFormat enum
        rdf_format = _get_rdf_format(format)
        
        # Parse the data using RdfFormat enum
        count = 0
        triples = pyoxigraph.parse(input=data.encode('utf-8'), format=rdf_format, base_iri=base_iri)
        
        for triple in triples:
            store.add(triple)
            count += 1
        
        return {
            "success": True,
            "message": f"Parsed and added {count} triples to store",
            "count": count
        }
    except Exception as e:
        logger.error(f"Error parsing RDF data: {e}")
        raise ValueError(f"Failed to parse RDF data: {e}")

def oxigraph_serialize(
    format: str = "turtle",
    store_path: Optional[str] = None
) -> Dict[str, Any]:
    """
    Serialize the store to a string.
    
    Args:
        format: Format for serialization (turtle, ntriples, nquads, etc.)
        store_path: Optional path to the store to use
    
    Returns:
        Dictionary with serialized data
    """
    try:
        # Open the store
        store = open_store(store_path)
        
        # Get all quads from the store
        quads = list(store.quads_for_pattern(None, None, None, None))
        
        # Convert string format to RdfFormat enum
        rdf_format = _get_rdf_format(format)
        
        # Create standard prefixes for formats that support them
        prefixes = {
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            "ex": "http://example.org/"
        }
        
        # Check if format supports datasets (quads with graph names)
        has_graph_names = any(quad.graph_name is not None for quad in quads)
        
        if has_graph_names and not rdf_format.supports_datasets:
            # Convert quads to triples for formats that don't support datasets
            triples = [pyoxigraph.Triple(q.subject, q.predicate, q.object) for q in quads]
            serialized_bytes = pyoxigraph.serialize(triples, format=rdf_format, prefixes=prefixes)
        else:
            # Formats that support datasets
            serialized_bytes = pyoxigraph.serialize(quads, format=rdf_format, prefixes=prefixes)
        
        # Convert bytes to string
        serialized = serialized_bytes.decode('utf-8')
        
        return {
            "data": serialized,
            "format": format,
            "count": len(quads)
        }
    except Exception as e:
        logger.error(f"Error serializing store: {e}")
        raise ValueError(f"Failed to serialize store: {e}")

def oxigraph_import_file(
    file_path: str, 
    format: Optional[str] = None, 
    base_iri: Optional[str] = None,
    store_path: Optional[str] = None
) -> Dict[str, Any]:
    """
    Import RDF data from a file.
    
    Args:
        file_path: Path to the file
        format: Format of the data (turtle, ntriples, nquads, etc.) or None to detect from extension
        base_iri: Optional base IRI for resolving relative IRIs
        store_path: Optional path to the store to use
    
    Returns:
        Success dictionary
    """
    try:
        # Expand user directory if needed
        if file_path.startswith("~"):
            file_path = os.path.expanduser(file_path)
        
        # Make sure the file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Open the store
        store = open_store(store_path)
        
        # Determine format based on file extension if not provided
        rdf_format = _get_rdf_format(format, file_path)
        
        # Use PyOxigraph's parse function with path parameter
        count = 0
        for triple in pyoxigraph.parse(path=file_path, format=rdf_format, base_iri=base_iri):
            store.add(triple)
            count += 1
        
        return {
            "success": True,
            "message": f"Imported {count} triples from {file_path}",
            "count": count
        }
    except Exception as e:
        logger.error(f"Error importing file: {e}")
        raise ValueError(f"Failed to import file: {e}")

def oxigraph_export_graph(
    file_path: str, 
    format: Optional[str] = None, 
    graph_name: Optional[str] = None,
    store_path: Optional[str] = None
) -> Dict[str, Any]:
    """
    Export a graph to a file.
    
    Args:
        file_path: Path to save the file
        format: Format for serialization or None to detect from file extension
        graph_name: Optional IRI of the graph to export
        store_path: Optional path to the store to use
    
    Returns:
        Success dictionary
    """
    try:
        # Open the store
        store = open_store(store_path)
        
        # Expand user directory if needed
        if file_path.startswith("~"):
            file_path = os.path.expanduser(file_path)
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Get the graph to export
        graph_node = None
        if graph_name:
            graph_node = pyoxigraph.NamedNode(graph_name)
        
        # Get quads from the store
        quads = list(store.quads_for_pattern(None, None, None, graph_node))
        
        # Determine format based on file extension if not provided
        rdf_format = _get_rdf_format(format, file_path)
        
        # Create standard prefixes for formats that support them
        prefixes = {
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            "ex": "http://example.org/"
        }
        
        # Check if format supports datasets (quads with graph names)
        has_graph_names = any(quad.graph_name is not None for quad in quads)
        
        if has_graph_names and not rdf_format.supports_datasets:
            # Convert quads to triples for formats that don't support datasets
            triples = [pyoxigraph.Triple(q.subject, q.predicate, q.object) for q in quads]
            pyoxigraph.serialize(triples, output=file_path, format=rdf_format, prefixes=prefixes)
        else:
            # Use serialize with output parameter for direct file writing
            pyoxigraph.serialize(quads, output=file_path, format=rdf_format, prefixes=prefixes)
        
        return {
            "success": True,
            "message": f"Exported {len(quads)} triples to {file_path}",
            "count": len(quads),
            "file_path": file_path
        }
    except Exception as e:
        logger.error(f"Error exporting graph: {e}")
        raise ValueError(f"Failed to export graph: {e}")

def oxigraph_get_supported_formats() -> Dict[str, Any]:
    """
    Get a list of supported RDF formats.
    
    Returns:
        Dictionary with supported formats
    """
    try:
        formats = [
            {
                "id": "turtle", 
                "name": "Turtle", 
                "extension": ".ttl", 
                "mime_type": "text/turtle",
                "supports_datasets": RdfFormat.TURTLE.supports_datasets
            },
            {
                "id": "ntriples", 
                "name": "N-Triples", 
                "extension": ".nt", 
                "mime_type": "application/n-triples",
                "supports_datasets": RdfFormat.N_TRIPLES.supports_datasets
            },
            {
                "id": "nquads", 
                "name": "N-Quads", 
                "extension": ".nq", 
                "mime_type": "application/n-quads",
                "supports_datasets": RdfFormat.N_QUADS.supports_datasets
            },
            {
                "id": "trig", 
                "name": "TriG", 
                "extension": ".trig", 
                "mime_type": "application/trig",
                "supports_datasets": RdfFormat.TRIG.supports_datasets
            },
            {
                "id": "rdfxml", 
                "name": "RDF/XML", 
                "extension": ".rdf", 
                "mime_type": "application/rdf+xml",
                "supports_datasets": RdfFormat.RDF_XML.supports_datasets
            },
            {
                "id": "n3", 
                "name": "N3", 
                "extension": ".n3", 
                "mime_type": "text/n3",
                "supports_datasets": RdfFormat.N3.supports_datasets
            }
        ]
        
        return {
            "formats": formats
        }
    except Exception as e:
        logger.error(f"Error getting supported formats: {e}")
        raise ValueError(f"Failed to get supported formats: {e}")

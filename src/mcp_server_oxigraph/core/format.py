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
        quad_count = 0
        
        # Check if format is supported
        if format not in _SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported format: {format}")
        
        # Handle each format appropriately
        base_node = pyoxigraph.NamedNode(base_iri) if base_iri else None
        
        if format == "turtle":
            # For Turtle format
            if hasattr(pyoxigraph, "parse_turtle"):
                if base_iri:
                    quads = pyoxigraph.parse_turtle(data, base=base_node)
                else:
                    quads = pyoxigraph.parse_turtle(data)
                    
                for quad in quads:
                    store.add(quad)
                    quad_count += 1
            else:
                # Alternative approach if method isn't available
                # Use StringIO for in-memory operation
                import io
                from tempfile import NamedTemporaryFile
                
                # Write data to a temporary file
                with NamedTemporaryFile(mode='w', suffix='.ttl', delete=False) as f:
                    f.write(data)
                    temp_path = f.name
                
                try:
                    # Create a temporary store to load the data
                    temp_store = pyoxigraph.Store()
                    
                    # Try using load method if available
                    if hasattr(temp_store, "load"):
                        temp_store.load(temp_path, "turtle", base_iri=base_iri)
                        
                        # Copy all quads to the target store
                        for quad in temp_store.quads_for_pattern(None, None, None):
                            store.add(quad)
                            quad_count += 1
                    else:
                        raise NotImplementedError("Neither parse_turtle nor load methods are available")
                finally:
                    # Clean up temporary file
                    if os.path.exists(temp_path):
                        os.unlink(temp_path)
                
        elif format == "ntriples":
            # For N-Triples format
            if hasattr(pyoxigraph, "parse_ntriples"):
                quads = pyoxigraph.parse_ntriples(data)
                for quad in quads:
                    store.add(quad)
                    quad_count += 1
            else:
                # Alternative approach
                import io
                from tempfile import NamedTemporaryFile
                
                # Write data to a temporary file
                with NamedTemporaryFile(mode='w', suffix='.nt', delete=False) as f:
                    f.write(data)
                    temp_path = f.name
                
                try:
                    # Create a temporary store to load the data
                    temp_store = pyoxigraph.Store()
                    
                    # Try using load method if available
                    if hasattr(temp_store, "load"):
                        temp_store.load(temp_path, "ntriples")
                        
                        # Copy all quads to the target store
                        for quad in temp_store.quads_for_pattern(None, None, None):
                            store.add(quad)
                            quad_count += 1
                    else:
                        raise NotImplementedError("Neither parse_ntriples nor load methods are available")
                finally:
                    # Clean up temporary file
                    if os.path.exists(temp_path):
                        os.unlink(temp_path)
                
        elif format == "nquads":
            # For N-Quads format
            if hasattr(pyoxigraph, "parse_nquads"):
                quads = pyoxigraph.parse_nquads(data)
                for quad in quads:
                    store.add(quad)
                    quad_count += 1
            else:
                # Alternative approach
                import io
                from tempfile import NamedTemporaryFile
                
                # Write data to a temporary file
                with NamedTemporaryFile(mode='w', suffix='.nq', delete=False) as f:
                    f.write(data)
                    temp_path = f.name
                
                try:
                    # Create a temporary store to load the data
                    temp_store = pyoxigraph.Store()
                    
                    # Try using load method if available
                    if hasattr(temp_store, "load"):
                        temp_store.load(temp_path, "nquads")
                        
                        # Copy all quads to the target store
                        for quad in temp_store.quads_for_pattern(None, None, None):
                            store.add(quad)
                            quad_count += 1
                    else:
                        raise NotImplementedError("Neither parse_nquads nor load methods are available")
                finally:
                    # Clean up temporary file
                    if os.path.exists(temp_path):
                        os.unlink(temp_path)
            
        elif format == "trig":
            # For TriG format
            if hasattr(pyoxigraph, "parse_trig"):
                if base_iri:
                    quads = pyoxigraph.parse_trig(data, base=base_node)
                else:
                    quads = pyoxigraph.parse_trig(data)
                    
                for quad in quads:
                    store.add(quad)
                    quad_count += 1
            else:
                # Alternative approach
                import io
                from tempfile import NamedTemporaryFile
                
                # Write data to a temporary file
                with NamedTemporaryFile(mode='w', suffix='.trig', delete=False) as f:
                    f.write(data)
                    temp_path = f.name
                
                try:
                    # Create a temporary store to load the data
                    temp_store = pyoxigraph.Store()
                    
                    # Try using load method if available
                    if hasattr(temp_store, "load"):
                        temp_store.load(temp_path, "trig", base_iri=base_iri)
                        
                        # Copy all quads to the target store
                        for quad in temp_store.quads_for_pattern(None, None, None):
                            store.add(quad)
                            quad_count += 1
                    else:
                        raise NotImplementedError("Neither parse_trig nor load methods are available")
                finally:
                    # Clean up temporary file
                    if os.path.exists(temp_path):
                        os.unlink(temp_path)
            
        elif format == "rdfxml":
            # For RDF/XML format
            if hasattr(pyoxigraph, "parse_rdf_xml"):
                if base_iri:
                    quads = pyoxigraph.parse_rdf_xml(data, base=base_node)
                else:
                    quads = pyoxigraph.parse_rdf_xml(data)
                    
                for quad in quads:
                    store.add(quad)
                    quad_count += 1
            else:
                # Alternative approach
                import io
                from tempfile import NamedTemporaryFile
                
                # Write data to a temporary file
                with NamedTemporaryFile(mode='w', suffix='.rdf', delete=False) as f:
                    f.write(data)
                    temp_path = f.name
                
                try:
                    # Create a temporary store to load the data
                    temp_store = pyoxigraph.Store()
                    
                    # Try using load method if available
                    if hasattr(temp_store, "load"):
                        temp_store.load(temp_path, "rdfxml", base_iri=base_iri)
                        
                        # Copy all quads to the target store
                        for quad in temp_store.quads_for_pattern(None, None, None):
                            store.add(quad)
                            quad_count += 1
                    else:
                        raise NotImplementedError("Neither parse_rdf_xml nor load methods are available")
                finally:
                    # Clean up temporary file
                    if os.path.exists(temp_path):
                        os.unlink(temp_path)
            
        else:
            raise ValueError(f"Format parsing not implemented: {format}")
        
        return {
            "success": True,
            "message": f"Parsed {quad_count} triples from {format} data"
        }
    
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
        quads = list(store.quads_for_pattern(None, None, None))
        
        # Check for serialization methods
        if format == "turtle":
            if hasattr(pyoxigraph, "serialize_turtle"):
                data = pyoxigraph.serialize_turtle(quads)
                return {
                    "success": True,
                    "data": data
                }
        elif format == "ntriples":
            if hasattr(pyoxigraph, "serialize_ntriples"):
                data = pyoxigraph.serialize_ntriples(quads)
                return {
                    "success": True,
                    "data": data
                }
        elif format == "nquads":
            if hasattr(pyoxigraph, "serialize_nquads"):
                data = pyoxigraph.serialize_nquads(quads)
                return {
                    "success": True,
                    "data": data
                }
        elif format == "trig":
            if hasattr(pyoxigraph, "serialize_trig"):
                data = pyoxigraph.serialize_trig(quads)
                return {
                    "success": True,
                    "data": data
                }
        elif format == "rdfxml":
            if hasattr(pyoxigraph, "serialize_rdf_xml"):
                data = pyoxigraph.serialize_rdf_xml(quads)
                return {
                    "success": True,
                    "data": data
                }
        
        # If we get here, no appropriate serialization method found
        # Fallback to manual serialization for simple formats
        if format == "ntriples" or format == "nquads":
            # Simple line-based formats can be manually generated
            lines = []
            for quad in quads:
                if format == "ntriples" and quad.graph_name:
                    # Skip quads with graph names for ntriples
                    continue
                    
                # Subject
                if isinstance(quad.subject, pyoxigraph.NamedNode):
                    subj = f"<{quad.subject.value}>"
                else:  # BlankNode
                    subj = f"_:{quad.subject.value}" if quad.subject.value else "_:b0"
                
                # Predicate
                pred = f"<{quad.predicate.value}>"
                
                # Object
                if isinstance(quad.object, pyoxigraph.NamedNode):
                    obj = f"<{quad.object.value}>"
                elif isinstance(quad.object, pyoxigraph.BlankNode):
                    obj = f"_:{quad.object.value}" if quad.object.value else "_:b0"
                else:  # Literal
                    value = quad.object.value.replace('"', '\\"').replace('\n', '\\n')
                    if quad.object.language:
                        obj = f'"{value}"@{quad.object.language}'
                    elif quad.object.datatype:
                        obj = f'"{value}"^^<{quad.object.datatype.value}>'
                    else:
                        obj = f'"{value}"'
                
                if format == "ntriples":
                    lines.append(f"{subj} {pred} {obj} .")
                else:  # nquads
                    if quad.graph_name:
                        if isinstance(quad.graph_name, pyoxigraph.NamedNode):
                            graph = f"<{quad.graph_name.value}>"
                        else:  # BlankNode
                            graph = f"_:{quad.graph_name.value}" if quad.graph_name.value else "_:g0"
                        lines.append(f"{subj} {pred} {obj} {graph} .")
                    else:
                        lines.append(f"{subj} {pred} {obj} .")
            
            return {
                "success": True,
                "data": "\n".join(lines)
            }
        
        # Otherwise, we need an external library or to use a temporary file
        try:
            import tempfile
            import subprocess
            
            # Create a temporary file to store the quads
            with tempfile.NamedTemporaryFile(mode="w+", suffix=".nq", delete=False) as temp:
                # Write quads in N-Quads format (which all tools should support)
                for quad in quads:
                    # Subject
                    if isinstance(quad.subject, pyoxigraph.NamedNode):
                        subj = f"<{quad.subject.value}>"
                    else:  # BlankNode
                        subj = f"_:{quad.subject.value}" if quad.subject.value else "_:b0"
                    
                    # Predicate
                    pred = f"<{quad.predicate.value}>"
                    
                    # Object
                    if isinstance(quad.object, pyoxigraph.NamedNode):
                        obj = f"<{quad.object.value}>"
                    elif isinstance(quad.object, pyoxigraph.BlankNode):
                        obj = f"_:{quad.object.value}" if quad.object.value else "_:b0"
                    else:  # Literal
                        value = quad.object.value.replace('"', '\\"').replace('\n', '\\n')
                        if quad.object.language:
                            obj = f'"{value}"@{quad.object.language}'
                        elif quad.object.datatype:
                            obj = f'"{value}"^^<{quad.object.datatype.value}>'
                        else:
                            obj = f'"{value}"'
                    
                    # Graph
                    if quad.graph_name:
                        if isinstance(quad.graph_name, pyoxigraph.NamedNode):
                            graph = f"<{quad.graph_name.value}>"
                        else:  # BlankNode
                            graph = f"_:{quad.graph_name.value}" if quad.graph_name.value else "_:g0"
                        temp.write(f"{subj} {pred} {obj} {graph} .\n")
                    else:
                        temp.write(f"{subj} {pred} {obj} .\n")
                
                temp_path = temp.name
            
            # Create a temporary output file
            output_file = tempfile.NamedTemporaryFile(mode="w+", delete=False)
            output_path = output_file.name
            output_file.close()
            
            # Try to use rapper (from Raptor RDF Syntax Library) if available
            try:
                # Map format to rapper syntax
                rapper_format = {
                    "turtle": "turtle",
                    "ntriples": "ntriples",
                    "nquads": "nquads",
                    "trig": "trig",
                    "rdfxml": "rdfxml"
                }.get(format, "turtle")
                
                # Run rapper to convert
                subprocess.run(
                    ["rapper", "-i", "nquads", "-o", rapper_format, temp_path, "-o", output_path],
                    check=True
                )
                
                # Read the output
                with open(output_path, "r") as f:
                    data = f.read()
                
                return {
                    "success": True,
                    "data": data
                }
            except (subprocess.SubprocessError, FileNotFoundError):
                # Rapper not available, try another tool (e.g., rdflib if installed)
                try:
                    import rdflib
                    
                    # Load the graph
                    g = rdflib.ConjunctiveGraph()
                    g.parse(temp_path, format="nquads")
                    
                    # Convert to the target format
                    rdflib_format = {
                        "turtle": "turtle",
                        "ntriples": "nt",
                        "nquads": "nquads",
                        "trig": "trig",
                        "rdfxml": "xml"
                    }.get(format, "turtle")
                    
                    data = g.serialize(format=rdflib_format)
                    
                    return {
                        "success": True,
                        "data": data
                    }
                except ImportError:
                    raise NotImplementedError(f"Serialization to {format} requires external tools")
        finally:
            # Clean up temporary files
            if 'temp_path' in locals() and os.path.exists(temp_path):
                os.unlink(temp_path)
            if 'output_path' in locals() and os.path.exists(output_path):
                os.unlink(output_path)
    
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
        
        store = oxigraph_get_store(store_id)
        
        if not store:
            raise ValueError(f"Store with ID '{store_id}' not found")
        
        # Try to use direct load method if available
        if hasattr(store, "load"):
            # Load directly from file
            store.load(file_path, format, base_iri=base_iri)
            
            return {
                "success": True,
                "message": f"Imported data from {file_path}"
            }
        else:
            # Read file content and use parse function
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            
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

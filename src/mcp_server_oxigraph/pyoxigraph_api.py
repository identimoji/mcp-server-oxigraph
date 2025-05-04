"""
Direct API interface to the pyoxigraph library.

This module provides direct access to the core functionality of the pyoxigraph library,
exposing it in a way that can be easily used by the MCP server.
"""

import os
import logging
import json
import pyoxigraph
from typing import Dict, Any, List, Optional, Union, Iterator

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PyOxigraphAPI:
    """
    Direct API interface to the pyoxigraph library.
    
    This class provides direct access to the core functionality of the pyoxigraph library,
    exposing it in a way that can be easily used by the MCP server.
    """
    
    def __init__(self, store_path: Optional[str] = None):
        """
        Initialize the pyoxigraph API.
        
        Args:
            store_path: Optional path to store the RDF data. If None, an in-memory store is used.
                        If a path is provided, it will be used as a persistent store on disk.
        """
        self._store = None
        self.store_path = store_path
        
        # If no store path is provided, check environment variable
        if self.store_path is None:
            self.store_path = os.environ.get("OXIGRAPH_DB_PATH")
        
        # Initialize the store
        self._initialize_store()
    
    def _initialize_store(self) -> None:
        """Initialize the Oxigraph store."""
        try:
            if self.store_path is None:
                # In-memory store
                self._store = pyoxigraph.Store()
                logger.info("Initialized in-memory pyoxigraph store")
            else:
                # Ensure directory exists
                os.makedirs(os.path.dirname(self.store_path), exist_ok=True)
                # Persistent store on disk
                self._store = pyoxigraph.Store(self.store_path)
                logger.info(f"Initialized persistent pyoxigraph store at {self.store_path}")
        except Exception as e:
            logger.error(f"Failed to initialize store: {e}")
            raise
    
    def close(self) -> None:
        """Close the pyoxigraph store."""
        try:
            # Flush any pending changes to disk
            if hasattr(self._store, 'flush'):
                self._store.flush()
            logger.info("Store closed successfully")
        except Exception as e:
            logger.error(f"Error closing store: {e}")
            raise
    
    # RDF Data Model Functions
    
    def oxigraph_create_named_node(self, iri: str) -> Dict[str, Any]:
        """
        Create a Named Node (IRI) for use in RDF statements.
        
        Args:
            iri: The IRI string for the node
            
        Returns:
            Dictionary representing the Named Node
        """
        try:
            # pyoxigraph.NamedNode is just a wrapper, so we return serializable representation
            return {
                "type": "NamedNode",
                "value": iri
            }
        except Exception as e:
            logger.error(f"Failed to create named node: {e}")
            return {"error": str(e)}
    
    def oxigraph_create_blank_node(self, id: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a Blank Node for use in RDF statements.
        
        Args:
            id: Optional identifier for the blank node
            
        Returns:
            Dictionary representing the Blank Node
        """
        try:
            # We return serializable representation
            return {
                "type": "BlankNode",
                "value": id if id else ""
            }
        except Exception as e:
            logger.error(f"Failed to create blank node: {e}")
            return {"error": str(e)}
    
    def oxigraph_create_literal(self, value: str, datatype: Optional[str] = None, 
                              language: Optional[str] = None) -> Dict[str, Any]:
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
            # We return serializable representation
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
            logger.error(f"Failed to create literal: {e}")
            return {"error": str(e)}
    
    def oxigraph_create_quad(self, subject: Dict[str, Any], predicate: Dict[str, Any], 
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
            # We return serializable representation
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
            logger.error(f"Failed to create quad: {e}")
            return {"error": str(e)}
    
    # Core Store Operations
    
    def _convert_to_node(self, node_dict: Dict[str, Any]) -> Union[pyoxigraph.NamedNode, pyoxigraph.BlankNode, pyoxigraph.Literal]:
        """
        Convert a dictionary representation of a node to a pyoxigraph node.
        
        Args:
            node_dict: Dictionary representing a node
            
        Returns:
            pyoxigraph node
        """
        node_type = node_dict.get("type")
        value = node_dict.get("value", "")
        
        if node_type == "NamedNode":
            return pyoxigraph.NamedNode(value)
        elif node_type == "BlankNode":
            return pyoxigraph.BlankNode(value)
        elif node_type == "Literal":
            datatype = node_dict.get("datatype")
            language = node_dict.get("language")
            
            if datatype:
                return pyoxigraph.Literal(value, datatype=pyoxigraph.NamedNode(datatype))
            elif language:
                return pyoxigraph.Literal(value, language=language)
            else:
                return pyoxigraph.Literal(value)
        else:
            raise ValueError(f"Unknown node type: {node_type}")
    
    def _convert_to_quad(self, quad_dict: Dict[str, Any]) -> pyoxigraph.Quad:
        """
        Convert a dictionary representation of a quad to a pyoxigraph quad.
        
        Args:
            quad_dict: Dictionary representing a quad
            
        Returns:
            pyoxigraph.Quad
        """
        subject = self._convert_to_node(quad_dict.get("subject"))
        predicate = self._convert_to_node(quad_dict.get("predicate"))
        object = self._convert_to_node(quad_dict.get("object"))
        
        graph_name_dict = quad_dict.get("graph_name")
        if graph_name_dict:
            graph_name = self._convert_to_node(graph_name_dict)
            return pyoxigraph.Quad(subject, predicate, object, graph_name)
        else:
            return pyoxigraph.Quad(subject, predicate, object)
    
    def _node_to_dict(self, node) -> Dict[str, Any]:
        """
        Convert a pyoxigraph node to a dictionary representation.
        
        Args:
            node: pyoxigraph node
            
        Returns:
            Dictionary representation
        """
        if isinstance(node, pyoxigraph.NamedNode):
            return {
                "type": "NamedNode",
                "value": str(node.value)
            }
        elif isinstance(node, pyoxigraph.BlankNode):
            return {
                "type": "BlankNode",
                "value": str(node.value)
            }
        elif isinstance(node, pyoxigraph.Literal):
            result = {
                "type": "Literal",
                "value": str(node.value)
            }
            
            if node.datatype:
                result["datatype"] = str(node.datatype.value)
            
            if node.language:
                result["language"] = node.language
            
            return result
        else:
            return {"error": f"Unknown node type: {type(node)}"}
    
    def _quad_to_dict(self, quad) -> Dict[str, Any]:
        """
        Convert a pyoxigraph quad to a dictionary representation.
        
        Args:
            quad: pyoxigraph quad
            
        Returns:
            Dictionary representation
        """
        result = {
            "type": "Quad",
            "subject": self._node_to_dict(quad.subject),
            "predicate": self._node_to_dict(quad.predicate),
            "object": self._node_to_dict(quad.object)
        }
        
        if quad.graph_name:
            result["graph_name"] = self._node_to_dict(quad.graph_name)
        
        return result
    
    def oxigraph_add(self, quad: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add a quad to the store.
        
        Args:
            quad: Dictionary representing a quad
            
        Returns:
            Success dictionary
        """
        try:
            pyoxigraph_quad = self._convert_to_quad(quad)
            self._store.add(pyoxigraph_quad)
            return {"success": True, "message": "Quad added successfully"}
        except Exception as e:
            logger.error(f"Failed to add quad: {e}")
            return {"error": str(e)}
    
    def oxigraph_add_many(self, quads: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Add multiple quads to the store.
        
        Args:
            quads: List of dictionaries representing quads
            
        Returns:
            Success dictionary
        """
        try:
            pyoxigraph_quads = [self._convert_to_quad(quad) for quad in quads]
            for quad in pyoxigraph_quads:
                self._store.add(quad)
            return {"success": True, "message": f"Added {len(quads)} quads"}
        except Exception as e:
            logger.error(f"Failed to add quads: {e}")
            return {"error": str(e)}
    
    def oxigraph_remove(self, quad: Dict[str, Any]) -> Dict[str, Any]:
        """
        Remove a quad from the store.
        
        Args:
            quad: Dictionary representing a quad
            
        Returns:
            Success dictionary
        """
        try:
            pyoxigraph_quad = self._convert_to_quad(quad)
            self._store.remove(pyoxigraph_quad)
            return {"success": True, "message": "Quad removed successfully"}
        except Exception as e:
            logger.error(f"Failed to remove quad: {e}")
            return {"error": str(e)}
    
    def oxigraph_remove_many(self, quads: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Remove multiple quads from the store.
        
        Args:
            quads: List of dictionaries representing quads
            
        Returns:
            Success dictionary
        """
        try:
            pyoxigraph_quads = [self._convert_to_quad(quad) for quad in quads]
            for quad in pyoxigraph_quads:
                self._store.remove(quad)
            return {"success": True, "message": f"Removed {len(quads)} quads"}
        except Exception as e:
            logger.error(f"Failed to remove quads: {e}")
            return {"error": str(e)}
    
    def oxigraph_clear(self) -> Dict[str, Any]:
        """
        Remove all quads from the store.
        
        Returns:
            Success dictionary
        """
        try:
            # To clear the store, we need to iterate through all quads and remove them
            quads = list(self._store.quads_for_pattern(None, None, None, None))
            for quad in quads:
                self._store.remove(quad)
            return {"success": True, "message": f"Cleared store, removed {len(quads)} quads"}
        except Exception as e:
            logger.error(f"Failed to clear store: {e}")
            return {"error": str(e)}
    
    def oxigraph_query(self, query: str) -> Dict[str, Any]:
        """
        Execute a SPARQL query against the store.
        
        Args:
            query: SPARQL query string
            
        Returns:
            Query results dictionary
        """
        try:
            # Execute query
            results = list(self._store.query(query))
            
            # Convert results to serializable format
            serializable_results = []
            
            for result in results:
                # Each result is a dictionary mapping variable names to RDF terms
                serializable_result = {}
                
                for var, term in result.items():
                    serializable_result[var] = self._node_to_dict(term)
                
                serializable_results.append(serializable_result)
            
            return {"success": True, "results": serializable_results}
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            return {"error": str(e)}
    
    def oxigraph_update(self, update: str) -> Dict[str, Any]:
        """
        Execute a SPARQL update against the store.
        
        Args:
            update: SPARQL update string
            
        Returns:
            Success dictionary
        """
        try:
            self._store.update(update)
            return {"success": True, "message": "Update executed successfully"}
        except Exception as e:
            logger.error(f"Failed to execute update: {e}")
            return {"error": str(e)}
    
    def oxigraph_quads_for_pattern(self, subject: Optional[Dict[str, Any]] = None, 
                                 predicate: Optional[Dict[str, Any]] = None,
                                 object: Optional[Dict[str, Any]] = None,
                                 graph_name: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Query for quads matching a pattern.
        
        Args:
            subject: Optional dictionary representing the subject (NamedNode or BlankNode)
            predicate: Optional dictionary representing the predicate (NamedNode)
            object: Optional dictionary representing the object (NamedNode, BlankNode, or Literal)
            graph_name: Optional dictionary representing the graph name (NamedNode or BlankNode)
            
        Returns:
            Dictionary with list of matching quads
        """
        try:
            # Convert inputs to pyoxigraph nodes, using None for any not provided
            subject_node = self._convert_to_node(subject) if subject else None
            predicate_node = self._convert_to_node(predicate) if predicate else None
            object_node = self._convert_to_node(object) if object else None
            graph_name_node = self._convert_to_node(graph_name) if graph_name else None
            
            # Query for matching quads
            quads = list(self._store.quads_for_pattern(subject_node, predicate_node, object_node, graph_name_node))
            
            # Convert quads to serializable format
            serializable_quads = [self._quad_to_dict(quad) for quad in quads]
            
            return {"success": True, "quads": serializable_quads}
        except Exception as e:
            logger.error(f"Failed to query for quads: {e}")
            return {"error": str(e)}
    
    # RDF Serialization Functions
    
    def oxigraph_parse(self, data: str, format: str = "turtle", base_iri: Optional[str] = None) -> Dict[str, Any]:
        """
        Parse RDF data and add to the store.
        
        Args:
            data: RDF data string
            format: Format of the data (turtle, ntriples, nquads, etc.)
            base_iri: Optional base IRI for resolving relative IRIs
            
        Returns:
            Success dictionary
        """
        try:
            # Handle different formats
            if format.lower() == "turtle":
                if base_iri:
                    quads = pyoxigraph.parse_turtle(data, base_iri=base_iri)
                else:
                    quads = pyoxigraph.parse_turtle(data)
            elif format.lower() == "ntriples":
                quads = pyoxigraph.parse_ntriples(data)
            elif format.lower() == "nquads":
                quads = pyoxigraph.parse_nquads(data)
            elif format.lower() == "trig":
                if base_iri:
                    quads = pyoxigraph.parse_trig(data, base_iri=base_iri)
                else:
                    quads = pyoxigraph.parse_trig(data)
            elif format.lower() == "rdfxml":
                if base_iri:
                    quads = pyoxigraph.parse_rdf_xml(data, base_iri=base_iri)
                else:
                    quads = pyoxigraph.parse_rdf_xml(data)
            else:
                return {"error": f"Unsupported format: {format}"}
            
            # Add parsed quads to the store
            quad_count = 0
            for quad in quads:
                self._store.add(quad)
                quad_count += 1
            
            return {"success": True, "message": f"Parsed and added {quad_count} quads"}
        except Exception as e:
            logger.error(f"Failed to parse RDF data: {e}")
            return {"error": str(e)}
    
    def oxigraph_serialize(self, format: str = "turtle") -> Dict[str, Any]:
        """
        Serialize the store to a string.
        
        Args:
            format: Format for serialization (turtle, ntriples, nquads, etc.)
            
        Returns:
            Dictionary with serialized data
        """
        try:
            # Get all quads
            quads = list(self._store.quads_for_pattern(None, None, None, None))
            
            # Handle different formats
            if format.lower() == "turtle":
                data = pyoxigraph.serialize_turtle(quads)
            elif format.lower() == "ntriples":
                data = pyoxigraph.serialize_ntriples(quads)
            elif format.lower() == "nquads":
                data = pyoxigraph.serialize_nquads(quads)
            elif format.lower() == "trig":
                data = pyoxigraph.serialize_trig(quads)
            elif format.lower() == "rdfxml":
                data = pyoxigraph.serialize_rdf_xml(quads)
            else:
                return {"error": f"Unsupported format: {format}"}
            
            return {"success": True, "data": data}
        except Exception as e:
            logger.error(f"Failed to serialize RDF data: {e}")
            return {"error": str(e)}
    
    # Transaction Support
    
    def oxigraph_transaction(self, callback_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a transaction with automatic commit/rollback.
        
        Note: This is a simplified version where we execute a sequence of operations
        in one transaction. For a real-world implementation, you would need a way to 
        send the callback function, which is not straightforward in an MCP server.
        
        Args:
            callback_data: Dictionary with operations to execute in the transaction
            
        Returns:
            Success dictionary with results of all operations
        """
        try:
            operations = callback_data.get("operations", [])
            results = []
            
            # Start transaction
            transaction = self._store.transaction()
            
            try:
                for op in operations:
                    op_type = op.get("type")
                    op_params = op.get("params", {})
                    
                    if op_type == "add":
                        quad = op_params.get("quad")
                        if quad:
                            pyoxigraph_quad = self._convert_to_quad(quad)
                            transaction.add(pyoxigraph_quad)
                            results.append({"success": True, "operation": "add"})
                    
                    elif op_type == "remove":
                        quad = op_params.get("quad")
                        if quad:
                            pyoxigraph_quad = self._convert_to_quad(quad)
                            transaction.remove(pyoxigraph_quad)
                            results.append({"success": True, "operation": "remove"})
                    
                    else:
                        results.append({"error": f"Unknown operation type: {op_type}"})
                
                # Commit transaction
                transaction.commit()
                
                return {
                    "success": True, 
                    "message": f"Transaction completed successfully with {len(operations)} operations",
                    "results": results
                }
            
            except Exception as e:
                # Rollback transaction
                transaction.rollback()
                raise e
        
        except Exception as e:
            logger.error(f"Transaction failed: {e}")
            return {"error": str(e)}

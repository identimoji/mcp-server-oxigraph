#!/usr/bin/env python3
"""
Utility functions for raw RDF operations with PyOxigraph.
These functions provide direct access to PyOxigraph's RDF capabilities.
"""

import os
import sys
import logging
from typing import Dict, List, Any, Optional, Union

try:
    # Import pyoxigraph
    import pyoxigraph as ox
    logger = logging.getLogger(__name__)
    logger.info("Successfully imported pyoxigraph")
except ImportError as e:
    logger = logging.getLogger(__name__)
    logger.error(f"Failed to import pyoxigraph: {e}")
    print(f"Failed to import pyoxigraph: {e}", file=sys.stderr)


class RdfOperations:
    """
    Provides operations for working directly with RDF in PyOxigraph.
    """
    
    def __init__(self, store_manager):
        self.store_manager = store_manager
    
    def create_named_node(self, uri: str) -> Dict[str, Any]:
        """
        Create a named node (IRI).
        
        Args:
            uri: The URI of the named node
            
        Returns:
            A dictionary with the result and the node
        """
        try:
            node = ox.NamedNode(uri)
            return {
                "success": True,
                "node_type": "named_node",
                "value": uri
            }
        except Exception as e:
            logger.error(f"Error creating named node: {e}")
            return {
                "success": False,
                "message": f"Failed to create named node: {str(e)}"
            }
    
    def create_blank_node(self, value: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a blank node.
        
        Args:
            value: Optional value for the blank node
            
        Returns:
            A dictionary with the result and the node
        """
        try:
            if value:
                node = ox.BlankNode(value)
                node_value = value
            else:
                node = ox.BlankNode()
                node_value = str(node)
            
            return {
                "success": True,
                "node_type": "blank_node",
                "value": node_value
            }
        except Exception as e:
            logger.error(f"Error creating blank node: {e}")
            return {
                "success": False,
                "message": f"Failed to create blank node: {str(e)}"
            }
    
    def create_literal(self, value: str, datatype: Optional[str] = None, language: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a literal.
        
        Args:
            value: The value of the literal
            datatype: Optional datatype URI
            language: Optional language tag
            
        Returns:
            A dictionary with the result and the literal
        """
        try:
            if datatype and language:
                return {
                    "success": False,
                    "message": "Cannot specify both datatype and language"
                }
            
            if datatype:
                datatype_node = ox.NamedNode(datatype)
                literal = ox.Literal(value, datatype=datatype_node)
            elif language:
                literal = ox.Literal(value, language=language)
            else:
                literal = ox.Literal(value)
            
            return {
                "success": True,
                "node_type": "literal",
                "value": value,
                "datatype": datatype,
                "language": language
            }
        except Exception as e:
            logger.error(f"Error creating literal: {e}")
            return {
                "success": False,
                "message": f"Failed to create literal: {str(e)}"
            }
    
    def _parse_node(self, node_info: Dict[str, Any]):
        """
        Parse a node from its dictionary representation.
        
        Args:
            node_info: Dictionary with node info
            
        Returns:
            A PyOxigraph node
        """
        node_type = node_info.get("node_type", "").lower()
        value = node_info.get("value", "")
        
        if node_type == "named_node":
            return ox.NamedNode(value)
        elif node_type == "blank_node":
            return ox.BlankNode(value) if value else ox.BlankNode()
        elif node_type == "literal":
            datatype = node_info.get("datatype")
            language = node_info.get("language")
            
            if datatype:
                datatype_node = ox.NamedNode(datatype)
                return ox.Literal(value, datatype=datatype_node)
            elif language:
                return ox.Literal(value, language=language)
            else:
                return ox.Literal(value)
        else:
            raise ValueError(f"Unknown node type: {node_type}")
    
    def create_quad(self, subject: Dict[str, Any], predicate: Dict[str, Any], 
                   object: Dict[str, Any], graph_name: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Create a quad (or triple if no graph name is provided).
        
        Args:
            subject: Dictionary with subject node info
            predicate: Dictionary with predicate node info
            object: Dictionary with object node info
            graph_name: Optional dictionary with graph name node info
            
        Returns:
            A dictionary with the result and the quad
        """
        try:
            # Parse nodes
            subj = self._parse_node(subject)
            pred = self._parse_node(predicate)
            obj = self._parse_node(object)
            
            if graph_name:
                graph = self._parse_node(graph_name)
                quad = ox.Quad(subj, pred, obj, graph)
            else:
                quad = ox.Quad(subj, pred, obj)
            
            return {
                "success": True,
                "quad": {
                    "subject": subject,
                    "predicate": predicate,
                    "object": object,
                    "graph_name": graph_name
                }
            }
        except Exception as e:
            logger.error(f"Error creating quad: {e}")
            return {
                "success": False,
                "message": f"Failed to create quad: {str(e)}"
            }
    
    def add_quad(self, store_id: Optional[str], subject: Dict[str, Any], predicate: Dict[str, Any], 
                object: Dict[str, Any], graph_name: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Add a quad to a store.
        
        Args:
            store_id: Optional store ID (uses default if None)
            subject: Dictionary with subject node info
            predicate: Dictionary with predicate node info
            object: Dictionary with object node info
            graph_name: Optional dictionary with graph name node info
            
        Returns:
            A dictionary with the result
        """
        try:
            # Get the store
            store = self.store_manager.get_store(store_id)
            if not store:
                return {
                    "success": False,
                    "message": f"Store not found: {store_id or '(default)'}"
                }
            
            # Create the quad
            quad_result = self.create_quad(subject, predicate, object, graph_name)
            if not quad_result["success"]:
                return quad_result
            
            # Parse nodes
            subj = self._parse_node(subject)
            pred = self._parse_node(predicate)
            obj = self._parse_node(object)
            
            if graph_name:
                graph = self._parse_node(graph_name)
                quad = ox.Quad(subj, pred, obj, graph)
            else:
                quad = ox.Quad(subj, pred, obj)
            
            # Add to store
            store.add(quad)
            
            return {
                "success": True,
                "message": "Added quad to store",
                "store_id": store_id or self.store_manager.default_store,
                "quad": quad_result["quad"]
            }
        except Exception as e:
            logger.error(f"Error adding quad: {e}")
            return {
                "success": False,
                "message": f"Failed to add quad: {str(e)}"
            }
    
    def remove_quad(self, store_id: Optional[str], subject: Dict[str, Any], predicate: Dict[str, Any], 
                   object: Dict[str, Any], graph_name: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Remove a quad from a store.
        
        Args:
            store_id: Optional store ID (uses default if None)
            subject: Dictionary with subject node info
            predicate: Dictionary with predicate node info
            object: Dictionary with object node info
            graph_name: Optional dictionary with graph name node info
            
        Returns:
            A dictionary with the result
        """
        try:
            # Get the store
            store = self.store_manager.get_store(store_id)
            if not store:
                return {
                    "success": False,
                    "message": f"Store not found: {store_id or '(default)'}"
                }
            
            # Create the quad
            quad_result = self.create_quad(subject, predicate, object, graph_name)
            if not quad_result["success"]:
                return quad_result
            
            # Parse nodes
            subj = self._parse_node(subject)
            pred = self._parse_node(predicate)
            obj = self._parse_node(object)
            
            if graph_name:
                graph = self._parse_node(graph_name)
                quad = ox.Quad(subj, pred, obj, graph)
            else:
                quad = ox.Quad(subj, pred, obj)
            
            # Remove from store
            store.remove(quad)
            
            return {
                "success": True,
                "message": "Removed quad from store",
                "store_id": store_id or self.store_manager.default_store,
                "quad": quad_result["quad"]
            }
        except Exception as e:
            logger.error(f"Error removing quad: {e}")
            return {
                "success": False,
                "message": f"Failed to remove quad: {str(e)}"
            }
    
    def bulk_load(self, store_id: Optional[str], quads: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Bulk load quads into a store.
        
        Args:
            store_id: Optional store ID (uses default if None)
            quads: List of quad dictionaries
            
        Returns:
            A dictionary with the result
        """
        try:
            # Get the store
            store = self.store_manager.get_store(store_id)
            if not store:
                return {
                    "success": False,
                    "message": f"Store not found: {store_id or '(default)'}"
                }
            
            # Convert quads to PyOxigraph quads
            ox_quads = []
            for quad_dict in quads:
                subject = quad_dict.get("subject")
                predicate = quad_dict.get("predicate")
                object = quad_dict.get("object")
                graph_name = quad_dict.get("graph_name")
                
                # Parse nodes
                subj = self._parse_node(subject)
                pred = self._parse_node(predicate)
                obj = self._parse_node(object)
                
                if graph_name:
                    graph = self._parse_node(graph_name)
                    quad = ox.Quad(subj, pred, obj, graph)
                else:
                    quad = ox.Quad(subj, pred, obj)
                
                ox_quads.append(quad)
            
            # Bulk load quads
            store.bulk_load(ox_quads)
            
            return {
                "success": True,
                "message": f"Bulk loaded {len(quads)} quads into store",
                "store_id": store_id or self.store_manager.default_store,
                "count": len(quads)
            }
        except Exception as e:
            logger.error(f"Error bulk loading quads: {e}")
            return {
                "success": False,
                "message": f"Failed to bulk load quads: {str(e)}"
            }
    
    def bulk_extend(self, store_id: Optional[str], quads: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Bulk extend a store with quads.
        
        Args:
            store_id: Optional store ID (uses default if None)
            quads: List of quad dictionaries
            
        Returns:
            A dictionary with the result
        """
        try:
            # Get the store
            store = self.store_manager.get_store(store_id)
            if not store:
                return {
                    "success": False,
                    "message": f"Store not found: {store_id or '(default)'}"
                }
            
            # Convert quads to PyOxigraph quads
            ox_quads = []
            for quad_dict in quads:
                subject = quad_dict.get("subject")
                predicate = quad_dict.get("predicate")
                object = quad_dict.get("object")
                graph_name = quad_dict.get("graph_name")
                
                # Parse nodes
                subj = self._parse_node(subject)
                pred = self._parse_node(predicate)
                obj = self._parse_node(object)
                
                if graph_name:
                    graph = self._parse_node(graph_name)
                    quad = ox.Quad(subj, pred, obj, graph)
                else:
                    quad = ox.Quad(subj, pred, obj)
                
                ox_quads.append(quad)
            
            # Bulk extend store
            store.extend(ox_quads)
            
            return {
                "success": True,
                "message": f"Bulk extended store with {len(quads)} quads",
                "store_id": store_id or self.store_manager.default_store,
                "count": len(quads)
            }
        except Exception as e:
            logger.error(f"Error bulk extending store: {e}")
            return {
                "success": False,
                "message": f"Failed to bulk extend store: {str(e)}"
            }
    
    def quads_for_pattern(self, store_id: Optional[str], subject: Optional[Dict[str, Any]] = None, 
                         predicate: Optional[Dict[str, Any]] = None, object: Optional[Dict[str, Any]] = None, 
                         graph_name: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Find quads matching a pattern.
        
        Args:
            store_id: Optional store ID (uses default if None)
            subject: Optional dictionary with subject node info
            predicate: Optional dictionary with predicate node info
            object: Optional dictionary with object node info
            graph_name: Optional dictionary with graph name node info
            
        Returns:
            A dictionary with the result and matching quads
        """
        try:
            # Get the store
            store = self.store_manager.get_store(store_id)
            if not store:
                return {
                    "success": False,
                    "message": f"Store not found: {store_id or '(default)'}"
                }
            
            # Parse pattern nodes
            subj = self._parse_node(subject) if subject else None
            pred = self._parse_node(predicate) if predicate else None
            obj = self._parse_node(object) if object else None
            graph = self._parse_node(graph_name) if graph_name else None
            
            # Get matching quads
            matching_quads = list(store.quads_for_pattern(subj, pred, obj, graph))
            
            # Convert quads to dictionaries
            result_quads = []
            for quad in matching_quads:
                quad_dict = {
                    "subject": {
                        "node_type": "named_node" if isinstance(quad.subject, ox.NamedNode) else ("blank_node" if isinstance(quad.subject, ox.BlankNode) else "literal"),
                        "value": str(quad.subject)
                    },
                    "predicate": {
                        "node_type": "named_node",  # Predicates are always named nodes
                        "value": str(quad.predicate)
                    },
                    "object": {
                        "node_type": "named_node" if isinstance(quad.object, ox.NamedNode) else 
                                    ("blank_node" if isinstance(quad.object, ox.BlankNode) else "literal"),
                        "value": str(quad.object)
                    }
                }
                
                if quad.graph_name:
                    quad_dict["graph_name"] = {
                        "node_type": "named_node" if isinstance(quad.graph_name, ox.NamedNode) else "blank_node",
                        "value": str(quad.graph_name)
                    }
                
                # Add datatype or language for literals
                if isinstance(quad.object, ox.Literal):
                    if quad.object.datatype:
                        quad_dict["object"]["datatype"] = str(quad.object.datatype)
                    if quad.object.language:
                        quad_dict["object"]["language"] = quad.object.language
                
                result_quads.append(quad_dict)
            
            return {
                "success": True,
                "store_id": store_id or self.store_manager.default_store,
                "quads": result_quads,
                "count": len(result_quads)
            }
        except Exception as e:
            logger.error(f"Error finding quads: {e}")
            return {
                "success": False,
                "message": f"Failed to find quads: {str(e)}"
            }

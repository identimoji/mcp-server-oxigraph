"""
Knowledge Graph API for PyOxigraph.

This module provides higher-level knowledge graph abstractions built on top of PyOxigraph.
These functions are NOT part of the core PyOxigraph library but provide a more
user-friendly interface for knowledge graph operations without requiring direct
understanding of RDF and SPARQL.

The Knowledge Graph API represents a demonstration of how PyOxigraph can be used
to implement a simple knowledge graph system.
"""

import logging
from typing import Dict, List, Optional, Any, Union
import pyoxigraph

logger = logging.getLogger(__name__)

# Type definitions for better readability
Entity = Dict[str, Any]
Relation = Dict[str, Any]
Observation = str


def create_entities(entities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Create multiple new entities in the knowledge graph.
    
    This is a higher-level abstraction built on top of PyOxigraph and is not
    part of the core PyOxigraph library.
    
    Args:
        entities: List of entity dictionaries, each containing:
            - name: The name of the entity
            - entityType: The type of the entity
            - observations: List of observation strings
            
    Returns:
        List of created entities
    """
    from .store import oxigraph_get_store
    store = oxigraph_get_store()
    if not store:
        raise ValueError("No store available")
    
    created_entities = []
    
    for entity_data in entities:
        name = entity_data.get("name")
        entity_type = entity_data.get("entityType")
        observations = entity_data.get("observations", [])
        
        if not name or not entity_type:
            logger.warning(f"Skipping entity with missing name or type: {entity_data}")
            continue
        
        # Create entity node
        entity_iri = f"http://example.org/entity/{name}"
        entity_node = pyoxigraph.NamedNode(entity_iri)
        type_node = pyoxigraph.NamedNode("http://example.org/type")
        name_node = pyoxigraph.NamedNode("http://example.org/name")
        
        # Add entity type
        store.add(pyoxigraph.Quad(
            entity_node,
            type_node,
            pyoxigraph.Literal(entity_type)
        ))
        
        # Add entity name
        store.add(pyoxigraph.Quad(
            entity_node,
            name_node,
            pyoxigraph.Literal(name)
        ))
        
        # Add observations
        observation_node = pyoxigraph.NamedNode("http://example.org/observation")
        for obs in observations:
            store.add(pyoxigraph.Quad(
                entity_node,
                observation_node,
                pyoxigraph.Literal(obs)
            ))
        
        created_entities.append(entity_data)
    
    return created_entities


def create_relations(relations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Create multiple new relations between entities in the knowledge graph.
    
    This is a higher-level abstraction built on top of PyOxigraph and is not
    part of the core PyOxigraph library.
    
    Args:
        relations: List of relation dictionaries, each containing:
            - from: The name of the entity where the relation starts
            - to: The name of the entity where the relation ends
            - relationType: The type of the relation
            
    Returns:
        List of created relations
    """
    from .store import oxigraph_get_store
    store = oxigraph_get_store()
    if not store:
        raise ValueError("No store available")
    
    created_relations = []
    
    for relation_data in relations:
        from_entity = relation_data.get("from")
        to_entity = relation_data.get("to")
        relation_type = relation_data.get("relationType")
        
        if not from_entity or not to_entity or not relation_type:
            logger.warning(f"Skipping relation with missing data: {relation_data}")
            continue
        
        # Create entity nodes
        from_iri = f"http://example.org/entity/{from_entity}"
        to_iri = f"http://example.org/entity/{to_entity}"
        from_node = pyoxigraph.NamedNode(from_iri)
        to_node = pyoxigraph.NamedNode(to_iri)
        
        # Create relation predicate
        relation_iri = f"http://example.org/relation/{relation_type}"
        relation_node = pyoxigraph.NamedNode(relation_iri)
        
        # Add relation
        store.add(pyoxigraph.Quad(
            from_node,
            relation_node,
            to_node
        ))
        
        created_relations.append(relation_data)
    
    return created_relations


def add_observations(observations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Add new observations to existing entities in the knowledge graph.
    
    This is a higher-level abstraction built on top of PyOxigraph and is not
    part of the core PyOxigraph library.
    
    Args:
        observations: List of observation dictionaries, each containing:
            - entityName: The name of the entity to add observations to
            - contents: List of observation strings to add
            
    Returns:
        List of dictionaries with the entity name and added observations
    """
    from .store import oxigraph_get_store
    store = oxigraph_get_store()
    if not store:
        raise ValueError("No store available")
    
    results = []
    
    for obs_data in observations:
        entity_name = obs_data.get("entityName")
        contents = obs_data.get("contents", [])
        
        if not entity_name or not contents:
            logger.warning(f"Skipping observations with missing data: {obs_data}")
            continue
        
        # Get entity node
        entity_iri = f"http://example.org/entity/{entity_name}"
        entity_node = pyoxigraph.NamedNode(entity_iri)
        
        # Add observations
        observation_node = pyoxigraph.NamedNode("http://example.org/observation")
        added_observations = []
        
        for content in contents:
            store.add(pyoxigraph.Quad(
                entity_node,
                observation_node,
                pyoxigraph.Literal(content)
            ))
            added_observations.append(content)
        
        results.append({
            "entityName": entity_name,
            "addedObservations": added_observations
        })
    
    return results


def delete_entities(entity_names: List[str]) -> str:
    """
    Delete multiple entities and their associated relations from the knowledge graph.
    
    This is a higher-level abstraction built on top of PyOxigraph and is not
    part of the core PyOxigraph library.
    
    Args:
        entity_names: List of entity names to delete
        
    Returns:
        Success message
    """
    from .store import oxigraph_get_store
    store = oxigraph_get_store()
    if not store:
        raise ValueError("No store available")
    
    for name in entity_names:
        entity_iri = f"http://example.org/entity/{name}"
        entity_node = pyoxigraph.NamedNode(entity_iri)
        
        # First find all quads where this entity is the subject
        subject_quads = list(store.quads_for_pattern(
            subject=entity_node,
            predicate=None,
            object=None
        ))
        
        # Then find all quads where this entity is the object
        object_quads = list(store.quads_for_pattern(
            subject=None,
            predicate=None,
            object=entity_node
        ))
        
        # Remove all quads
        for quad in subject_quads + object_quads:
            store.remove(quad)
    
    return "Entities deleted successfully"


def delete_observations(deletions: List[Dict[str, Any]]) -> str:
    """
    Delete specific observations from entities in the knowledge graph.
    
    This is a higher-level abstraction built on top of PyOxigraph and is not
    part of the core PyOxigraph library.
    
    Args:
        deletions: List of deletion dictionaries, each containing:
            - entityName: The name of the entity containing the observations
            - observations: List of observation strings to delete
            
    Returns:
        Success message
    """
    from .store import oxigraph_get_store
    store = oxigraph_get_store()
    if not store:
        raise ValueError("No store available")
    
    for deletion in deletions:
        entity_name = deletion.get("entityName")
        
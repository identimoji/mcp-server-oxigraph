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
from .store import get_store

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
    store = get_store()
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
    store = get_store()
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
    store = get_store()
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
    store = get_store()
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
    store = get_store()
    if not store:
        raise ValueError("No store available")
    
    for deletion in deletions:
        entity_name = deletion.get("entityName")
        observations_to_delete = deletion.get("observations", [])
        
        if not entity_name or not observations_to_delete:
            logger.warning(f"Skipping deletion with missing data: {deletion}")
            continue
        
        # Get entity node
        entity_iri = f"http://example.org/entity/{entity_name}"
        entity_node = pyoxigraph.NamedNode(entity_iri)
        observation_node = pyoxigraph.NamedNode("http://example.org/observation")
        
        # Find and remove matching observations
        for obs in observations_to_delete:
            quad = pyoxigraph.Quad(
                entity_node,
                observation_node,
                pyoxigraph.Literal(obs)
            )
            store.remove(quad)
    
    return "Observations deleted successfully"


def delete_relations(relations: List[Dict[str, Any]]) -> str:
    """
    Delete multiple relations from the knowledge graph.
    
    This is a higher-level abstraction built on top of PyOxigraph and is not
    part of the core PyOxigraph library.
    
    Args:
        relations: List of relation dictionaries to delete, each containing:
            - from: The name of the entity where the relation starts
            - to: The name of the entity where the relation ends
            - relationType: The type of the relation
            
    Returns:
        Success message
    """
    store = get_store()
    if not store:
        raise ValueError("No store available")
    
    for relation in relations:
        from_entity = relation.get("from")
        to_entity = relation.get("to")
        relation_type = relation.get("relationType")
        
        if not from_entity or not to_entity or not relation_type:
            logger.warning(f"Skipping relation deletion with missing data: {relation}")
            continue
        
        # Create entity nodes
        from_iri = f"http://example.org/entity/{from_entity}"
        to_iri = f"http://example.org/entity/{to_entity}"
        from_node = pyoxigraph.NamedNode(from_iri)
        to_node = pyoxigraph.NamedNode(to_iri)
        
        # Create relation predicate
        relation_iri = f"http://example.org/relation/{relation_type}"
        relation_node = pyoxigraph.NamedNode(relation_iri)
        
        # Remove relation
        quad = pyoxigraph.Quad(
            from_node,
            relation_node,
            to_node
        )
        store.remove(quad)
    
    return "Relations deleted successfully"


def search_nodes(query: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Search for nodes in the knowledge graph based on a query.
    
    This is a higher-level abstraction built on top of PyOxigraph and is not
    part of the core PyOxigraph library.
    
    Args:
        query: The search query to match against entity names, types, and observation content
        
    Returns:
        Dictionary with entities and relations that match the query
    """
    store = get_store()
    if not store:
        raise ValueError("No store available")
    
    # Build a SPARQL query to search for entities
    sparql_query = f"""
    SELECT DISTINCT ?entity ?name ?type
    WHERE {{
      ?entity <http://example.org/name> ?name .
      ?entity <http://example.org/type> ?type .
      OPTIONAL {{ ?entity <http://example.org/observation> ?observation }}
      FILTER(
        CONTAINS(LCASE(STR(?name)), LCASE("{query}")) ||
        CONTAINS(LCASE(STR(?type)), LCASE("{query}")) ||
        CONTAINS(LCASE(STR(?observation)), LCASE("{query}"))
      )
    }}
    """
    
    try:
        results = store.query(sparql_query)
        
        # Process results
        entities = []
        relations = []
        
        # Get all matching entities
        for result in results:
            entity_iri = str(result["entity"])
            entity_name = str(result["name"])
            entity_type = str(result["type"])
            
            # Get observations for this entity
            observation_query = f"""
            SELECT ?observation
            WHERE {{
              <{entity_iri}> <http://example.org/observation> ?observation .
            }}
            """
            observation_results = store.query(observation_query)
            observations = [str(r["observation"]) for r in observation_results]
            
            entities.append({
                "type": "entity",
                "name": entity_name,
                "entityType": entity_type,
                "observations": observations
            })
        
        # We could also search for relations here, but to keep it simple,
        # we'll just return the matching entities
        
        return {
            "entities": entities,
            "relations": relations
        }
    
    except Exception as e:
        logger.error(f"Error searching nodes: {e}")
        return {"entities": [], "relations": []}


def open_nodes(names: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Open specific nodes in the knowledge graph by their names.
    
    This is a higher-level abstraction built on top of PyOxigraph and is not
    part of the core PyOxigraph library.
    
    Args:
        names: List of entity names to retrieve
        
    Returns:
        Dictionary with entities and their relations
    """
    store = get_store()
    if not store:
        raise ValueError("No store available")
    
    entities = []
    relations = []
    
    for name in names:
        # Create entity IRI
        entity_iri = f"http://example.org/entity/{name}"
        entity_node = pyoxigraph.NamedNode(entity_iri)
        
        # Get entity type
        type_node = pyoxigraph.NamedNode("http://example.org/type")
        type_results = list(store.quads_for_pattern(
            subject=entity_node,
            predicate=type_node,
            object=None
        ))
        
        if not type_results:
            logger.warning(f"Entity not found: {name}")
            continue
        
        entity_type = str(type_results[0].object.value)
        
        # Get observations
        observation_node = pyoxigraph.NamedNode("http://example.org/observation")
        observation_results = list(store.quads_for_pattern(
            subject=entity_node,
            predicate=observation_node,
            object=None
        ))
        
        observations = [str(quad.object.value) for quad in observation_results]
        
        entities.append({
            "type": "entity",
            "name": name,
            "entityType": entity_type,
            "observations": observations
        })
        
        # We could also get relations here, but for simplicity we'll skip them
    
    return {
        "entities": entities,
        "relations": relations
    }


def read_graph() -> Dict[str, List[Dict[str, Any]]]:
    """
    Read the entire knowledge graph.
    
    This is a higher-level abstraction built on top of PyOxigraph and is not
    part of the core PyOxigraph library.
    
    Returns:
        Dictionary with all entities and relations in the knowledge graph
    """
    store = get_store()
    if not store:
        raise ValueError("No store available")
    
    # Get all entities with their names and types
    sparql_query = """
    SELECT ?entity ?name ?type
    WHERE {
      ?entity <http://example.org/name> ?name .
      ?entity <http://example.org/type> ?type .
    }
    """
    
    try:
        results = store.query(sparql_query)
        
        entities = []
        entity_map = {}  # Map entity IRIs to their positions in the entities list
        
        # Process entities
        for result in results:
            entity_iri = str(result["entity"])
            entity_name = str(result["name"])
            entity_type = str(result["type"])
            
            # Get observations for this entity
            observation_query = f"""
            SELECT ?observation
            WHERE {{
              <{entity_iri}> <http://example.org/observation> ?observation .
            }}
            """
            observation_results = store.query(observation_query)
            observations = [str(r["observation"]) for r in observation_results]
            
            entity = {
                "type": "entity",
                "name": entity_name,
                "entityType": entity_type,
                "observations": observations
            }
            
            entities.append(entity)
            entity_map[entity_iri] = len(entities) - 1
        
        # Get all relations
        relations = []
        
        # Query to find all relation predicates
        relation_predicates_query = """
        SELECT DISTINCT ?predicate
        WHERE {
          ?s ?predicate ?o .
          FILTER(STRSTARTS(STR(?predicate), "http://example.org/relation/"))
        }
        """
        
        predicate_results = store.query(relation_predicates_query)
        
        # For each relation predicate, find all instances
        for predicate_result in predicate_results:
            relation_predicate = predicate_result["predicate"]
            relation_type = str(relation_predicate).replace("http://example.org/relation/", "")
            
            # Find all instances of this relation
            relation_query = f"""
            SELECT ?s ?o
            WHERE {{
              ?s <{relation_predicate}> ?o .
            }}
            """
            
            relation_results = store.query(relation_query)
            
            for relation_result in relation_results:
                from_entity_iri = str(relation_result["s"])
                to_entity_iri = str(relation_result["o"])
                
                # Get entity names
                from_name_query = f"""
                SELECT ?name
                WHERE {{
                  <{from_entity_iri}> <http://example.org/name> ?name .
                }}
                """
                
                to_name_query = f"""
                SELECT ?name
                WHERE {{
                  <{to_entity_iri}> <http://example.org/name> ?name .
                }}
                """
                
                from_name_results = store.query(from_name_query)
                to_name_results = store.query(to_name_query)
                
                if from_name_results and to_name_results:
                    from_name = str(list(from_name_results)[0]["name"])
                    to_name = str(list(to_name_results)[0]["name"])
                    
                    relations.append({
                        "type": "relation",
                        "from": from_name,
                        "to": to_name,
                        "relationType": relation_type
                    })
        
        return {
            "entities": entities,
            "relations": relations
        }
    
    except Exception as e:
        logger.error(f"Error reading graph: {e}")
        return {"entities": [], "relations": []}

# Helper function to get the store instance
def get_store() -> Optional[pyoxigraph.Store]:
    """Get the default store instance."""
    try:
        from .store import oxigraph_get_store
        return oxigraph_get_store()
    except ImportError:
        logger.error("Failed to import oxigraph_get_store")
        return None
    except Exception as e:
        logger.error(f"Error getting store: {e}")
        return None

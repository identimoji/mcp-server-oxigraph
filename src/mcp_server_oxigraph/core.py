"""
Core implementation of the knowledge graph operations using Oxigraph.
"""

import os
import logging
import pyoxigraph
from typing import List, Dict, Optional, Union, Any, Tuple, Set, Iterator
from pathlib import Path

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OxigraphKnowledgeGraph:
    """
    A knowledge graph implementation using Oxigraph.
    """
    
    def __init__(self, store_path: Optional[str] = None):
        """
        Initialize the knowledge graph.
        
        Args:
            store_path: Optional path to store the knowledge graph. If None, an in-memory store is used.
        """
        self._store = None
        self.store_path = store_path
        
        # If no store path is provided, check environment variable
        if self.store_path is None:
            self.store_path = os.environ.get("OXIGRAPH_DB_PATH")
        
        # If still no store path, use in-memory store
        if self.store_path is None:
            logger.info("Initializing OxigraphKnowledgeGraph in-memory")
        else:
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.store_path), exist_ok=True)
            logger.info(f"Initializing OxigraphKnowledgeGraph at {self.store_path}")
        
        self._initialize_store()
    
    def _initialize_store(self) -> None:
        """Initialize the Oxigraph store."""
        try:
            if self.store_path is None:
                # In-memory store
                self._store = pyoxigraph.Store()
            else:
                # Persistent store on disk
                os.makedirs(os.path.dirname(self.store_path), exist_ok=True)
                self._store = pyoxigraph.Store(self.store_path)
            
            logger.info("Store initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize store: {e}")
            raise
    
    @property
    def store(self) -> pyoxigraph.Store:
        """Get the underlying Oxigraph store."""
        return self._store
    
    # Entity management methods
    
    def create_entity(self, name: str, entity_type: str, observations: List[str]) -> None:
        """
        Create a new entity in the knowledge graph.
        
        Args:
            name: The name of the entity
            entity_type: The type of the entity
            observations: Observations associated with the entity
        """
        try:
            entity_node = pyoxigraph.NamedNode(f"http://example.org/entity/{name}")
            type_node = pyoxigraph.NamedNode("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
            entity_type_node = pyoxigraph.NamedNode(f"http://example.org/type/{entity_type}")
            
            # Add entity type triple
            self._store.add(pyoxigraph.Quad(
                entity_node,
                type_node,
                entity_type_node,
                None  # Default graph
            ))
            
            # Add name triple
            name_predicate = pyoxigraph.NamedNode("http://example.org/predicate/name")
            self._store.add(pyoxigraph.Quad(
                entity_node,
                name_predicate,
                pyoxigraph.Literal(name),
                None  # Default graph
            ))
            
            # Add observations
            observation_predicate = pyoxigraph.NamedNode("http://example.org/predicate/observation")
            for idx, observation in enumerate(observations):
                observation_node = pyoxigraph.BlankNode(f"observation_{name}_{idx}")
                
                # Link observation to entity
                self._store.add(pyoxigraph.Quad(
                    entity_node,
                    observation_predicate,
                    observation_node,
                    None  # Default graph
                ))
                
                # Add observation content
                content_predicate = pyoxigraph.NamedNode("http://example.org/predicate/content")
                self._store.add(pyoxigraph.Quad(
                    observation_node,
                    content_predicate,
                    pyoxigraph.Literal(observation),
                    None  # Default graph
                ))
            
            logger.info(f"Created entity: {name} of type {entity_type} with {len(observations)} observations")
        except Exception as e:
            logger.error(f"Failed to create entity {name}: {e}")
            raise
    
    def create_entities(self, entities: List[Dict[str, Any]]) -> None:
        """
        Create multiple entities in the knowledge graph.
        
        Args:
            entities: List of entity dictionaries, each with name, entityType, and observations
        """
        for entity in entities:
            self.create_entity(
                name=entity["name"],
                entity_type=entity["entityType"],
                observations=entity["observations"]
            )
    
    def add_observations(self, entity_name: str, observations: List[str]) -> None:
        """
        Add observations to an existing entity.
        
        Args:
            entity_name: Name of the entity to add observations to
            observations: List of observation contents to add
        """
        try:
            entity_node = pyoxigraph.NamedNode(f"http://example.org/entity/{entity_name}")
            observation_predicate = pyoxigraph.NamedNode("http://example.org/predicate/observation")
            
            # Get current number of observations to avoid ID collisions
            query = f"""
            SELECT (COUNT(?o) as ?count)
            WHERE {{
                <http://example.org/entity/{entity_name}> <http://example.org/predicate/observation> ?o .
            }}
            """
            
            result = list(self._store.query(query))
            if result and result[0].get("count"):
                start_idx = int(result[0]["count"].value)
            else:
                start_idx = 0
            
            # Add new observations
            content_predicate = pyoxigraph.NamedNode("http://example.org/predicate/content")
            for idx, observation in enumerate(observations, start=start_idx):
                observation_node = pyoxigraph.BlankNode(f"observation_{entity_name}_{idx}")
                
                # Link observation to entity
                self._store.add(pyoxigraph.Quad(
                    entity_node,
                    observation_predicate,
                    observation_node,
                    None  # Default graph
                ))
                
                # Add observation content
                self._store.add(pyoxigraph.Quad(
                    observation_node,
                    content_predicate,
                    pyoxigraph.Literal(observation),
                    None  # Default graph
                ))
            
            logger.info(f"Added {len(observations)} observations to entity {entity_name}")
        except Exception as e:
            logger.error(f"Failed to add observations to entity {entity_name}: {e}")
            raise
    
    def create_relation(self, from_entity: str, relation_type: str, to_entity: str) -> None:
        """
        Create a relation between two entities.
        
        Args:
            from_entity: Name of the source entity
            relation_type: Type of the relation
            to_entity: Name of the target entity
        """
        try:
            from_node = pyoxigraph.NamedNode(f"http://example.org/entity/{from_entity}")
            to_node = pyoxigraph.NamedNode(f"http://example.org/entity/{to_entity}")
            relation_predicate = pyoxigraph.NamedNode(f"http://example.org/relation/{relation_type}")
            
            # Add relation triple
            self._store.add(pyoxigraph.Quad(
                from_node,
                relation_predicate,
                to_node,
                None  # Default graph
            ))
            
            logger.info(f"Created relation: {from_entity} --{relation_type}--> {to_entity}")
        except Exception as e:
            logger.error(f"Failed to create relation: {e}")
            raise
    
    def create_relations(self, relations: List[Dict[str, str]]) -> None:
        """
        Create multiple relations in the knowledge graph.
        
        Args:
            relations: List of relation dictionaries, each with from, relationType, and to fields
        """
        for relation in relations:
            self.create_relation(
                from_entity=relation["from"],
                relation_type=relation["relationType"],
                to_entity=relation["to"]
            )
    
    def delete_entity(self, entity_name: str) -> None:
        """
        Delete an entity and all its associated relations and observations.
        
        Args:
            entity_name: Name of the entity to delete
        """
        try:
            entity_node = pyoxigraph.NamedNode(f"http://example.org/entity/{entity_name}")
            
            # First, find all observation nodes to delete their content triples
            observation_predicate = pyoxigraph.NamedNode("http://example.org/predicate/observation")
            content_predicate = pyoxigraph.NamedNode("http://example.org/predicate/content")
            
            # Use SPARQL to find all observations
            query = f"""
            SELECT ?obs
            WHERE {{
                <http://example.org/entity/{entity_name}> <http://example.org/predicate/observation> ?obs .
            }}
            """
            
            observation_nodes = [binding["obs"] for binding in self._store.query(query)]
            
            # Delete observation content triples
            for obs_node in observation_nodes:
                for quad in self._store.quads_for_pattern(obs_node, content_predicate, None, None):
                    self._store.remove(quad)
            
            # Delete all quads where the entity is the subject
            for quad in self._store.quads_for_pattern(entity_node, None, None, None):
                self._store.remove(quad)
            
            # Delete all quads where the entity is the object
            for quad in self._store.quads_for_pattern(None, None, entity_node, None):
                self._store.remove(quad)
            
            logger.info(f"Deleted entity: {entity_name}")
        except Exception as e:
            logger.error(f"Failed to delete entity {entity_name}: {e}")
            raise
    
    def delete_entities(self, entity_names: List[str]) -> None:
        """
        Delete multiple entities from the knowledge graph.
        
        Args:
            entity_names: List of entity names to delete
        """
        for name in entity_names:
            self.delete_entity(name)
    
    def delete_relation(self, from_entity: str, relation_type: str, to_entity: str) -> None:
        """
        Delete a specific relation between entities.
        
        Args:
            from_entity: Name of the source entity
            relation_type: Type of the relation
            to_entity: Name of the target entity
        """
        try:
            from_node = pyoxigraph.NamedNode(f"http://example.org/entity/{from_entity}")
            to_node = pyoxigraph.NamedNode(f"http://example.org/entity/{to_entity}")
            relation_predicate = pyoxigraph.NamedNode(f"http://example.org/relation/{relation_type}")
            
            # Find and remove the specific relation
            for quad in self._store.quads_for_pattern(from_node, relation_predicate, to_node, None):
                self._store.remove(quad)
            
            logger.info(f"Deleted relation: {from_entity} --{relation_type}--> {to_entity}")
        except Exception as e:
            logger.error(f"Failed to delete relation: {e}")
            raise
    
    def delete_relations(self, relations: List[Dict[str, str]]) -> None:
        """
        Delete multiple relations from the knowledge graph.
        
        Args:
            relations: List of relation dictionaries, each with from, relationType, and to fields
        """
        for relation in relations:
            self.delete_relation(
                from_entity=relation["from"],
                relation_type=relation["relationType"],
                to_entity=relation["to"]
            )
    
    def delete_observation(self, entity_name: str, observation: str) -> None:
        """
        Delete a specific observation from an entity.
        
        Args:
            entity_name: Name of the entity
            observation: Content of the observation to delete
        """
        try:
            entity_node = pyoxigraph.NamedNode(f"http://example.org/entity/{entity_name}")
            observation_predicate = pyoxigraph.NamedNode("http://example.org/predicate/observation")
            content_predicate = pyoxigraph.NamedNode("http://example.org/predicate/content")
            
            # Use SPARQL to find the specific observation
            query = f"""
            SELECT ?obs
            WHERE {{
                <http://example.org/entity/{entity_name}> <http://example.org/predicate/observation> ?obs .
                ?obs <http://example.org/predicate/content> "{observation}" .
            }}
            """
            
            results = list(self._store.query(query))
            if not results:
                logger.warning(f"Observation '{observation}' not found for entity {entity_name}")
                return
            
            observation_node = results[0]["obs"]
            
            # Delete content triple
            for quad in self._store.quads_for_pattern(observation_node, content_predicate, None, None):
                self._store.remove(quad)
            
            # Delete link from entity to observation
            for quad in self._store.quads_for_pattern(entity_node, observation_predicate, observation_node, None):
                self._store.remove(quad)
            
            logger.info(f"Deleted observation '{observation}' from entity {entity_name}")
        except Exception as e:
            logger.error(f"Failed to delete observation: {e}")
            raise
    
    def delete_observations(self, deletions: List[Dict[str, Any]]) -> None:
        """
        Delete specific observations from entities.
        
        Args:
            deletions: List of deletion dictionaries, each with entityName and observations
        """
        for deletion in deletions:
            entity_name = deletion["entityName"]
            for observation in deletion["observations"]:
                self.delete_observation(entity_name, observation)
    
    def read_graph(self) -> Dict[str, Any]:
        """
        Read the entire knowledge graph.
        
        Returns:
            A dictionary containing all entities and relations
        """
        try:
            # Find all entities
            entity_query = """
            SELECT ?entity ?type ?name
            WHERE {
                ?entity a ?type .
                ?entity <http://example.org/predicate/name> ?name .
            }
            """
            
            entity_results = self._store.query(entity_query)
            
            entities = []
            entity_map = {}  # For quick lookup of entity names
            
            for binding in entity_results:
                entity_uri = str(binding["entity"].value)
                entity_name = str(binding["name"].value)
                entity_type = str(binding["type"].value).split("/")[-1]  # Extract type name from URI
                
                # Store entity name for quick lookup
                entity_map[entity_uri] = entity_name
                
                # Get observations for this entity
                observation_query = f"""
                SELECT ?content
                WHERE {{
                    <{entity_uri}> <http://example.org/predicate/observation> ?obs .
                    ?obs <http://example.org/predicate/content> ?content .
                }}
                """
                
                observations = [
                    str(binding["content"].value)
                    for binding in self._store.query(observation_query)
                ]
                
                entities.append({
                    "name": entity_name,
                    "entityType": entity_type,
                    "observations": observations
                })
            
            # Find all relations
            relation_query = """
            PREFIX ex: <http://example.org/>
            
            SELECT ?from ?predicate ?to
            WHERE {
                ?from ?predicate ?to .
                FILTER(STRSTARTS(STR(?predicate), "http://example.org/relation/"))
            }
            """
            
            relation_results = self._store.query(relation_query)
            
            relations = []
            
            for binding in relation_results:
                from_uri = str(binding["from"].value)
                to_uri = str(binding["to"].value)
                predicate_uri = str(binding["predicate"].value)
                
                # Extract relation type from URI
                relation_type = predicate_uri.split("/")[-1]
                
                if from_uri in entity_map and to_uri in entity_map:
                    relations.append({
                        "from": entity_map[from_uri],
                        "relationType": relation_type,
                        "to": entity_map[to_uri]
                    })
            
            return {
                "entities": entities,
                "relations": relations
            }
        except Exception as e:
            logger.error(f"Failed to read graph: {e}")
            raise
    
    def search_nodes(self, query: str) -> Dict[str, Any]:
        """
        Search for nodes in the knowledge graph.
        
        Args:
            query: Search query string
            
        Returns:
            Dictionary with matched entities and their associated relations
        """
        try:
            # Escape the query string for SPARQL
            escaped_query = query.replace('"', '\\"')
            
            # Find entities by name
            name_query = f"""
            SELECT ?entity ?type ?name
            WHERE {{
                ?entity a ?type .
                ?entity <http://example.org/predicate/name> ?name .
                FILTER(CONTAINS(LCASE(STR(?name)), LCASE("{escaped_query}")))
            }}
            """
            
            # Find entities by observation content
            observation_query = f"""
            SELECT ?entity ?type ?name
            WHERE {{
                ?entity a ?type .
                ?entity <http://example.org/predicate/name> ?name .
                ?entity <http://example.org/predicate/observation> ?obs .
                ?obs <http://example.org/predicate/content> ?content .
                FILTER(CONTAINS(LCASE(STR(?content)), LCASE("{escaped_query}")))
            }}
            """
            
            # Execute queries
            name_results = list(self._store.query(name_query))
            observation_results = list(self._store.query(observation_query))
            
            # Combine and deduplicate results
            entity_uris = set()
            matched_entities = []
            entity_map = {}  # For quick lookup of entity names
            
            for binding in name_results + observation_results:
                entity_uri = str(binding["entity"].value)
                
                if entity_uri not in entity_uris:
                    entity_uris.add(entity_uri)
                    entity_name = str(binding["name"].value)
                    entity_type = str(binding["type"].value).split("/")[-1]
                    
                    # Store entity name for quick lookup
                    entity_map[entity_uri] = entity_name
                    
                    # Get observations for this entity
                    obs_query = f"""
                    SELECT ?content
                    WHERE {{
                        <{entity_uri}> <http://example.org/predicate/observation> ?obs .
                        ?obs <http://example.org/predicate/content> ?content .
                    }}
                    """
                    
                    observations = [
                        str(binding["content"].value)
                        for binding in self._store.query(obs_query)
                    ]
                    
                    matched_entities.append({
                        "name": entity_name,
                        "entityType": entity_type,
                        "observations": observations
                    })
            
            # Find relations involving matched entities
            matched_relations = []
            
            if entity_map:
                entity_list = ", ".join(f"<{uri}>" for uri in entity_uris)
                
                relation_query = f"""
                PREFIX ex: <http://example.org/>
                
                SELECT ?from ?predicate ?to
                WHERE {{
                    ?from ?predicate ?to .
                    FILTER(STRSTARTS(STR(?predicate), "http://example.org/relation/"))
                    FILTER(?from IN ({entity_list}) || ?to IN ({entity_list}))
                }}
                """
                
                relation_results = self._store.query(relation_query)
                
                for binding in relation_results:
                    from_uri = str(binding["from"].value)
                    to_uri = str(binding["to"].value)
                    predicate_uri = str(binding["predicate"].value)
                    
                    # Extract relation type from URI
                    relation_type = predicate_uri.split("/")[-1]
                    
                    if from_uri in entity_map and to_uri in entity_map:
                        matched_relations.append({
                            "from": entity_map[from_uri],
                            "relationType": relation_type,
                            "to": entity_map[to_uri]
                        })
            
            return {
                "entities": matched_entities,
                "relations": matched_relations
            }
        except Exception as e:
            logger.error(f"Failed to search nodes: {e}")
            raise
    
    def open_nodes(self, names: List[str]) -> Dict[str, Any]:
        """
        Open specific nodes in the knowledge graph by their names.
        
        Args:
            names: List of entity names to retrieve
            
        Returns:
            Dictionary with matched entities and their associated relations
        """
        try:
            matched_entities = []
            entity_uris = []
            entity_map = {}  # For quick lookup of entity names
            
            for name in names:
                # Escape the name for SPARQL
                escaped_name = name.replace('"', '\\"')
                
                # Find entity by exact name
                entity_query = f"""
                SELECT ?entity ?type ?name
                WHERE {{
                    ?entity a ?type .
                    ?entity <http://example.org/predicate/name> "{escaped_name}" .
                }}
                """
                
                entity_results = list(self._store.query(entity_query))
                
                if entity_results:
                    binding = entity_results[0]
                    entity_uri = str(binding["entity"].value)
                    entity_name = name
                    entity_type = str(binding["type"].value).split("/")[-1]
                    
                    entity_uris.append(entity_uri)
                    entity_map[entity_uri] = entity_name
                    
                    # Get observations for this entity
                    obs_query = f"""
                    SELECT ?content
                    WHERE {{
                        <{entity_uri}> <http://example.org/predicate/observation> ?obs .
                        ?obs <http://example.org/predicate/content> ?content .
                    }}
                    """
                    
                    observations = [
                        str(binding["content"].value)
                        for binding in self._store.query(obs_query)
                    ]
                    
                    matched_entities.append({
                        "name": entity_name,
                        "entityType": entity_type,
                        "observations": observations
                    })
            
            # Find relations between matched entities
            matched_relations = []
            
            if entity_map:
                entity_list = ", ".join(f"<{uri}>" for uri in entity_uris)
                
                relation_query = f"""
                PREFIX ex: <http://example.org/>
                
                SELECT ?from ?predicate ?to
                WHERE {{
                    ?from ?predicate ?to .
                    FILTER(STRSTARTS(STR(?predicate), "http://example.org/relation/"))
                    FILTER(?from IN ({entity_list}) && ?to IN ({entity_list}))
                }}
                """
                
                relation_results = self._store.query(relation_query)
                
                for binding in relation_results:
                    from_uri = str(binding["from"].value)
                    to_uri = str(binding["to"].value)
                    predicate_uri = str(binding["predicate"].value)
                    
                    # Extract relation type from URI
                    relation_type = predicate_uri.split("/")[-1]
                    
                    matched_relations.append({
                        "from": entity_map[from_uri],
                        "relationType": relation_type,
                        "to": entity_map[to_uri]
                    })
            
            return {
                "entities": matched_entities,
                "relations": matched_relations
            }
        except Exception as e:
            logger.error(f"Failed to open nodes: {e}")
            raise
    
    def close(self) -> None:
        """Close the Oxigraph store."""
        if self._store is not None:
            try:
                # Flush any pending changes to disk
                if hasattr(self._store, 'flush'):
                    self._store.flush()
                
                logger.info("Store closed successfully")
            except Exception as e:
                logger.error(f"Error closing store: {e}")
                raise

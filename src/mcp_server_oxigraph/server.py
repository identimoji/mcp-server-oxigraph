"""
PyOxigraph MCP Server.

This module provides an MCP server implementation that exposes PyOxigraph functionality.
"""

import os
import sys
import json
import logging
from mcp.server.fastmcp import FastMCP

# Import core functionality
from .core.store import (
    oxigraph_create_store,
    oxigraph_open_store,
    oxigraph_close_store,
    oxigraph_backup_store,
    oxigraph_restore_store,
    oxigraph_optimize_store,
    oxigraph_list_stores,
    oxigraph_get_store
)

from .core.rdf import (
    oxigraph_create_named_node,
    oxigraph_create_blank_node,
    oxigraph_create_literal,
    oxigraph_create_quad,
    oxigraph_add,
    oxigraph_add_many,
    oxigraph_remove,
    oxigraph_remove_many,
    oxigraph_clear,
    oxigraph_quads_for_pattern
)

from .core.sparql import (
    oxigraph_query,
    oxigraph_update,
    oxigraph_query_with_options,
    oxigraph_prepare_query,
    oxigraph_execute_prepared_query,
    oxigraph_run_query
)

from .core.format import (
    oxigraph_parse,
    oxigraph_serialize,
    oxigraph_import_file,
    oxigraph_export_graph,
    oxigraph_get_supported_formats
)

# Import knowledge graph functionality
from .core.knowledge_graph import (
    kg_create_entities,
    kg_create_relations,
    kg_read_graph,
    kg_search_nodes,
    kg_open_nodes,
    kg_add_observations,
    kg_delete_entities,
    kg_delete_relations,
    kg_delete_observations
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class OxigraphMCP:
    """MCP server for PyOxigraph functionality."""
    
    def __init__(self):
        """Initialize the MCP server."""
        from .core.store import normalize_path
        from .core.config import get_default_store_path, get_system_default_store_path, has_user_default_store
        
        # Log configuration information
        system_path = normalize_path(get_system_default_store_path())
        logger.info(f"System default store path: {system_path}")
        
        if has_user_default_store():
            user_path = normalize_path(get_default_store_path())
            logger.info(f"User default store path: {user_path}")
        else:
            logger.info("No user default store configured")
        
        # Note: Store initialization is now done explicitly in main()


def main():
    """Start the Oxigraph MCP server."""
    # Create MCP server
    mcp = FastMCP(name="oxigraph-mcp", version="0.1.0")
    
    # Initialize store manager to ensure stores are ready
    from .core.store import _store_manager
    from .core.config import get_default_store_path
    logger.info("Server starting: Explicitly initializing stores")
    
    # Initialize stores
    _store_manager.initialize_stores()
    
    # Log store status after initialization
    stores = _store_manager.list_stores()
    logger.info(f"Available stores after initialization: {stores}")
    
    # Verify the active default store is working
    if _store_manager.active_default_path:
        logger.info(f"Using active default store at '{_store_manager.active_default_path}'")
        
        # Test that we can actually use the store
        from .core.sparql import oxigraph_query
        try:
            result = oxigraph_query("ASK { ?s ?p ?o }", store_path=_store_manager.active_default_path)
            logger.info(f"Verified default store is accessible")
        except Exception as query_e:
            logger.error(f"Default store exists but query failed: {query_e}")
            logger.info("Trying to recreate store")
            
            # If the store doesn't work, try to create it
            from .core.store import oxigraph_create_store
            try:
                # Try user store path first
                user_path = get_default_store_path()
                if user_path:
                    logger.info(f"Creating user store at: {user_path}")
                    oxigraph_create_store(user_path)
                    logger.info("Successfully created user store")
                else:
                    # Fall back to system path
                    logger.info(f"Creating system store at: {_store_manager.system_store_path}")
                    oxigraph_create_store(_store_manager.system_store_path)
                    logger.info("Successfully created system store")
            except Exception as create_e:
                logger.error(f"Failed to create store: {create_e}")
    else:
        logger.error("No active default store available!")
    
    # Configure to never exit on stdin EOF
    import time
    import signal
    
    # Override sys.exit to prevent it from being called
    original_exit = sys.exit
    def exit_prevention(code=0):
        print(f"Exit prevented with code {code}", file=sys.stderr)
    sys.exit = exit_prevention
    
    # Set up signal handlers to prevent termination
    def handle_signal(sig, frame):
        print(f"Signal {sig} ignored", file=sys.stderr)
    for sig in [signal.SIGINT, signal.SIGTERM, signal.SIGHUP, signal.SIGQUIT]:
        try:
            signal.signal(sig, handle_signal)
        except Exception:
            pass  # Some signals might not be available on all platforms
    
    # Force unbuffered mode for all IO
    os.environ['PYTHONUNBUFFERED'] = '1'
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', buffering=1)
    sys.stderr = os.fdopen(sys.stderr.fileno(), 'w', buffering=1)
    
    # Register core store management functions
    mcp.tool()(oxigraph_create_store)
    mcp.tool()(oxigraph_open_store)
    mcp.tool()(oxigraph_close_store)
    mcp.tool()(oxigraph_backup_store)
    mcp.tool()(oxigraph_restore_store)
    mcp.tool()(oxigraph_optimize_store)
    mcp.tool()(oxigraph_list_stores)
    
    # Register core RDF functions
    mcp.tool()(oxigraph_create_named_node)
    mcp.tool()(oxigraph_create_blank_node)
    mcp.tool()(oxigraph_create_literal)
    mcp.tool()(oxigraph_create_quad)
    mcp.tool()(oxigraph_add)
    mcp.tool()(oxigraph_add_many)
    mcp.tool()(oxigraph_remove)
    mcp.tool()(oxigraph_remove_many)
    mcp.tool()(oxigraph_clear)
    mcp.tool()(oxigraph_quads_for_pattern)
    
    # Register SPARQL functions
    mcp.tool()(oxigraph_query)
    mcp.tool()(oxigraph_update)
    mcp.tool()(oxigraph_query_with_options)
    mcp.tool()(oxigraph_prepare_query)
    mcp.tool()(oxigraph_execute_prepared_query)
    mcp.tool()(oxigraph_run_query)
    
    # Register serialization functions
    mcp.tool()(oxigraph_parse)
    mcp.tool()(oxigraph_serialize)
    mcp.tool()(oxigraph_import_file)
    mcp.tool()(oxigraph_export_graph)
    mcp.tool()(oxigraph_get_supported_formats)
    
    # Register knowledge graph functions
    mcp.tool()(kg_create_entities)
    mcp.tool()(kg_create_relations)
    mcp.tool()(kg_read_graph)
    mcp.tool()(kg_search_nodes)
    mcp.tool()(kg_open_nodes)
    mcp.tool()(kg_add_observations)
    mcp.tool()(kg_delete_entities)
    mcp.tool()(kg_delete_relations)
    mcp.tool()(kg_delete_observations)
    
    # Start the server
    logger.info("Oxigraph MCP server starting...")
    mcp.run()


if __name__ == "__main__":
    main()

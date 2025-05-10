"""
PyOxigraph MCP Server.

This module provides an MCP server implementation that exposes PyOxigraph functionality.
Designed for stateless operation where each function call is independent.
"""

import os
import sys
import json
import logging
from mcp.server.fastmcp import FastMCP

# Import utilities
from .utils import setup_resilient_process

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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Start the Oxigraph MCP server."""
    # Create MCP server
    mcp = FastMCP(name="oxigraph", version="0.1.0")
    
    # Log server initialization
    logger.info("Oxigraph MCP server initializing")
    
    # Initialize a default store if needed
    try:
        # Try to create a default store
        from .core.config import get_default_store_path, get_system_default_store_path
        
        # Try user store path first
        user_path = get_default_store_path()
        if user_path:
            try:
                logger.info(f"Creating/opening user default store at: {user_path}")
                result = oxigraph_create_store(user_path)
                logger.info(f"Default store: {result.get('store', user_path)}")
            except Exception as e:
                logger.error(f"Failed to create user default store: {e}")
        
        # Then try system path
        system_path = get_system_default_store_path()
        try:
            logger.info(f"Creating/opening system default store at: {system_path}")
            result = oxigraph_create_store(system_path)
            logger.info(f"System default store: {result.get('store', system_path)}")
        except Exception as e:
            logger.error(f"Failed to create system default store: {e}")
    except Exception as e:
        logger.error(f"Error initializing default stores: {e}")
        logger.info("Continuing despite store initialization errors")
    
    # Configure to never exit on stdin EOF and handle signals
    original_exit = setup_resilient_process()
    
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
    
    # Start the server
    logger.info("Oxigraph MCP server starting...")
    mcp.run()


if __name__ == "__main__":
    main()

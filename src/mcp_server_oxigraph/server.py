"""
PyOxigraph MCP Server.

This module provides an MCP server implementation that exposes PyOxigraph functionality.
"""

import os
import sys
import json
import logging
from fastmcp import FastMCP

# Import core functionality
from .core.store import (
    oxigraph_create_store,
    oxigraph_open_store,
    oxigraph_open_read_only,
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
    create_entities,
    create_relations,
    read_graph,
    search_nodes,
    open_nodes,
    add_observations,
    delete_entities,
    delete_relations,
    delete_observations
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
        # Initialize a default store
        try:
            oxigraph_create_store("default")
            logger.info("Initialized default in-memory pyoxigraph store")
        except ValueError:
            # Store already exists
            logger.info("Using existing default pyoxigraph store")


def main():
    """Start the Oxigraph MCP server."""
    # Create MCP server
    mcp = FastMCP("Oxigraph MCP", name="oxigraph-mcp", version="0.1.0")
    
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
    mcp.tool()(oxigraph_open_read_only)
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
    mcp.tool()(create_entities)
    mcp.tool()(create_relations)
    mcp.tool()(read_graph)
    mcp.tool()(search_nodes)
    mcp.tool()(open_nodes)
    mcp.tool()(add_observations)
    mcp.tool()(delete_entities)
    mcp.tool()(delete_relations)
    mcp.tool()(delete_observations)
    
    # Start the server
    logger.info("Oxigraph MCP server starting...")
    mcp.start()


if __name__ == "__main__":
    main()

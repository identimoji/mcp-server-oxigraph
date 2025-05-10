"""
Oxigraph MCP server.
Direct wrapper for PyOxigraph functionality exposed via MCP.
"""

__version__ = "0.1.0"

# Import and re-export core functionality
from mcp_server_oxigraph.core.store import (
    oxigraph_create_store,
    oxigraph_open_store,
    oxigraph_close_store,
    oxigraph_backup_store,
    oxigraph_restore_store,
    oxigraph_optimize_store,
    oxigraph_list_stores
)

from mcp_server_oxigraph.core.rdf import (
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

from mcp_server_oxigraph.core.sparql import (
    oxigraph_query,
    oxigraph_update,
    oxigraph_query_with_options,
    oxigraph_prepare_query,
    oxigraph_execute_prepared_query,
    oxigraph_run_query
)

from mcp_server_oxigraph.core.format import (
    oxigraph_parse,
    oxigraph_serialize,
    oxigraph_import_file,
    oxigraph_export_graph,
    oxigraph_get_supported_formats
)

# Re-export server functions
from mcp_server_oxigraph.server import main

__all__ = [
    # Core store functionality
    "oxigraph_create_store",
    "oxigraph_open_store",
    "oxigraph_close_store",
    "oxigraph_backup_store",
    "oxigraph_restore_store",
    "oxigraph_optimize_store",
    "oxigraph_list_stores",
    
    # Core RDF functionality
    "oxigraph_create_named_node",
    "oxigraph_create_blank_node",
    "oxigraph_create_literal",
    "oxigraph_create_quad",
    "oxigraph_add",
    "oxigraph_add_many",
    "oxigraph_remove",
    "oxigraph_remove_many",
    "oxigraph_clear",
    "oxigraph_quads_for_pattern",
    
    # SPARQL functionality
    "oxigraph_query",
    "oxigraph_update",
    "oxigraph_query_with_options",
    "oxigraph_prepare_query",
    "oxigraph_execute_prepared_query",
    "oxigraph_run_query",
    
    # Format functionality
    "oxigraph_parse",
    "oxigraph_serialize",
    "oxigraph_import_file",
    "oxigraph_export_graph",
    "oxigraph_get_supported_formats",
    
    # Server function
    "main"
]

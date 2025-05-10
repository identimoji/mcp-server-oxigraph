# Oxigraph MCP Server

A Model Context Protocol (MCP) server for [PyOxigraph](https://github.com/oxigraph/oxigraph/tree/main/python), providing direct access to PyOxigraph functionality through the MCP protocol.

## Overview

This package provides a clean, direct wrapper for PyOxigraph that exposes all its functionality through the Model Context Protocol (MCP). It follows a consistent naming convention and organization, making it easy to use PyOxigraph's RDF and SPARQL capabilities from any MCP client.

## Features

- **Store Management**: Create, open, close, and manage RDF stores
- **RDF Data Model**: Create and manipulate RDF nodes, literals, and quads
- **SPARQL Operations**: Execute SPARQL queries and updates
- **Serialization**: Parse and serialize RDF data in various formats
- **Knowledge Graph API**: Higher-level knowledge graph operations (for demonstration purposes)

## Installation

```bash
pip install mcp-server-oxigraph
```

Or with UV:

```bash
uv pip install mcp-server-oxigraph
```

## Usage

### Configure Claude Desktop to use Oxigraph MCP

Add the following to your Claude Desktop MCP configuration:

```json
"oxigraph_mcp": {
  "command": "uv",
  "args": ["run", "oxigraph-mcp"],
  "env": {
    "PYTHONUNBUFFERED": "1",
    "OXIGRAPH_DEFAULT_STORE": "~/.mcp-server-oxigraph/default.oxigraph"
  }
}
```

### Default Store Configuration

The server manages two types of default stores:

1. **System Default Store**: Located at `~/.mcp-server-oxigraph/default.oxigraph`
2. **User Default Store**: Specified by setting the `OXIGRAPH_DEFAULT_STORE` environment variable

On startup, the server initializes both stores (if configured):
- The system default store is always created
- If a user default store is specified, it's also initialized
- Operations will use the user default store if specified, otherwise the system default

You don't need to explicitly create or open these stores - they're automatically initialized when the server starts. All operations that don't specify a store ID will use the appropriate default store.

### Basic Usage Examples

Once configured, you can use the Oxigraph MCP tools in Claude Desktop to work with RDF data:

1. Add RDF triples to the default store
   ```
   oxigraph_create_named_node(iri: "http://example.org/subject")
   oxigraph_create_named_node(iri: "http://example.org/predicate")
   oxigraph_create_literal(value: "Object value")
   oxigraph_create_quad(subject: subject, predicate: predicate, object: object)
   oxigraph_add(quad: quad)
   ```

2. Query with SPARQL
   ```
   oxigraph_query(query: "SELECT * WHERE { ?s ?p ?o } LIMIT 10")
   ```

3. Create custom stores if needed
   ```
   oxigraph_create_store(store_id: "/path/to/my/custom.oxigraph", path: "/path/to/my/custom.oxigraph")
   oxigraph_query(query: "SELECT * WHERE { ?s ?p ?o }", store_id: "/path/to/my/custom.oxigraph")
   ```

4. Manage knowledge graphs with higher-level functions
   ```
   kg_create_entities(entities: [{"name": "Alice", "entityType": "Person", "observations": ["CEO of Example Corp"]}])
   kg_search_nodes(query: "Alice")
   ```

## API Documentation

### Store Management Functions

- `oxigraph_create_store`: Create a new store (in-memory or file-based)
- `oxigraph_open_store`: Open an existing file-based store
- `oxigraph_close_store`: Close a store and remove it from the manager
- `oxigraph_list_stores`: List all managed stores

Note: Most operations will work with the default store without needing to specify a store_id. For persistent storage, we recommend using the file path as the store_id for clarity.

### RDF Data Model Functions

- `oxigraph_create_named_node`: Create a NamedNode for use in RDF statements
- `oxigraph_create_blank_node`: Create a BlankNode for use in RDF statements
- `oxigraph_create_literal`: Create a Literal for use in RDF statements
- `oxigraph_create_quad`: Create a Quad (triple with optional graph)
- `oxigraph_add`: Add a quad to the store
- `oxigraph_quads_for_pattern`: Query for quads matching a pattern

### SPARQL Functions

- `oxigraph_query`: Execute a SPARQL query against the store
- `oxigraph_update`: Execute a SPARQL update against the store
- `oxigraph_run_query`: Run a query against Oxigraph

### Knowledge Graph API (for demonstration)

- `create_entities`: Create multiple new entities in the knowledge graph
- `create_relations`: Create multiple new relations between entities
- `read_graph`: Read the entire knowledge graph
- `search_nodes`: Search for nodes in the knowledge graph based on a query

## License

MIT License

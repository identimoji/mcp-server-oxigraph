# Oxigraph MCP Server
A Model Context Protocol (MCP) server for [PyOxigraph](https://github.com/oxigraph/oxigraph/tree/main/python), providing direct access to PyOxigraph functionality through the MCP protocol.

## Overview

This package provides a clean, direct wrapper for PyOxigraph that exposes all its functionality through the Model Context Protocol (MCP). It follows a consistent naming convention and organization, making it easy to use PyOxigraph's RDF and SPARQL capabilities from any MCP client.

## Features

- **Store Management**: Create, open, close, backup, restore, and manage RDF stores
- **RDF Data Model**: Create and manipulate RDF nodes, literals, and quads
- **SPARQL Operations**: Execute SPARQL queries and updates with full result binding support
- **Serialization**: Parse and serialize RDF data in various formats (Turtle, N-Triples, N-Quads, TriG, RDF/XML, N3)

## Supported RDF Formats

The Oxigraph MCP server supports the following RDF serialization formats:

| Format | File Extension | MIME Type | Supports Named Graphs |
|--------|---------------|-----------|----------------------|
| Turtle | .ttl | text/turtle | No |
| N-Triples | .nt | application/n-triples | No |
| N-Quads | .nq | application/n-quads | Yes |
| TriG | .trig | application/trig | Yes |
| RDF/XML | .rdf | application/rdf+xml | No |
| N3 | .n3 | text/n3 | No |

You can retrieve this information programmatically using the `oxigraph_get_supported_formats()` function.

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
"oxigraph": {
  "command": "uv",
  "args": ["run", "mcp-server-oxigraph"],
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

You don't need to explicitly create or open these stores - they're automatically initialized when the server starts. All operations that don't specify a store path will use the appropriate default store.

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

3. Import and export RDF data in different formats
   ```
   oxigraph_parse(data: "@prefix ex: <http://example.org/> . ex:a ex:b ex:c .", format: "turtle")
   oxigraph_serialize(format: "ntriples")
   oxigraph_get_supported_formats()
   ```

4. Create custom stores if needed
   ```
   oxigraph_create_store(store_path: "/path/to/my/custom.oxigraph")
   oxigraph_query(query: "SELECT * WHERE { ?s ?p ?o }", store_path: "/path/to/my/custom.oxigraph")
   ```

## API Documentation

### Store Management Functions

- `oxigraph_create_store`: Create a new store (in-memory or file-based)
- `oxigraph_open_store`: Open an existing file-based store
- `oxigraph_close_store`: Close a store and remove it from the manager
- `oxigraph_backup_store`: Create a backup of a store
- `oxigraph_restore_store`: Restore a store from a backup
- `oxigraph_optimize_store`: Optimize a store for better performance
- `oxigraph_list_stores`: List all managed stores

Note: Most operations will work with the default store without needing to specify a store_path. For persistent storage, we recommend using the file path as the store_path for clarity.

### RDF Data Model Functions

- `oxigraph_create_named_node`: Create a NamedNode for use in RDF statements
- `oxigraph_create_blank_node`: Create a BlankNode for use in RDF statements
- `oxigraph_create_literal`: Create a Literal for use in RDF statements
- `oxigraph_create_quad`: Create a Quad (triple with optional graph)
- `oxigraph_add`: Add a quad to the store
- `oxigraph_add_many`: Add multiple quads to the store
- `oxigraph_remove`: Remove a quad from the store
- `oxigraph_remove_many`: Remove multiple quads from the store
- `oxigraph_clear`: Remove all quads from the store
- `oxigraph_quads_for_pattern`: Query for quads matching a pattern

### SPARQL Functions

- `oxigraph_query`: Execute a SPARQL query against the store
- `oxigraph_update`: Execute a SPARQL update against the store
- `oxigraph_query_with_options`: Execute a SPARQL query with custom options
- `oxigraph_run_query`: Run a SPARQL query or update against the store

### Serialization Functions

- `oxigraph_parse`: Parse RDF data and add to the store
- `oxigraph_serialize`: Serialize the store to a string
- `oxigraph_import_file`: Import RDF data from a file
- `oxigraph_export_graph`: Export a graph to a file
- `oxigraph_get_supported_formats`: Get a list of supported RDF formats

## License

MIT License

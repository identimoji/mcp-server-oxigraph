# Configuration Guide for Oxigraph MCP Server

This document provides detailed information about configuring the Oxigraph MCP server.

## Configuration Options

### Environment Variables

The server can be configured using the following environment variables:

| Variable | Description | Default Value |
|----------|-------------|---------------|
| `OXIGRAPH_DEFAULT_STORE` | Path to the default persistent store | `~/.mcp-server-oxigraph/default.oxigraph` |
| `PYTHONUNBUFFERED` | Ensures Python output is unbuffered (recommended to set to "1") | N/A |

### Default Stores

The server manages two types of default stores:

1. **System Default Store**: Always created at `~/.mcp-server-oxigraph/default.oxigraph`
2. **User Default Store**: Optional, specified via the `OXIGRAPH_DEFAULT_STORE` environment variable

This dual approach provides several benefits:

1. **Always-available system store**: Ensures a working store even if user configuration fails
2. **User customization**: Allows changing the location without modifying code
3. **Persistence across sessions**: Data is automatically saved between sessions
4. **Simplified API usage**: No need to pass a store_id for most operations

At startup, the server:
1. Always initializes the system default store
2. If a user default is specified, initializes that too
3. Prefers the user default for operations when available
4. Falls back to the system default if the user store isn't available

### Store Identifiers

The updated server uses the file path as the store identifier for persistent stores. This approach:

1. **Improves clarity**: The ID directly indicates where the data is stored
2. **Prevents collisions**: Each store has a unique path and therefore a unique ID
3. **Simplifies management**: You can easily understand where your data is being stored

## Configuration Examples

### Basic MCP Configuration (Desktop)

```json
"oxigraph_mcp": {
  "command": "uv",
  "args": ["run", "oxigraph-mcp"],
  "env": {
    "PYTHONUNBUFFERED": "1"
  }
}
```

### Custom Default Store Location

```json
"oxigraph_mcp": {
  "command": "uv",
  "args": ["run", "oxigraph-mcp"],
  "env": {
    "PYTHONUNBUFFERED": "1",
    "OXIGRAPH_DEFAULT_STORE": "~/my-data/custom-oxigraph-store.oxigraph"
  }
}
```

## Error Handling

The server includes robust error handling for store operations:

1. If the default store location cannot be created (e.g., due to permissions), a clear error message is provided
2. If the default store cannot be opened, the server will attempt to create a new one
3. If all persistent store operations fail, the server will fall back to an in-memory store

## Performance Considerations

For optimal performance with persistent stores:

1. **Store Size**: Large stores (>100MB) may benefit from periodic optimization using `oxigraph_optimize_store`
2. **Query Complexity**: Complex SPARQL queries on large stores might be slower than in-memory operations
3. **Backup Strategy**: For important data, consider using `oxigraph_backup_store` periodically

## Security Notes

1. The default store is created in the user's home directory, which is generally accessible only to the user
2. No sensitive authentication information is stored by default
3. Consider the permission settings of custom store locations if using alternative paths

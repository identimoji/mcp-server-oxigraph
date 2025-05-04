# Oxigraph MCP Server - Configuration

## Claude Desktop Configuration
```json
"oxigraph_mcp": {
  "command": "sh",
  "args": ["-c", "pip install fastmcp>=2.0.0 && uvx @identimoji/mcp-server-oxigraph"],
  "env": {
    "OXIGRAPH_DB_PATH": "/path/to/database",  // OPTIONAL
    "PYTHONUNBUFFERED": "1",
    "PYTHONPATH": "/Users/rob/repos/mcp-server-oxigraph"
  }
}
```

## Alternative Configuration
```json
"oxigraph_mcp": {
  "command": "python",
  "args": ["-m", "oxigraph_server"],
  "env": {
    "OXIGRAPH_DB_PATH": "/path/to/database",  // OPTIONAL
    "PYTHONUNBUFFERED": "1",
    "PYTHONPATH": "/Users/rob/repos/mcp-server-oxigraph"
  }
}
```

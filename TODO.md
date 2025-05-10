# PyOxigraph MCP Server Implementation TODO

## Core Functionality

### 1. Store Management Functions
- [x] Initialize in-memory store
- [ ] `create_store` - Create a new store (in-memory or file-based)
- [ ] `open_store` - Open an existing store
- [ ] `open_read_only` - Open a store in read-only mode
- [ ] `backup_store` - Create a backup
- [ ] `restore_store` - Restore from backup
- [ ] `optimize_store` - Optimize the store for performance
- [ ] `close_store` - Properly close a store

### 2. Raw RDF Operations
- [ ] `add_quad` - Add a single quad
- [ ] `remove_quad` - Remove a specific quad
- [ ] `bulk_load` - Load multiple quads efficiently
- [ ] `bulk_extend` - Extend with multiple quads

### 3. Graph Management
- [ ] `add_graph` - Create a new named graph
- [ ] `remove_graph` - Remove a named graph
- [ ] `clear_graph` - Clear all triples in a graph
- [ ] `list_named_graphs` - List all named graphs

### 4. SPARQL Operations
- [ ] `sparql_query` - Enhanced query with more parameters
- [ ] `sparql_update` - Execute SPARQL 1.1 Update operations
- [ ] `sparql_with_options` - Query with custom options

### 5. Format Conversion
- [ ] `serialize_graph` - Serialize graph to different formats
- [ ] `parse_data` - Parse RDF data from different formats

### 6. Pattern Matching
- [ ] `quads_for_pattern` - Find quads matching a pattern

## Infrastructure
- [x] Fix MCP server connection issue (implemented EOF handling, signal catching, and unbuffered IO)
- [ ] Add proper error handling with detailed messages
- [ ] Add comprehensive logging
- [ ] Implement proper test suite

## Documentation
- [ ] API Documentation
- [ ] Usage Examples
- [ ] Installation Instructions

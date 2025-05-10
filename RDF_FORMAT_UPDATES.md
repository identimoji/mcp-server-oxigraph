# RDF Format Updates

## Overview

This update enhances the `format.py` module to use PyOxigraph's native `RdfFormat` enum instead of string-based format handling. This provides several benefits:

1. Better integration with PyOxigraph's native capabilities
2. Proper format detection from file extensions
3. Format validation for supported operations (e.g., checking if a format supports datasets)
4. Consistent handling across all serialization/deserialization operations

## Key Changes

### 1. Added `RdfFormat` Enum Usage

The code now uses PyOxigraph's enum directly:

```python
from pyoxigraph import RdfFormat

# Convert string format to RdfFormat enum
rdf_format = _get_rdf_format(format)

# Parse with the enum
pyoxigraph.parse(input=data, format=rdf_format, base_iri=base_iri)
```

### 2. Added Helper Function for Format Detection

Added a new private helper function that handles format detection from both strings and file extensions:

```python
def _get_rdf_format(format_str: Optional[str] = None, file_path: Optional[str] = None) -> RdfFormat:
    """Convert a format string to a RdfFormat enum value or detect from file extension."""
    # Implementation...
```

### 3. Enhanced Format Support Detection

The code now checks if a format supports datasets (quads with graph names) and handles the conversion when needed:

```python
# Check if format supports datasets (quads with graph names)
has_graph_names = any(quad.graph_name is not None for quad in quads)

if has_graph_names and not rdf_format.supports_datasets:
    # Convert quads to triples for formats that don't support datasets
    triples = [pyoxigraph.Triple(q.subject, q.predicate, q.object) for q in quads]
    serialized_bytes = pyoxigraph.serialize(triples, format=rdf_format, prefixes=prefixes)
else:
    # Formats that support datasets
    serialized_bytes = pyoxigraph.serialize(quads, format=rdf_format, prefixes=prefixes)
```

### 4. Updated Format Enumeration

The `oxigraph_get_supported_formats()` function now includes information about which formats support datasets:

```python
{
    "id": "turtle", 
    "name": "Turtle", 
    "extension": ".ttl", 
    "mime_type": "text/turtle",
    "supports_datasets": RdfFormat.TURTLE.supports_datasets
}
```

## Testing

A new test file has been added at `tests/test_rdf_format.py` to validate the RdfFormat functionality. The tests cover:

1. Format string and file extension detection
2. Parsing and serialization with different formats
3. Format enumeration and metadata

## Usage Example

```python
from mcp_server_oxigraph import oxigraph_parse, oxigraph_serialize

# Parse Turtle data
parse_result = oxigraph_parse(turtle_data, 'turtle')

# Serialize to N-Triples
nt_result = oxigraph_serialize('ntriples')

# Get supported formats
formats = oxigraph_get_supported_formats()
```

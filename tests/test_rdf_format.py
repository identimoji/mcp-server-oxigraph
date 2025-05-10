"""
Test the RDF format functionality of the Oxigraph MCP server.
"""

import os
import sys
import unittest
import tempfile
from pyoxigraph import RdfFormat
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from mcp_server_oxigraph.core.format import (
    _get_rdf_format,
    oxigraph_parse,
    oxigraph_serialize,
    oxigraph_get_supported_formats
)

class TestRdfFormat(unittest.TestCase):
    """Test the RDF format functionality."""

    def test_get_rdf_format(self):
        """Test the _get_rdf_format function."""
        # Test format string detection
        self.assertEqual(_get_rdf_format('turtle'), RdfFormat.TURTLE)
        self.assertEqual(_get_rdf_format('ttl'), RdfFormat.TURTLE)
        self.assertEqual(_get_rdf_format('ntriples'), RdfFormat.N_TRIPLES)
        self.assertEqual(_get_rdf_format('nt'), RdfFormat.N_TRIPLES)
        self.assertEqual(_get_rdf_format('nquads'), RdfFormat.N_QUADS)
        self.assertEqual(_get_rdf_format('nq'), RdfFormat.N_QUADS)
        self.assertEqual(_get_rdf_format('trig'), RdfFormat.TRIG)
        self.assertEqual(_get_rdf_format('rdfxml'), RdfFormat.RDF_XML)
        self.assertEqual(_get_rdf_format('rdf/xml'), RdfFormat.RDF_XML)
        self.assertEqual(_get_rdf_format('rdf'), RdfFormat.RDF_XML)
        self.assertEqual(_get_rdf_format('xml'), RdfFormat.RDF_XML)
        self.assertEqual(_get_rdf_format('n3'), RdfFormat.N3)
        
        # Test file extension detection
        self.assertEqual(_get_rdf_format(file_path='test.ttl'), RdfFormat.TURTLE)
        self.assertEqual(_get_rdf_format(file_path='test.nt'), RdfFormat.N_TRIPLES)
        self.assertEqual(_get_rdf_format(file_path='test.nq'), RdfFormat.N_QUADS)
        self.assertEqual(_get_rdf_format(file_path='test.trig'), RdfFormat.TRIG)
        self.assertEqual(_get_rdf_format(file_path='test.rdf'), RdfFormat.RDF_XML)
        self.assertEqual(_get_rdf_format(file_path='test.n3'), RdfFormat.N3)
        
        # Test default fallback
        self.assertEqual(_get_rdf_format('unknown'), RdfFormat.TURTLE)
        self.assertEqual(_get_rdf_format(file_path='test.unknown'), RdfFormat.TURTLE)
        self.assertEqual(_get_rdf_format(), RdfFormat.TURTLE)

    def test_parse_and_serialize(self):
        """Test the parse and serialize functions."""
        # Simple Turtle data
        turtle_data = """
        @prefix ex: <http://example.org/> .
        
        ex:subject ex:predicate "object" .
        """
        
        # Create a temporary store
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = os.path.join(tmpdir, 'store')
            
            # Parse the data
            parse_result = oxigraph_parse(turtle_data, 'turtle', store_path=store_path)
            self.assertTrue(parse_result['success'])
            self.assertEqual(parse_result['count'], 1)
            
            # Serialize the data
            serialize_result = oxigraph_serialize('turtle', store_path=store_path)
            self.assertIn('data', serialize_result)
            self.assertEqual(serialize_result['count'], 1)
            
            # Check N-Triples serialization
            nt_result = oxigraph_serialize('ntriples', store_path=store_path)
            self.assertIn('data', nt_result)
            self.assertIn('<http://example.org/subject>', nt_result['data'])
            self.assertIn('<http://example.org/predicate>', nt_result['data'])
            self.assertIn('"object"', nt_result['data'])
            
            # Try formats that support datasets (with multiple graphs)
            nq_result = oxigraph_serialize('nquads', store_path=store_path)
            self.assertIn('data', nq_result)

    def test_supported_formats(self):
        """Test the get_supported_formats function."""
        formats = oxigraph_get_supported_formats()
        self.assertIn('formats', formats)
        self.assertGreaterEqual(len(formats['formats']), 6)  # At least 6 formats
        
        # Check if the format data includes the expected keys
        for fmt in formats['formats']:
            self.assertIn('id', fmt)
            self.assertIn('name', fmt)
            self.assertIn('extension', fmt)
            self.assertIn('mime_type', fmt)
            self.assertIn('supports_datasets', fmt)
        
        # Verify Turtle and N-Quads are present
        format_ids = [fmt['id'] for fmt in formats['formats']]
        self.assertIn('turtle', format_ids)
        self.assertIn('nquads', format_ids)

if __name__ == '__main__':
    unittest.main()

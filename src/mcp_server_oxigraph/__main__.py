"""
Main entry point for the Oxigraph MCP server.
"""

import logging
import sys
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Add some diagnostics to help debug startup issues
logger.info("Starting Oxigraph MCP server")
logger.info(f"Python version: {sys.version}")
logger.info(f"Current directory: {os.getcwd()}")
logger.info(f"Module path: {__file__}")

try:
    # Import server main function
    from mcp_server_oxigraph.server import main
    
    # Run the server
    if __name__ == "__main__":
        logger.info("Calling main()")
        main()
except Exception as e:
    logger.error(f"Error starting server: {e}", exc_info=True)
    sys.exit(1)

"""
Utility functions for the Oxigraph MCP server.

This module provides utility functions for the Oxigraph MCP server.
"""

import os
import sys
import signal
import logging

logger = logging.getLogger(__name__)

def setup_resilient_process():
    """
    Set up the process with development-friendly signal handling.
    
    This function:
    1. Allows normal termination via Ctrl+C (SIGINT)
    2. Sets up graceful shutdown handlers
    3. Ensures unbuffered I/O
    
    Returns:
        The original sys.exit function in case it needs to be restored
    """
    # Store original exit function
    original_exit = sys.exit
    
    # Set up graceful signal handlers
    def handle_sigint(sig, frame):
        logger.info("Received SIGINT (Ctrl+C), shutting down gracefully...")
        original_exit(0)
    
    def handle_sigterm(sig, frame):
        logger.info("Received SIGTERM, shutting down gracefully...")
        original_exit(0)
    
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, handle_sigint)
    signal.signal(signal.SIGTERM, handle_sigterm)
    
    # Force unbuffered mode for all IO
    os.environ['PYTHONUNBUFFERED'] = '1'
    try:
        sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', buffering=1)
        sys.stderr = os.fdopen(sys.stderr.fileno(), 'w', buffering=1)
    except Exception as e:
        logger.warning(f"Could not set unbuffered I/O: {e}")
    
    return original_exit

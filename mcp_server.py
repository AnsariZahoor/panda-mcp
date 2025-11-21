#!/usr/bin/env python3
"""
FastMCP Cloud-compatible entrypoint for Panda MCP server
This file uses absolute imports and is suitable for FastMCP Cloud deployment
"""

import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Import the FastMCP instance (mcp) not the Starlette app
from src.app import mcp

# Export for fastmcp inspect and fastmcp run
__all__ = ['mcp']

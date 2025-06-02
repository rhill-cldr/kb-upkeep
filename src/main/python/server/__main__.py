import os
import contextlib
import uvicorn
from starlette.applications import Starlette
from starlette.routing import Mount, Host
from fastapi import FastAPI
from app import main
import service


# Mount the SSE server to the existing ASGI server
app = Starlette(
    routes=[
        Mount('/', app=main.mcp.sse_app()),
    ]
)

if __name__ == "__main__":
    #main.mcp.run(transport="sse", mount_path="/")
    ip_addr = os.environ.get("IP_ADDRESS", "0.0.0.0")
    mcp_port = os.environ.get("MCP_PORT", 8000)
    uvicorn.run(app,host=ip_addr, port=mcp_port)


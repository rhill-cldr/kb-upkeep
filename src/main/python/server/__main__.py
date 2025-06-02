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
    uvicorn.run(app,host='192.168.1.55',port=8000)


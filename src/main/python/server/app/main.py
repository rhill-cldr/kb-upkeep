import logging
from typing import Optional
# Add lifespan support for startup/shutdown with strong typing
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from mcp.server.fastmcp import FastMCP


_APP_PKG_NAME = __name__.split(".", maxsplit=1)[0]

logger = logging.getLogger(__name__)
_request_received_logger = logging.getLogger(_APP_PKG_NAME + ".access")


@dataclass
class AppContext:
    chk: Optional[bool] = field(default=False)


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    """Manage application lifecycle with type-safe context"""
    # Initialize on startup
    # db = await Database.connect()
    try:
        yield AppContext(chk=True)
    finally:
        # Cleanup on shutdown
        # await db.disconnect()
        pass


# Pass lifespan to server
mcp = FastMCP("KB Upkeep", lifespan=app_lifespan)


# Access type-safe lifespan context in tools
@mcp.tool()
def query_db() -> str:
    """Tool that uses initialized resources"""
    ctx = mcp.get_context()
    #db = ctx.request_context.lifespan_context["db"]
    return "ok"


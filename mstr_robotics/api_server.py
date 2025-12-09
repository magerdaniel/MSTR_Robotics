import httpx
import sys
import asyncio
from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
from mcp.server.fastmcp import FastMCP
from mstr_robotics import user_run_compare
from mstrio.connection import Connection

mcp = FastMCP("poke")


DEFAULT_REST_PORT = 8000

# Command-line argument indices
ARG_MODE_INDEX = 1
ARG_COMMAND_INDEX = 2
ARG_PARAMETER_INDEX = 3

# Minimum argument count to have a mode
MIN_ARGS_WITH_MODE = 2

# --- Tool: List popular Pokémon ---
@mcp.tool()
async def list_popular_pokemon(test_string: str) -> str:
    """List popular tournament-ready Pokémon."""
    return "\n".join([test_string,
        "Charizard", "Garchomp", "Lucario",
        "Dragonite", "Metagross", "Gardevoir","danTheMan"
    ])

# --- Entry point ---
if __name__ == "__main__":
    if len(sys.argv) >= MIN_ARGS_WITH_MODE and sys.argv[ARG_MODE_INDEX] == "--cli":
        # CLI mode
        command = sys.argv[ARG_COMMAND_INDEX] if len(sys.argv) > ARG_COMMAND_INDEX else None

        if command == "list_popular_pokemon":
            test_string = sys.argv[ARG_PARAMETER_INDEX] if len(sys.argv) > ARG_PARAMETER_INDEX else ""
            result = asyncio.run(list_popular_pokemon(test_string))
            print(result)

    elif len(sys.argv) >= MIN_ARGS_WITH_MODE and sys.argv[ARG_MODE_INDEX] == "--rest":
        # REST API mode with FastAPI

        app = FastAPI(title="MSTR_ROBOTICS_MCP")
        user_connections = {}
        class LoginRequest(BaseModel):
            session_id: str
            conn_params: dict

        @app.post("/login")
        async def login(request: LoginRequest):
            """Initialize connection with user credentials"""
            try:
                #print(request.conn_params)
                conn = Connection(**request.conn_params)
                conn.headers['Content-type'] = "application/json"
                print(conn.headers)
                user_connections[request.session_id] = {
                    "conn": conn,
                    "conn_params": request.conn_params,
                    "redis_config": None,
                    "selected_redis_env": None
                }
                return {"result": "Connection established", "session_id": request.session_id}
            except Exception as e:
                return {"error": str(e)}

        class RedisConnectionRequest(BaseModel):
            session_id: str
            redis_config: dict
            selected_env: str

        @app.post("/connect_redis")
        async def connect_redis(request: RedisConnectionRequest):
            """Initialize Redis connection with configuration"""
            try:
                if request.session_id not in user_connections:
                    return {"error": "Session not found. Please call /login first"}

                # Store Redis config and selected environment in session
                user_connections[request.session_id]["redis_config"] = request.redis_config
                user_connections[request.session_id]["selected_redis_env"] = request.selected_env

                return {
                    "result": "Redis configuration stored",
                    "session_id": request.session_id,
                    "selected_env": request.selected_env
                }
            except Exception as e:
                return {"error": str(e)}

        @app.post("/logout")
        async def logout(session_id: str):
            """Close connection and remove session"""
            if session_id in user_connections:
                # Close connection if it has a close method
                del user_connections[session_id]
                return {"result": "Session closed"}
            return {"error": "Session not found"}

        class comparison_run(BaseModel):
            session_id: str
            play_compare_d: dict

        @app.post("/run_comparison")
        async def run_comparison(request: comparison_run):
            if request.session_id not in user_connections:
                return {"error": "Not logged in. Please call /login first"}

            session_data = user_connections[request.session_id]
            conn = session_data["conn"]
            redis_config = session_data.get("redis_config")
            selected_redis_env = session_data.get("selected_redis_env")

            # Create run_compare instance with Redis config if available
            if redis_config and selected_redis_env:
                i_user_run_compare = user_run_compare.run_compare(
                    conn=conn,
                    redis_config=redis_config,
                    selected_redis_env=selected_redis_env
                )
            else:
                # Fallback to default behavior (loads from file)
                i_user_run_compare = user_run_compare.run_compare(conn)

            result = i_user_run_compare.run_comparison(request.play_compare_d)
            return {"result": result}

        port = int(sys.argv[ARG_COMMAND_INDEX]) if len(sys.argv) > ARG_COMMAND_INDEX else DEFAULT_REST_PORT
        print(f"Starting REST API server on http://0.0.0.0:{port}")
        uvicorn.run(app, host="0.0.0.0", port=port)
    else:
        # MCP mode (stdio)
        mcp.run(transport="stdio")
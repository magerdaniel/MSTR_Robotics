import httpx
import sys
import asyncio
from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("poke")

POKEAPI_BASE = "https://pokeapi.co/api/v2"
DEFAULT_REST_PORT = 8000

# Command-line argument indices
ARG_MODE_INDEX = 1
ARG_COMMAND_INDEX = 2
ARG_PARAMETER_INDEX = 3

# Minimum argument count to have a mode
MIN_ARGS_WITH_MODE = 2

# --- Helper to fetch Pokémon data ---
async def fetch_pokemon_data(name: str) -> dict:
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f"{POKEAPI_BASE}/pokemon/{name.lower()}")
            if response.status_code == 200:
                return response.json()
        except httpx.HTTPError:
            pass
    return {}

# --- Tool: Get info about a Pokémon ---
@mcp.tool()
async def get_pokemon_info(name: str) -> str:
    """Get detailed info about a Pokémon by name."""
    data = await fetch_pokemon_data(name)
    if not data:
        return f"No data found for Pokémon: {name}"

    stats = {stat['stat']['name']: stat['base_stat'] for stat in data['stats']}
    types_ = [t['type']['name'] for t in data['types']]
    abilities = [a['ability']['name'] for a in data['abilities']]

    return f"""
Name: {data['name'].capitalize()}
Types: {', '.join(types_)}
Abilities: {', '.join(abilities)}
Stats: {', '.join(f"{k}: {v}" for k, v in stats.items())}
"""

# --- Tool: Create a tournament squad ---
@mcp.tool()
async def create_tournament_squad() -> str:
    """Create a powerful squad of Pokémon for a tournament."""
    top_pokemon = ["charizard", "garchomp", "lucario", "dragonite", "metagross", "gardevoir"]
    squad = []

    for name in top_pokemon:
        data = await fetch_pokemon_data(name)
        if data:
            squad.append(data["name"].capitalize())

    return "Tournament Squad:\n" + "\n".join(squad)

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
        elif command == "get_pokemon_info":
            name = sys.argv[ARG_PARAMETER_INDEX] if len(sys.argv) > ARG_PARAMETER_INDEX else ""
            result = asyncio.run(get_pokemon_info(name))
            print(result)
        elif command == "create_tournament_squad":
            result = asyncio.run(create_tournament_squad())
            print(result)
    elif len(sys.argv) >= MIN_ARGS_WITH_MODE and sys.argv[ARG_MODE_INDEX] == "--rest":
        # REST API mode with FastAPI

        app = FastAPI(title="Pokemon MCP Server")

        class PokemonRequest(BaseModel):
            name: str

        class PopularPokemonRequest(BaseModel):
            test_string: str = ""

        @app.get("/")
        async def root():
            return {"message": "Pokemon MCP REST API", "status": "running"}

        @app.get("/tools/list")
        async def list_tools():
            return {
                "tools": [
                    {"name": "get_pokemon_info", "description": "Get detailed info about a Pokémon by name"},
                    {"name": "create_tournament_squad", "description": "Create a powerful squad of Pokémon for a tournament"},
                    {"name": "list_popular_pokemon", "description": "List popular tournament-ready Pokémon"}
                ]
            }

        @app.post("/get_pokemon_info")
        async def api_get_pokemon_info(request: PokemonRequest):
            result = await get_pokemon_info(request.name)
            return {"result": result}

        @app.post("/create_tournament_squad")
        async def api_create_tournament_squad():
            result = await create_tournament_squad()
            return {"result": result}

        @app.post("/list_popular_pokemon")
        async def api_list_popular_pokemon(request: PopularPokemonRequest):
            result = await list_popular_pokemon(request.test_string)
            return {"result": result}

        port = int(sys.argv[ARG_COMMAND_INDEX]) if len(sys.argv) > ARG_COMMAND_INDEX else DEFAULT_REST_PORT
        print(f"Starting REST API server on http://0.0.0.0:{port}")
        uvicorn.run(app, host="0.0.0.0", port=port)
    else:
        # MCP mode (stdio)
        mcp.run(transport="stdio")
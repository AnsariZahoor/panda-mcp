from fastmcp import Client
import asyncio

async def main():
    # The client will automatically handle Auth0 OAuth flows
    async with Client("http://localhost:8000/mcp", auth="oauth") as client:
        # First-time connection will open Auth0 login in your browser
        print("✓ Authenticated with Auth0!")

        # Test the protected tool
        result = await client.call_tool("get_token_info")
        print(f"Auth0 audience: {result}")

if __name__ == "__main__":
    asyncio.run(main())
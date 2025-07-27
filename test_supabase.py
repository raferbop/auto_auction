import httpx
import asyncio
import os
from dotenv import load_dotenv

async def test_supabase():
    # Test 1: Check environment variables
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_ANON_KEY")
    
    print("=== Environment Variables ===")
    print(f"URL: {url}")
    print(f"Key: {key[:20]}..." if key else "Key: None")
    
    if not url or not key:
        print("❌ Environment variables not loaded!")
        return
    print("✅ Environment variables loaded successfully")
    
    # Test 2: Test API connectivity
    print("\n=== API Connectivity ===")
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}"
    }
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(f"{url}/rest/v1/", headers=headers)
            print(f"✅ API Response: {response.status_code}")
            
            # Test vehicles table
            response2 = await client.get(f"{url}/rest/v1/vehicles?select=count&limit=1", headers=headers)
            print(f"✅ Vehicles table: {response2.status_code}")
            
    except Exception as e:
        print(f"❌ API Error: {e}")

asyncio.run(test_supabase())
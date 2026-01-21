"""
Test the FastAPI endpoints
"""

import requests
import json

BASE_URL = "http://localhost:8000"

def test_endpoints():
    """Test all API endpoints"""
    print("Testing Medical Telegram Warehouse API...")
    print("="*60)
    
    tests = [
        ("GET", "/status", "API Status"),
        ("GET", "/health/", "Health Check"),
        ("GET", "/health/tables", "Database Tables"),
        ("GET", "/reports/top-products?limit=5", "Top Products"),
        ("GET", "/reports/daily-trends?days=7", "Daily Trends"),
        ("GET", "/reports/visual-content", "Visual Content Stats"),
        ("GET", "/reports/channel-performance?min_posts=1", "Channel Performance"),
        ("GET", "/channels/", "List Channels"),
        ("GET", "/search/messages?query=medical&limit=5", "Search Messages"),
        ("GET", "/search/channels?min_posts=1", "Search Channels"),
    ]
    
    for method, endpoint, description in tests:
        try:
            url = f"{BASE_URL}{endpoint}"
            print(f"\n📡 Testing: {description}")
            print(f"   Endpoint: {endpoint}")
            
            if method == "GET":
                response = requests.get(url, timeout=10)
            else:
                response = requests.post(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                print(f"   ✅ Status: {response.status_code}")
                print(f"   📊 Response keys: {list(data.keys())}")
                
                # Show sample data for some endpoints
                if "products" in data and data["products"]:
                    print(f"   🏆 Top product: {data['products'][0]['product_name']}")
                elif "channels" in data and data["channels"]:
                    print(f"   📢 Top channel: {data['channels'][0]['channel_name']}")
                elif "messages" in data and data["messages"]:
                    print(f"   💬 Messages found: {len(data['messages'])}")
                elif "trends" in data and data["trends"]:
                    print(f"   📅 Days analyzed: {len(data['trends'])}")
                    
            else:
                print(f"   ❌ Status: {response.status_code}")
                print(f"   Error: {response.text[:100]}")
                
        except requests.exceptions.ConnectionError:
            print(f"   ❌ Cannot connect to API. Is it running?")
            print(f"   Run: uvicorn api.main:app --reload")
            break
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    print("\n" + "="*60)
    print("✅ API Testing Complete!")
    
    # Test a specific channel if available
    try:
        # First get list of channels
        response = requests.get(f"{BASE_URL}/channels/", timeout=10)
        if response.status_code == 200:
            channels = response.json()
            if channels:
                channel_name = channels[0]["channel_name"]
                print(f"\n📋 Testing channel-specific endpoints for: {channel_name}")
                
                # Test channel activity
                response = requests.get(
                    f"{BASE_URL}/channels/{channel_name}/activity?days=7",
                    timeout=10
                )
                if response.status_code == 200:
                    print(f"   ✅ Channel activity: {len(response.json()['recent_messages'])} recent messages")
                
                # Test channel stats
                response = requests.get(
                    f"{BASE_URL}/channels/{channel_name}/stats",
                    timeout=10
                )
                if response.status_code == 200:
                    print(f"   ✅ Channel stats: Retrieved successfully")
    except:
        pass  # Skip if channel test fails

if __name__ == "__main__":
    test_endpoints()
#!/usr/bin/env python3
"""
Test script to verify URL encoding fix works correctly
"""
import asyncio
import aiohttp
from yarl import URL

async def test_url_encoding():
    """Test that pipe characters are preserved in URLs"""
    
    # Test cases from the logs
    test_cases = [
        ("NSE_FO|77167", "2026-01-02"),
        ("NSE_FO|117528", "2026-01-06"),
        ("NSE_FO|117523", "2026-01-06"),
    ]
    
    print("=" * 80)
    print("URL Encoding Fix Verification")
    print("=" * 80)
    print()
    
    async with aiohttp.ClientSession() as session:
        for instrument_key, date in test_cases:
            url = f"https://api.upstox.com/v2/historical-candle/{instrument_key}/1minute/{date}/{date}"
            
            print(f"Testing: {instrument_key} on {date}")
            print(f"  Original URL: {url}")
            
            # Test WITHOUT fix (will fail)
            try:
                async with session.get(url) as response:
                    print(f"  Without fix - Status: {response.status} (Expected: 400)")
                    if response.status == 400:
                        print(f"    ❌ Failed as expected (| encoded to %7C)")
            except Exception as e:
                print(f"    ❌ Error: {e}")
            
            # Test WITH fix (should work)
            try:
                url_obj = URL(url, encoded=True)
                print(f"  Fixed URL object: {url_obj}")
                async with session.get(url_obj) as response:
                    print(f"  With fix - Status: {response.status}")
                    if response.status == 200:
                        data = await response.json()
                        candles = len(data.get('data', {}).get('candles', []))
                        print(f"    ✅ SUCCESS! Retrieved {candles} candles")
                    else:
                        print(f"    ⚠️  Unexpected status: {response.status}")
            except Exception as e:
                print(f"    ❌ Error: {e}")
            
            print()
    
    print("=" * 80)
    print("Verification Complete")
    print("=" * 80)

if __name__ == '__main__':
    asyncio.run(test_url_encoding())


#!/usr/bin/env python3
"""
Simple script to fetch SEC EDGAR data with proper rate limiting.
"""

import urllib.request
import time
from pathlib import Path

def fetch_sec_tickers():
    """Fetch all tickers from SEC with rate limiting."""
    
    # SEC endpoints
    SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
    
    print("Fetching SEC ticker data...")
    print("Using rate limit: 5 requests per second")
    
    # Add delay to respect rate limits
    time.sleep(0.2)
    
    # Create request with proper headers
    req = urllib.request.Request(SEC_TICKERS_URL, 
        headers={
            'User-Agent': 'Apache-Calcite-SEC-Adapter/1.0 (contact: admin@example.com)',
            'Accept': 'application/json',
            'Accept-Encoding': 'identity',
            'Host': 'www.sec.gov',
            'Connection': 'close'
        })
    
    try:
        print(f"Requesting: {SEC_TICKERS_URL}")
        with urllib.request.urlopen(req, timeout=30) as response:
            data = response.read().decode('utf-8')
            print(f"Received {len(data)} bytes")
            
            # Parse JSON manually
            import json
            parsed = json.loads(data)
            
            # Convert to ticker -> CIK mapping
            ticker_to_cik = {}
            for item in parsed.values():
                ticker = item.get('ticker', '').upper()
                cik = str(item.get('cik_str', '')).zfill(10)
                if ticker and cik:
                    ticker_to_cik[ticker] = cik
            
            print(f"Successfully fetched {len(ticker_to_cik)} ticker-to-CIK mappings")
            
            # Show sample
            print("\nSample mappings:")
            for ticker, cik in list(ticker_to_cik.items())[:10]:
                print(f"  {ticker}: {cik}")
            
            # Get unique CIKs
            all_ciks = list(set(ticker_to_cik.values()))
            print(f"\nTotal unique CIKs: {len(all_ciks)}")
            
            # Save to file
            output_dir = Path(__file__).parent.parent / "src/main/resources"
            output_file = output_dir / "all-edgar-registry.json"
            
            registry = {
                "_metadata": {
                    "description": "Complete list of all SEC EDGAR filers",
                    "last_updated": time.strftime("%Y-%m-%dT%H:%M:%S"),
                    "total_companies": len(all_ciks),
                    "total_tickers": len(ticker_to_cik)
                },
                "tickers": ticker_to_cik,
                "groups": {
                    "ALL_EDGAR_FILERS": {
                        "description": f"All {len(all_ciks)} companies with SEC filings",
                        "members": all_ciks
                    }
                }
            }
            
            with open(output_file, 'w') as f:
                json.dump(registry, f, indent=2)
            
            print(f"\nSaved to: {output_file}")
            print(f"File contains {len(all_ciks)} unique CIKs and {len(ticker_to_cik)} ticker mappings")
            
    except urllib.error.HTTPError as e:
        print(f"HTTP Error {e.code}: {e.reason}")
        print("The SEC may be rate limiting. Please wait a moment and try again.")
        print("\nTips:")
        print("1. Ensure User-Agent header includes contact email")
        print("2. Respect rate limits (10 requests per second max)")
        print("3. Try during off-peak hours")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    fetch_sec_tickers()
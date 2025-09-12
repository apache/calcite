#!/usr/bin/env python3
"""
Create comprehensive CIK groups for ALL, S&P 500, Russell 2000, etc.
This script creates special marker groups that the SEC adapter can use.
"""

import json
from pathlib import Path
from datetime import datetime

def create_comprehensive_groups():
    """Create comprehensive group definitions."""
    
    registry_dir = Path(__file__).parent.parent / "src/main/resources"
    
    # Create comprehensive groups file
    comprehensive = {
        "_metadata": {
            "description": "Comprehensive market index groups",
            "last_updated": datetime.now().isoformat(),
            "note": "These are special markers that the SEC adapter should handle dynamically"
        },
        
        "groups": {
            "ALL": {
                "description": "ALL companies with SEC EDGAR filings (10,000+ companies)",
                "members": ["_ALL_EDGAR_FILERS"],
                "_special": True,
                "_note": "The adapter should dynamically fetch all CIKs from SEC EDGAR"
            },
            
            "SP500_COMPLETE": {
                "description": "Complete S&P 500 Index - All 500 companies",
                "members": ["_SP500_CONSTITUENTS"],
                "_special": True,
                "_note": "The adapter should fetch current S&P 500 constituents",
                "_sample_tickers": ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA", "BRK.B", "JPM", "JNJ"]
            },
            
            "RUSSELL2000_COMPLETE": {
                "description": "Complete Russell 2000 Index - All 2000 small-cap companies",
                "members": ["_RUSSELL2000_CONSTITUENTS"],
                "_special": True,
                "_note": "The adapter should fetch current Russell 2000 constituents",
                "_sample_tickers": ["SMCI", "MARA", "APP", "CELH", "CHRD", "TMDX", "RMBS", "AFRM", "PLTR", "IONQ"]
            },
            
            "RUSSELL1000_COMPLETE": {
                "description": "Complete Russell 1000 Index - Top 1000 US companies",
                "members": ["_RUSSELL1000_CONSTITUENTS"],
                "_special": True,
                "_note": "The adapter should fetch current Russell 1000 constituents"
            },
            
            "RUSSELL3000_COMPLETE": {
                "description": "Complete Russell 3000 Index - Broad US market",
                "members": ["_RUSSELL3000_CONSTITUENTS"],
                "_special": True,
                "_note": "Russell 1000 + Russell 2000 combined"
            },
            
            "NASDAQ100_COMPLETE": {
                "description": "Complete NASDAQ-100 Index",
                "members": ["_NASDAQ100_CONSTITUENTS"],
                "_special": True,
                "_note": "The adapter should fetch current NASDAQ-100 constituents"
            },
            
            "NYSE_ALL": {
                "description": "All NYSE listed companies",
                "members": ["_NYSE_LISTED"],
                "_special": True,
                "_note": "All companies listed on New York Stock Exchange"
            },
            
            "NASDAQ_ALL": {
                "description": "All NASDAQ listed companies",
                "members": ["_NASDAQ_LISTED"],
                "_special": True,
                "_note": "All companies listed on NASDAQ"
            },
            
            "WILSHIRE5000_COMPLETE": {
                "description": "Wilshire 5000 - Total US stock market",
                "members": ["_WILSHIRE5000_CONSTITUENTS"],
                "_special": True,
                "_note": "Nearly all publicly traded US companies"
            },
            
            "FTSE100_COMPLETE": {
                "description": "FTSE 100 - Top 100 UK companies (with US listings)",
                "members": ["_FTSE100_US_LISTED"],
                "_special": True,
                "_note": "UK companies with ADRs or US listings"
            },
            
            "GLOBAL_MEGA_CAP": {
                "description": "Global mega-cap companies ($200B+ market cap)",
                "members": ["_GLOBAL_MEGA_CAP"],
                "_special": True,
                "_note": "Companies with market cap > $200 billion"
            },
            
            "US_LARGE_CAP": {
                "description": "US Large-cap companies ($10B+ market cap)",
                "members": ["_US_LARGE_CAP"],
                "_special": True,
                "_note": "US companies with market cap > $10 billion"
            },
            
            "US_MID_CAP": {
                "description": "US Mid-cap companies ($2B-$10B market cap)",
                "members": ["_US_MID_CAP"],
                "_special": True,
                "_note": "US companies with market cap $2-10 billion"
            },
            
            "US_SMALL_CAP": {
                "description": "US Small-cap companies ($300M-$2B market cap)",
                "members": ["_US_SMALL_CAP"],
                "_special": True,
                "_note": "US companies with market cap $300M-2B"
            },
            
            "US_MICRO_CAP": {
                "description": "US Micro-cap companies (<$300M market cap)",
                "members": ["_US_MICRO_CAP"],
                "_special": True,
                "_note": "US companies with market cap < $300 million"
            }
        },
        
        "implementation_notes": {
            "special_markers": {
                "_ALL_EDGAR_FILERS": "Fetch all CIKs from SEC EDGAR API",
                "_SP500_CONSTITUENTS": "Fetch from S&P or financial data provider",
                "_RUSSELL2000_CONSTITUENTS": "Fetch from FTSE Russell or data provider",
                "_NYSE_LISTED": "Fetch from NYSE listing data",
                "_NASDAQ_LISTED": "Fetch from NASDAQ listing data"
            },
            "dynamic_loading": "The SEC adapter should recognize these special markers and dynamically load the data",
            "caching": "Results should be cached with appropriate TTL (e.g., daily for indices, weekly for ALL)"
        }
    }
    
    # Save comprehensive groups file
    output_file = registry_dir / "comprehensive-groups.json"
    with open(output_file, 'w') as f:
        json.dump(comprehensive, f, indent=2)
    
    print(f"Created {output_file}")
    
    # Update main registry to reference these groups
    main_registry = registry_dir / "cik-registry.json"
    if main_registry.exists():
        with open(main_registry, 'r') as f:
            registry = json.load(f)
        
        # Add references to comprehensive groups
        registry['groups']['ALL'] = {
            "description": "ALL SEC EDGAR filers - See comprehensive-groups.json",
            "members": ["_ALL_EDGAR_FILERS"],
            "_special": True
        }
        
        registry['groups']['SP500_COMPLETE'] = {
            "description": "Complete S&P 500 - See comprehensive-groups.json",
            "members": ["_SP500_CONSTITUENTS"],
            "_special": True
        }
        
        registry['groups']['RUSSELL2000_COMPLETE'] = {
            "description": "Complete Russell 2000 - See comprehensive-groups.json",  
            "members": ["_RUSSELL2000_CONSTITUENTS"],
            "_special": True
        }
        
        with open(main_registry, 'w') as f:
            json.dump(registry, f, indent=2)
        
        print(f"Updated {main_registry} with comprehensive group references")
    
    print("\nComprehensive groups created successfully!")
    print("\nUsage in SEC adapter:")
    print("  jdbc:sec:ciks=ALL                    # All EDGAR filers")
    print("  jdbc:sec:ciks=SP500_COMPLETE         # Complete S&P 500")
    print("  jdbc:sec:ciks=RUSSELL2000_COMPLETE   # Complete Russell 2000")
    print("  jdbc:sec:ciks=NYSE_ALL               # All NYSE companies")
    print("\nNote: The adapter needs to implement dynamic loading for these special groups.")

if __name__ == "__main__":
    create_comprehensive_groups()
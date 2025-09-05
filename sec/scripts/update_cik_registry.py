#!/usr/bin/env python3
"""
CIK Registry Update Script
Updates the CIK registry files with the latest data from SEC EDGAR.
Can also use Anthropic API for intelligent ticker-to-CIK mapping.
"""

import json
import os
import sys
import time
import logging
import argparse
import urllib.request
import urllib.error
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
from pathlib import Path

# Optional: Use Anthropic for intelligent mapping
try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False
    print("Warning: anthropic package not installed. Install with: pip install anthropic")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CIKRegistryUpdater:
    """Updates CIK registry files with SEC EDGAR data."""
    
    SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
    SEC_CIK_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
    SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{}.json"
    
    def __init__(self, registry_dir: str = None):
        """Initialize the updater with the registry directory."""
        if registry_dir is None:
            # Default to the sec resources directory
            registry_dir = Path(__file__).parent.parent / "src/main/resources"
        self.registry_dir = Path(registry_dir)
        self.anthropic_client = None
        
        # Try to initialize Anthropic if available and API key is set
        if ANTHROPIC_AVAILABLE and os.environ.get("ANTHROPIC_API_KEY"):
            try:
                self.anthropic_client = anthropic.Anthropic()
                logger.info("Anthropic client initialized")
            except Exception as e:
                logger.warning(f"Failed to initialize Anthropic: {e}")
    
    def fetch_all_sec_tickers(self) -> Dict[str, str]:
        """Fetch all tickers and CIKs from SEC."""
        logger.info("Fetching all SEC tickers...")
        try:
            # SEC rate limit is 10 requests per second, so add delay
            time.sleep(0.2)  # 5 requests per second to be safe
            
            req = urllib.request.Request(self.SEC_TICKERS_URL, 
                headers={
                    'User-Agent': 'Apache-Calcite-SEC-Adapter admin@example.com',
                    'Accept': 'application/json',
                    'Host': 'www.sec.gov'
                })
            with urllib.request.urlopen(req, timeout=30) as response:
                data = json.loads(response.read().decode())
            
            # Convert to ticker -> CIK mapping
            ticker_to_cik = {}
            for item in data.values():
                ticker = item.get('ticker', '').upper()
                cik = str(item.get('cik_str', '')).zfill(10)
                if ticker and cik:
                    ticker_to_cik[ticker] = cik
            
            logger.info(f"Fetched {len(ticker_to_cik)} ticker-to-CIK mappings")
            return ticker_to_cik
        except Exception as e:
            logger.error(f"Failed to fetch SEC tickers: {e}")
            return {}
    
    def fetch_exchange_data(self) -> Dict[str, Dict]:
        """Fetch detailed exchange and company data from SEC."""
        logger.info("Fetching SEC exchange data...")
        try:
            # SEC rate limit is 10 requests per second, so add delay
            time.sleep(0.2)  # 5 requests per second to be safe
            
            req = urllib.request.Request(self.SEC_CIK_URL,
                headers={
                    'User-Agent': 'Apache-Calcite-SEC-Adapter admin@example.com',
                    'Accept': 'application/json',
                    'Host': 'www.sec.gov'
                })
            with urllib.request.urlopen(req, timeout=30) as response:
                data = json.loads(response.read().decode())
            
            # Process the data
            exchange_data = {}
            for item in data.get('data', []):
                if len(item) >= 4:
                    cik = str(item[0]).zfill(10)
                    name = item[1]
                    ticker = item[2]
                    exchange = item[3] if len(item) > 3 else ''
                    
                    exchange_data[ticker] = {
                        'cik': cik,
                        'name': name,
                        'exchange': exchange
                    }
            
            logger.info(f"Fetched {len(exchange_data)} exchange records")
            return exchange_data
        except Exception as e:
            logger.error(f"Failed to fetch exchange data: {e}")
            return {}
    
    def fetch_russell_2000_list(self) -> List[str]:
        """
        Fetch Russell 2000 constituent list.
        Note: This would need a data provider API. For now, returns a sample.
        """
        # TODO: Integrate with a financial data provider for Russell 2000 constituents
        logger.warning("Russell 2000 list requires external data provider")
        
        # For now, we'll use the tickers we already have
        russell_file = self.registry_dir / "russell2000-complete.json"
        if russell_file.exists():
            with open(russell_file, 'r') as f:
                data = json.load(f)
                return list(data.get('tickers', {}).keys())
        return []
    
    def get_all_edgar_ciks(self) -> Set[str]:
        """Get all CIKs that have filed with EDGAR."""
        logger.info("Fetching all EDGAR CIKs...")
        all_ciks = set()
        
        # Get from company tickers
        ticker_data = self.fetch_all_sec_tickers()
        all_ciks.update(ticker_data.values())
        
        # Get from exchange data
        exchange_data = self.fetch_exchange_data()
        for info in exchange_data.values():
            all_ciks.add(info['cik'])
        
        logger.info(f"Found {len(all_ciks)} unique CIKs in EDGAR")
        return all_ciks
    
    def use_anthropic_for_mapping(self, unmapped_tickers: List[str]) -> Dict[str, str]:
        """Use Anthropic to help map tickers to CIKs."""
        if not self.anthropic_client:
            logger.warning("Anthropic client not available")
            return {}
        
        logger.info(f"Using Anthropic to map {len(unmapped_tickers)} tickers...")
        
        # Process in batches
        batch_size = 20
        mappings = {}
        
        for i in range(0, len(unmapped_tickers), batch_size):
            batch = unmapped_tickers[i:i+batch_size]
            
            prompt = f"""
            I need to find the SEC CIK numbers for these company tickers.
            Please provide the 10-digit CIK (padded with zeros) for each ticker.
            If you're not certain about a CIK, mark it as "UNKNOWN".
            
            Tickers: {', '.join(batch)}
            
            Respond in JSON format like:
            {{"TICKER": "0000123456", "TICKER2": "0000789012"}}
            """
            
            try:
                response = self.anthropic_client.messages.create(
                    model="claude-3-haiku-20240307",
                    max_tokens=1000,
                    messages=[{"role": "user", "content": prompt}]
                )
                
                # Parse the response
                content = response.content[0].text
                # Extract JSON from the response
                import re
                json_match = re.search(r'\{[^}]+\}', content)
                if json_match:
                    result = json.loads(json_match.group())
                    for ticker, cik in result.items():
                        if cik != "UNKNOWN" and len(cik) == 10:
                            mappings[ticker] = cik
                
                time.sleep(0.5)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Anthropic API error: {e}")
        
        logger.info(f"Anthropic helped map {len(mappings)} tickers")
        return mappings
    
    def update_main_registry(self):
        """Update the main cik-registry.json file."""
        registry_file = self.registry_dir / "cik-registry.json"
        
        # Load existing registry
        if registry_file.exists():
            with open(registry_file, 'r') as f:
                registry = json.load(f)
        else:
            registry = {"tickers": {}, "groups": {}}
        
        # Fetch latest SEC data
        sec_tickers = self.fetch_all_sec_tickers()
        
        # Update tickers (preserve existing, add new)
        existing_count = len(registry.get('tickers', {}))
        registry['tickers'].update(sec_tickers)
        new_count = len(registry['tickers']) - existing_count
        
        logger.info(f"Updated main registry: {new_count} new tickers added")
        
        # Add ALL group with all CIKs
        all_ciks = list(set(sec_tickers.values()))
        registry['groups']['ALL'] = {
            "description": f"All {len(all_ciks)} companies with SEC filings",
            "members": all_ciks[:100]  # Limit for practicality
        }
        
        registry['groups']['ALL_EDGAR'] = {
            "description": f"Reference to all {len(all_ciks)} EDGAR filers (use with caution)",
            "members": ["_ALL_SEC_FILERS"],
            "_note": "This is a special marker. The adapter should fetch all CIKs dynamically."
        }
        
        # Save updated registry
        with open(registry_file, 'w') as f:
            json.dump(registry, f, indent=2)
        
        logger.info(f"Main registry updated: {len(registry['tickers'])} total tickers")
    
    def update_russell_2000_registry(self):
        """Update the Russell 2000 registry file."""
        russell_file = self.registry_dir / "russell2000-complete.json"
        
        # Load existing file
        if russell_file.exists():
            with open(russell_file, 'r') as f:
                russell_data = json.load(f)
        else:
            russell_data = {"tickers": {}, "groups": {}}
        
        # Get SEC ticker data
        sec_tickers = self.fetch_all_sec_tickers()
        exchange_data = self.fetch_exchange_data()
        
        # Update CIKs for existing tickers
        updated = 0
        unmapped = []
        
        for ticker in russell_data.get('tickers', {}).keys():
            if ticker in sec_tickers:
                russell_data['tickers'][ticker] = sec_tickers[ticker]
                updated += 1
            elif ticker in exchange_data:
                russell_data['tickers'][ticker] = exchange_data[ticker]['cik']
                updated += 1
            elif russell_data['tickers'][ticker] == "":
                unmapped.append(ticker)
        
        logger.info(f"Updated {updated} Russell 2000 CIKs from SEC data")
        logger.info(f"{len(unmapped)} tickers still need CIKs")
        
        # Try to map remaining using Anthropic if available
        if unmapped and self.anthropic_client:
            anthropic_mappings = self.use_anthropic_for_mapping(unmapped[:50])  # Limit for cost
            for ticker, cik in anthropic_mappings.items():
                if ticker in russell_data['tickers']:
                    russell_data['tickers'][ticker] = cik
        
        # Update metadata
        russell_data['_metadata'] = {
            "last_updated": datetime.now().isoformat(),
            "total_tickers": len(russell_data['tickers']),
            "mapped_ciks": len([v for v in russell_data['tickers'].values() if v and v != ""]),
            "unmapped": len([v for v in russell_data['tickers'].values() if not v or v == ""])
        }
        
        # Save updated file
        with open(russell_file, 'w') as f:
            json.dump(russell_data, f, indent=2)
        
        logger.info(f"Russell 2000 registry updated")
    
    def create_all_edgar_registry(self):
        """Create a registry file with ALL EDGAR filers."""
        all_edgar_file = self.registry_dir / "all-edgar-registry.json"
        
        # Get all CIKs
        all_ciks = self.get_all_edgar_ciks()
        
        # Get exchange data for company names
        exchange_data = self.fetch_exchange_data()
        
        # Create registry structure
        registry = {
            "_metadata": {
                "description": "Complete list of all SEC EDGAR filers",
                "last_updated": datetime.now().isoformat(),
                "total_companies": len(all_ciks),
                "warning": "This is a very large dataset. Use with caution."
            },
            "groups": {
                "ALL_EDGAR_FILERS": {
                    "description": f"All {len(all_ciks)} companies with SEC filings",
                    "members": list(all_ciks)
                }
            }
        }
        
        # Save the file
        with open(all_edgar_file, 'w') as f:
            json.dump(registry, f, indent=2)
        
        logger.info(f"Created all-edgar-registry.json with {len(all_ciks)} CIKs")
    
    def validate_ciks(self, sample_size: int = 10):
        """Validate a sample of CIKs by checking if they have filings."""
        logger.info(f"Validating {sample_size} random CIKs...")
        
        # Load main registry
        registry_file = self.registry_dir / "cik-registry.json"
        with open(registry_file, 'r') as f:
            registry = json.load(f)
        
        # Sample some CIKs
        import random
        tickers = random.sample(list(registry['tickers'].items()), 
                               min(sample_size, len(registry['tickers'])))
        
        valid = 0
        for ticker, cik in tickers:
            url = self.SEC_SUBMISSIONS_URL.format(cik.lstrip('0'))
            try:
                # Add proper headers and rate limiting
                req = urllib.request.Request(url, headers={
                    'User-Agent': 'Apache-Calcite-SEC-Adapter admin@example.com',
                    'Accept': 'application/json',
                    'Host': 'data.sec.gov'
                })
                response = urllib.request.urlopen(req, timeout=10)
                if response.getcode() == 200:
                    valid += 1
                    logger.info(f"✓ {ticker} (CIK {cik}) is valid")
                else:
                    logger.warning(f"✗ {ticker} (CIK {cik}) returned {response.getcode()}")
            except Exception as e:
                logger.error(f"✗ {ticker} (CIK {cik}) failed: {e}")
            
            time.sleep(0.2)  # Rate limit: 5 requests per second
        
        logger.info(f"Validation complete: {valid}/{sample_size} CIKs are valid")
    
    def run_full_update(self):
        """Run a complete update of all registry files."""
        logger.info("Starting full registry update...")
        
        # Update main registry
        logger.info("Updating main CIK registry...")
        self.update_main_registry()
        
        # Update Russell 2000
        logger.info("Updating Russell 2000 registry...")
        self.update_russell_2000_registry()
        
        # Create ALL EDGAR registry
        logger.info("Creating ALL EDGAR registry...")
        self.create_all_edgar_registry()
        
        # Validate a sample
        self.validate_ciks(5)
        
        logger.info("Full update complete!")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Update CIK Registry Files')
    parser.add_argument('--registry-dir', type=str, 
                       help='Directory containing registry files')
    parser.add_argument('--update-main', action='store_true',
                       help='Update main cik-registry.json')
    parser.add_argument('--update-russell', action='store_true',
                       help='Update Russell 2000 registry')
    parser.add_argument('--create-all', action='store_true',
                       help='Create ALL EDGAR registry')
    parser.add_argument('--validate', type=int, metavar='N',
                       help='Validate N random CIKs')
    parser.add_argument('--full', action='store_true',
                       help='Run full update')
    
    args = parser.parse_args()
    
    updater = CIKRegistryUpdater(args.registry_dir)
    
    if args.full:
        updater.run_full_update()
    else:
        if args.update_main:
            updater.update_main_registry()
        if args.update_russell:
            updater.update_russell_2000_registry()
        if args.create_all:
            updater.create_all_edgar_registry()
        if args.validate:
            updater.validate_ciks(args.validate)
        
        if not any([args.update_main, args.update_russell, 
                   args.create_all, args.validate]):
            logger.info("No action specified. Use --help for options.")


if __name__ == "__main__":
    main()
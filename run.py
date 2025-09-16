#!/usr/bin/env python3
"""
Main runner for California license data collectors.
"""

import sys
import argparse
import logging
from datetime import datetime
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from common.config import get_config
from common.progress import ScraperProgress
from cslb.collector import CSLBCollector


def setup_logging(config):
    """Set up logging configuration."""
    log_format = config.log_format
    log_level = getattr(logging, config.log_level.upper(), logging.INFO)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)  # Only warnings and errors to console

    # Configure root logger
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[console_handler]
    )

    # Reduce noise from libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


def run_cslb():
    """Run CSLB collector."""
    print("\n" + "="*70)
    print("  CSLB COLLECTOR")
    print("="*70)

    collector = CSLBCollector()
    return collector.run()


def run_dca():
    """Run DCA collector."""
    print("\n" + "="*70)
    print("  DCA COLLECTOR")
    print("="*70)

    # TODO: Implement DCA collector
    print("DCA collector not yet implemented")
    return False


def run_all():
    """Run all collectors."""
    print("\n" + "="*70)
    print("  RUNNING ALL CALIFORNIA COLLECTORS")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)

    results = {
        'CSLB': run_cslb(),
        'DCA': run_dca()
    }

    # Print summary
    print("\n" + "="*70)
    print("  SUMMARY")
    print("="*70)
    for collector, success in results.items():
        status = "✅ Success" if success else "❌ Failed"
        print(f"  {collector}: {status}")
    print("="*70)

    return all(results.values())


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="California License Data Collectors")
    parser.add_argument(
        "collector",
        choices=["cslb", "dca", "all"],
        help="Which collector to run"
    )
    parser.add_argument(
        "--config",
        help="Path to configuration file",
        default=None
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose output"
    )

    args = parser.parse_args()

    # Load configuration
    config = get_config()
    if args.verbose:
        config.log_level = "DEBUG"

    # Set up logging
    setup_logging(config)

    # Run selected collector
    success = False
    if args.collector == "cslb":
        success = run_cslb()
    elif args.collector == "dca":
        success = run_dca()
    elif args.collector == "all":
        success = run_all()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
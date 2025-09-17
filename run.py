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
from vk_api_utils import SlackNotifier


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


def run_cslb(slack=None):
    """Run CSLB collector."""
    print("\n" + "="*70)
    print("  CSLB COLLECTOR")
    print("="*70)

    if slack:
        slack.notify_progress("Starting CSLB collector...")

    collector = CSLBCollector()
    success = collector.run()

    if slack:
        if success:
            slack.notify_progress("✅ CSLB collector completed successfully")
        else:
            slack.notify_warning("⚠️ CSLB collector failed")

    return success


def run_dca(slack=None):
    """Run DCA collector."""
    print("\n" + "="*70)
    print("  DCA COLLECTOR")
    print("="*70)

    if slack:
        slack.notify_progress("Starting DCA collector...")

    # TODO: Implement DCA collector
    print("DCA collector not yet implemented")

    if slack:
        slack.notify_warning("⚠️ DCA collector not yet implemented")

    return False


def run_all(slack=None):
    """Run all collectors."""
    print("\n" + "="*70)
    print("  RUNNING ALL CALIFORNIA COLLECTORS")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)

    results = {
        'CSLB': run_cslb(slack),
        'DCA': run_dca(slack)
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
    parser.add_argument(
        "--slack",
        default="on",
        choices=["on", "off"],
        help="Enable/disable Slack notifications (default: on)"
    )

    args = parser.parse_args()

    # Load configuration
    config = get_config()
    if args.verbose:
        config.log_level = "DEBUG"

    # Set up logging
    setup_logging(config)

    # Set up Slack notifications
    slack = None
    if args.slack.lower() != "off":
        slack = SlackNotifier("California Collectors")
        slack.notify_start({
            "Collector": args.collector.upper(),
            "Time": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })

    # Run selected collector
    success = False
    try:
        if args.collector == "cslb":
            success = run_cslb(slack)
        elif args.collector == "dca":
            success = run_dca(slack)
        elif args.collector == "all":
            success = run_all(slack)

        # Send final notification
        if slack:
            if success:
                slack.notify_success("California collectors completed successfully")
            else:
                slack.notify_warning("California collectors completed with errors")

    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        if slack:
            slack.notify_error(f"California collectors failed", exception=e)
        success = False

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
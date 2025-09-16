import os
import sys
import time
import logging
from datetime import datetime
from typing import Optional, Dict, Any
from tqdm import tqdm
from contextlib import contextmanager

class ScraperProgress:
    """
    Unified progress tracking system for VK scrapers with phase tracking,
    progress bars, and file logging.
    """

    PHASES = {
        'INIT': '🔧 Initializing',
        'CONFIG': '⚙️  Configuring',
        'AUTH': '🔐 Authenticating',
        'SEARCH': '🔍 Searching',
        'COLLECT': '📊 Collecting',
        'PROCESS': '⚡ Processing',
        'VALIDATE': '✓ Validating',
        'UPLOAD': '☁️  Uploading',
        'CLEANUP': '🧹 Cleaning up',
        'COMPLETE': '✅ Complete',
        'ERROR': '❌ Error'
    }

    def __init__(self, scraper_name: str, log_dir: Optional[str] = None):
        """
        Initialize progress tracker for a scraper.

        Args:
            scraper_name: Name of the scraper (e.g., "CA CSLB", "NY DMV")
            log_dir: Directory for log files (defaults to ../logs/)
        """
        self.scraper_name = scraper_name
        self.start_time = time.time()
        self.current_phase = 'INIT'
        self.phase_start_time = time.time()
        self.stats = {
            'total_records': 0,
            'processed_records': 0,
            'uploaded_records': 0,
            'failed_records': 0,
            'phases_completed': []
        }

        # Setup logging
        if log_dir is None:
            log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')

        os.makedirs(log_dir, exist_ok=True)

        # Create log filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        safe_name = scraper_name.replace(' ', '_').replace('/', '_')
        self.log_file = os.path.join(log_dir, f'{safe_name}_{timestamp}.log')

        # Configure file logger
        self.logger = logging.getLogger(f'scraper_{safe_name}')
        self.logger.setLevel(logging.DEBUG)

        # Remove existing handlers to avoid duplicates
        self.logger.handlers = []

        # File handler for detailed logs
        fh = logging.FileHandler(self.log_file)
        fh.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

        # Progress bar placeholder
        self.pbar = None
        self.sub_pbar = None

        self._print_header()

    def _print_header(self):
        """Print scraper header with current status."""
        os.system('clear' if os.name == 'posix' else 'cls')
        print("=" * 70)
        print(f"  {self.scraper_name} SCRAPER")
        print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Log: {self.log_file}")
        print("=" * 70)
        print()

    def set_phase(self, phase: str, description: Optional[str] = None):
        """
        Update the current phase of the scraper.

        Args:
            phase: Phase key from PHASES dict
            description: Optional additional description
        """
        if self.current_phase != phase:
            # Log phase completion
            if self.current_phase != 'INIT':
                elapsed = time.time() - self.phase_start_time
                self.stats['phases_completed'].append({
                    'phase': self.current_phase,
                    'duration': elapsed
                })
                self.logger.info(f"Completed phase {self.current_phase} in {elapsed:.2f}s")

            self.current_phase = phase
            self.phase_start_time = time.time()

            # Update display
            phase_display = self.PHASES.get(phase, phase)
            if description:
                status_msg = f"{phase_display}: {description}"
            else:
                status_msg = phase_display

            # Clear line and print status
            print(f"\r{' ' * 80}\r{status_msg}", flush=True)
            self.logger.info(f"Phase: {status_msg}")

    @contextmanager
    def progress_bar(self, total: int, desc: str, unit: str = "records"):
        """
        Context manager for progress bar.

        Args:
            total: Total number of items
            desc: Description for progress bar
            unit: Unit of measurement
        """
        self.pbar = tqdm(
            total=total,
            desc=desc,
            unit=unit,
            ncols=100,
            bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'
        )
        try:
            yield self.pbar
        finally:
            self.pbar.close()
            self.pbar = None

    @contextmanager
    def sub_progress(self, total: int, desc: str, leave: bool = False):
        """
        Context manager for sub-progress bar (nested operations).

        Args:
            total: Total number of items
            desc: Description for progress bar
            leave: Whether to leave the bar on screen after completion
        """
        self.sub_pbar = tqdm(
            total=total,
            desc=f"  └─ {desc}",
            unit="items",
            ncols=100,
            leave=leave,
            bar_format='  {l_bar}{bar}| {n_fmt}/{total_fmt}'
        )
        try:
            yield self.sub_pbar
        finally:
            self.sub_pbar.close()
            self.sub_pbar = None

    def update(self, n: int = 1):
        """Update main progress bar."""
        if self.pbar:
            self.pbar.update(n)
            self.stats['processed_records'] += n

    def log(self, message: str, level: str = 'info'):
        """
        Log a message to file (not displayed on screen).

        Args:
            message: Message to log
            level: Log level (debug, info, warning, error)
        """
        getattr(self.logger, level.lower())(message)

    def log_error(self, error: Exception, context: str = ""):
        """
        Log an error with full traceback.

        Args:
            error: Exception object
            context: Additional context about where error occurred
        """
        import traceback
        self.stats['failed_records'] += 1
        error_msg = f"Error in {context}: {str(error)}\n{traceback.format_exc()}"
        self.logger.error(error_msg)

    def set_total(self, total: int):
        """Set total number of records to process."""
        self.stats['total_records'] = total
        self.log(f"Total records to process: {total}")

    def complete(self):
        """Mark scraper as complete and show summary."""
        self.set_phase('COMPLETE')

        elapsed_total = time.time() - self.start_time

        # Print summary
        print("\n" + "=" * 70)
        print(f"  SCRAPER COMPLETE: {self.scraper_name}")
        print("=" * 70)
        print(f"  Total Time: {self._format_time(elapsed_total)}")
        print(f"  Records Processed: {self.stats['processed_records']:,}")
        print(f"  Records Uploaded: {self.stats['uploaded_records']:,}")
        print(f"  Failed Records: {self.stats['failed_records']:,}")

        if self.stats['processed_records'] > 0:
            success_rate = (self.stats['uploaded_records'] / self.stats['processed_records']) * 100
            print(f"  Success Rate: {success_rate:.1f}%")

            if elapsed_total > 0:
                rate = self.stats['processed_records'] / elapsed_total
                print(f"  Processing Rate: {rate:.1f} records/sec")

        print(f"\n  Detailed log: {self.log_file}")
        print("=" * 70)

        # Log summary
        self.logger.info("=" * 50)
        self.logger.info(f"SUMMARY - {self.scraper_name}")
        self.logger.info(f"Total Time: {self._format_time(elapsed_total)}")
        self.logger.info(f"Records Processed: {self.stats['processed_records']}")
        self.logger.info(f"Records Uploaded: {self.stats['uploaded_records']}")
        self.logger.info(f"Failed Records: {self.stats['failed_records']}")

        # Log phase breakdown
        if self.stats['phases_completed']:
            self.logger.info("\nPhase Breakdown:")
            for phase_info in self.stats['phases_completed']:
                self.logger.info(f"  {phase_info['phase']}: {self._format_time(phase_info['duration'])}")
        self.logger.info("=" * 50)

    def _format_time(self, seconds: float) -> str:
        """Format seconds into human-readable time."""
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            mins = seconds / 60
            return f"{mins:.1f}m"
        else:
            hours = seconds / 3600
            return f"{hours:.1f}h"

    def increment_uploaded(self, count: int = 1):
        """Increment uploaded records counter."""
        self.stats['uploaded_records'] += count

    def increment_failed(self, count: int = 1):
        """Increment failed records counter."""
        self.stats['failed_records'] += count


# Example usage function for reference
def example_scraper_with_progress():
    """Example of how to use ScraperProgress in a scraper."""

    # Initialize progress tracker
    progress = ScraperProgress("Example Scraper")

    try:
        # Configuration phase
        progress.set_phase('CONFIG', 'Loading settings')
        time.sleep(1)  # Simulate configuration

        # Authentication phase
        progress.set_phase('AUTH', 'Logging into system')
        time.sleep(1)  # Simulate auth

        # Search phase
        progress.set_phase('SEARCH', 'Finding records')
        time.sleep(1)  # Simulate search

        # Collection phase
        progress.set_phase('COLLECT', 'Gathering data')
        total_records = 100
        progress.set_total(total_records)

        with progress.progress_bar(total_records, "Collecting records") as pbar:
            for i in range(total_records):
                # Simulate processing
                time.sleep(0.01)
                progress.log(f"Processing record {i+1}")
                progress.update(1)

        # Upload phase
        progress.set_phase('UPLOAD', 'Sending to database')
        with progress.progress_bar(total_records, "Uploading") as pbar:
            for i in range(total_records):
                time.sleep(0.01)
                progress.increment_uploaded()
                progress.update(1)

        # Complete
        progress.complete()

    except Exception as e:
        progress.set_phase('ERROR')
        progress.log_error(e, "Main execution")
        raise


if __name__ == "__main__":
    # Run example if script is executed directly
    example_scraper_with_progress()
"""
Configuration management for California collectors.
"""

import os
import json
from typing import Dict, Any, Optional
from pathlib import Path
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class CSLBConfig:
    """Configuration for CSLB collector."""
    api_url: str = "https://www.cslb.ca.gov/onlineservices/DataPortalAPI/GetbyClassification.asmx?op=GetMasterFile"
    api_token: str = "9504E105-22B0-40E5-945F-D713A09C14AE"
    batch_size: int = 5000
    agency_name: str = "Contractors State Licensing Board"


@dataclass
class DCAConfig:
    """Configuration for DCA collector."""
    base_url: str = "https://www.breeze.ca.gov"
    batch_size: int = 1000
    timeout: int = 30


@dataclass
class CollectorConfig:
    """Main configuration for California collectors."""

    # Paths
    base_dir: Path = Path(__file__).parent.parent.parent
    data_dir: Path = None
    log_dir: Path = None
    config_dir: Path = None

    # Database
    db_batch_size: int = 5000
    use_delta_table: bool = True

    # Collectors
    cslb: CSLBConfig = None
    dca: DCAConfig = None

    # Logging
    log_level: str = "INFO"
    log_to_file: bool = True
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    def __post_init__(self):
        """Initialize paths and sub-configs after dataclass creation."""
        if self.data_dir is None:
            self.data_dir = self.base_dir / "data"
        if self.log_dir is None:
            self.log_dir = self.base_dir / "logs"
        if self.config_dir is None:
            self.config_dir = self.base_dir / "config"

        if self.cslb is None:
            self.cslb = CSLBConfig()
        if self.dca is None:
            self.dca = DCAConfig()

        # Ensure directories exist
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        (self.data_dir / "input").mkdir(exist_ok=True)
        (self.data_dir / "output").mkdir(exist_ok=True)
        (self.data_dir / "temp").mkdir(exist_ok=True)

    @classmethod
    def from_file(cls, config_file: str = None) -> "CollectorConfig":
        """
        Load configuration from JSON file.

        Args:
            config_file: Path to configuration file

        Returns:
            CollectorConfig instance
        """
        if config_file is None:
            base_dir = Path(__file__).parent.parent.parent
            config_file = base_dir / "config" / "config.json"

        config_data = {}
        if Path(config_file).exists():
            with open(config_file, "r") as f:
                config_data = json.load(f)
                logger.info(f"Loaded configuration from {config_file}")

        # Create config with loaded data
        config = cls()

        # Update with loaded values
        if "database" in config_data:
            config.db_batch_size = config_data["database"].get("batch_size", config.db_batch_size)
            config.use_delta_table = config_data["database"].get("use_delta_table", config.use_delta_table)

        if "cslb" in config_data:
            config.cslb.api_url = config_data["cslb"].get("api_url", config.cslb.api_url)
            config.cslb.api_token = config_data["cslb"].get("api_token", config.cslb.api_token)
            config.cslb.batch_size = config_data["cslb"].get("batch_size", config.cslb.batch_size)

        if "dca" in config_data:
            config.dca.base_url = config_data["dca"].get("base_url", config.dca.base_url)
            config.dca.batch_size = config_data["dca"].get("batch_size", config.dca.batch_size)
            config.dca.timeout = config_data["dca"].get("timeout", config.dca.timeout)

        if "logging" in config_data:
            config.log_level = config_data["logging"].get("level", config.log_level)
            config.log_to_file = config_data["logging"].get("to_file", config.log_to_file)

        return config

    def save_to_file(self, config_file: str = None) -> None:
        """
        Save configuration to JSON file.

        Args:
            config_file: Path to save configuration
        """
        if config_file is None:
            config_file = self.config_dir / "config.json"

        config_data = {
            "database": {
                "batch_size": self.db_batch_size,
                "use_delta_table": self.use_delta_table
            },
            "cslb": {
                "api_url": self.cslb.api_url,
                "api_token": self.cslb.api_token,
                "batch_size": self.cslb.batch_size,
                "agency_name": self.cslb.agency_name
            },
            "dca": {
                "base_url": self.dca.base_url,
                "batch_size": self.dca.batch_size,
                "timeout": self.dca.timeout
            },
            "logging": {
                "level": self.log_level,
                "to_file": self.log_to_file,
                "format": self.log_format
            },
            "paths": {
                "data": str(self.data_dir),
                "logs": str(self.log_dir),
                "config": str(self.config_dir)
            }
        }

        Path(config_file).parent.mkdir(parents=True, exist_ok=True)
        with open(config_file, "w") as f:
            json.dump(config_data, f, indent=2, default=str)
            logger.info(f"Saved configuration to {config_file}")

    def get_input_path(self, filename: str) -> Path:
        """Get path for input file."""
        return self.data_dir / "input" / filename

    def get_output_path(self, filename: str) -> Path:
        """Get path for output file."""
        return self.data_dir / "output" / filename

    def get_temp_path(self, filename: str) -> Path:
        """Get path for temporary file."""
        return self.data_dir / "temp" / filename

    def get_log_path(self, filename: str) -> Path:
        """Get path for log file."""
        return self.log_dir / filename


# Singleton instance
_config: Optional[CollectorConfig] = None


def get_config() -> CollectorConfig:
    """
    Get the singleton configuration instance.

    Returns:
        CollectorConfig instance
    """
    global _config
    if _config is None:
        _config = CollectorConfig.from_file()
    return _config
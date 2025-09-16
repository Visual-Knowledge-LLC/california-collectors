# California License Data Collectors

Modern, efficient collectors for California state licensing data from CSLB and DCA.

## Features

- 🚀 **Modern Architecture**: Clean, modular design with separation of concerns
- 📊 **Progress Tracking**: Real-time progress bars with phase indicators
- 📝 **Detailed Logging**: Comprehensive logs for debugging, minimal console output
- ⚡ **Efficient Processing**: Batch processing with configurable sizes
- 🔧 **Configuration Management**: JSON-based configuration with environment variable support
- 🛡️ **Error Handling**: Robust error handling with automatic retries
- 📈 **Performance Metrics**: Processing rates and success statistics

## Quick Start

### Installation

1. Clone the repository:
```bash
git clone https://github.com/Visual-Knowledge-LLC/california-collectors.git
cd california-collectors
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your database credentials
```

4. Set up input data:
```bash
# Copy ZIP code mappings to data/input/zips/all_zips.csv
# Copy agency ID mappings to data/input/licensing_agencies/cslb_agency_ids.csv
```

### Usage

Run CSLB collector:
```bash
python run.py cslb
```

Run DCA collector:
```bash
python run.py dca
```

Run all collectors:
```bash
python run.py all
```

Enable verbose logging:
```bash
python run.py cslb --verbose
```

## Project Structure

```
california-collectors/
├── src/
│   ├── common/          # Shared utilities
│   │   ├── config.py     # Configuration management
│   │   ├── database.py   # Database operations
│   │   └── progress.py   # Progress tracking
│   ├── cslb/            # CSLB collector
│   │   └── collector.py
│   └── dca/             # DCA collector
│       └── collector.py
├── data/
│   ├── input/           # Input files (mappings, etc.)
│   ├── output/          # Output files
│   └── temp/            # Temporary files
├── logs/                # Log files
├── config/              # Configuration files
├── tests/               # Unit tests
├── run.py               # Main runner script
└── requirements.txt     # Python dependencies
```

## Configuration

Configuration is managed through `config/config.json` and environment variables.

### Environment Variables

Create a `.env` file in the project root:

```env
# Database Configuration
DB_HOST=your-database-host
DB_PORT=5432
DB_NAME=vk_production
DB_USER=your-username
DB_PASSWORD=your-password
DB_POOL_SIZE=5
DB_MAX_OVERFLOW=10
```

### Configuration File

Example `config/config.json`:

```json
{
  "database": {
    "batch_size": 5000,
    "use_delta_table": true
  },
  "cslb": {
    "api_url": "https://www.cslb.ca.gov/...",
    "api_token": "your-token",
    "batch_size": 5000
  },
  "dca": {
    "base_url": "https://www.breeze.ca.gov",
    "batch_size": 1000,
    "timeout": 30
  },
  "logging": {
    "level": "INFO",
    "to_file": true
  }
}
```

## Progress Tracking

The collectors use an advanced progress tracking system that shows:

- **Current Phase**: Configuration, Authentication, Collection, Processing, Upload, etc.
- **Progress Bars**: Visual progress with time estimates
- **Statistics**: Records processed, success rate, processing speed
- **Clean Output**: Detailed logs go to files, only essential info on screen

Example output:
```
==================================================================
  CA CSLB COLLECTOR
  Started: 2025-09-16 10:30:00
  Log: logs/CA_CSLB_Collector_20250916_103000.log
==================================================================

📊 Collecting: Downloading CSLB master file
⚡ Processing: Processing CSLB records
Processing records: 100%|████████| 450000/450000 [02:30<00:00, 3000.0 recs/s]
☁️ Uploading: Sending to database
Uploading to database: 100%|█████| 425000/425000 [01:45<00:00, 4047.6 recs/s]

==================================================================
  SCRAPER COMPLETE: CA CSLB Collector
==================================================================
  Total Time: 4.5m
  Records Processed: 450,000
  Records Uploaded: 425,000
  Failed Records: 0
  Success Rate: 94.4%
  Processing Rate: 1666.7 records/sec

  Detailed log: logs/CA_CSLB_Collector_20250916_103000.log
==================================================================
```

## Development

### Running Tests

```bash
pytest tests/
pytest tests/ --cov=src --cov-report=html
```

### Code Formatting

```bash
black src/ tests/
flake8 src/ tests/
mypy src/
```

## Logging

Logs are stored in the `logs/` directory with timestamps:
- `CA_CSLB_Collector_YYYYMMDD_HHMMSS.log`
- `CA_DCA_Collector_YYYYMMDD_HHMMSS.log`

Each log contains:
- Detailed processing information
- Error tracebacks
- Performance metrics
- Data validation issues

## Troubleshooting

### Common Issues

1. **Database Connection Failed**
   - Check your `.env` file has correct credentials
   - Ensure database is accessible from your network
   - Verify VPN connection if required

2. **Missing Input Files**
   - Ensure ZIP mappings are in `data/input/zips/all_zips.csv`
   - Ensure agency mappings are in `data/input/licensing_agencies/`

3. **API Timeouts**
   - CSLB API may be slow for large datasets
   - Timeout is set to 5 minutes by default
   - Can be adjusted in configuration

## Performance

- **CSLB Collector**: ~450,000 records in ~5 minutes
- **DCA Collector**: Variable based on agencies selected
- **Database Upload**: ~4,000 records/second in batches

## License

Proprietary - Visual Knowledge LLC

## Support

For issues or questions, contact the Visual Knowledge development team.
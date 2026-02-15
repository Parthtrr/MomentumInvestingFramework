# 📁 Project Structure – Stock Pattern Enricher

```text
stock_pattern_enricher/
│
├── config/                  # Configuration files
│   └── config.py            # Central configuration (Elastic, threading, etc.)
│
├── dao/                     # Data access layer (ElasticSearch I/O)
│   ├── elastic_interface.py # Interface for data access
│   └── elastic_impl.py      # Implementation for ElasticSearch
│
├── models/                  # Data models
│   └── stock_data.py        # OHLCV data + patterns container
│
├── services/                # Core services and orchestration
│   ├── pipeline.py          # Core pipeline orchestration
│   ├── service.py           # Main service orchestration (stitches everything)
│   └── thread_executor.py   # Threading logic for batch execution
│
├── pattern_helpers/         # Pattern recognition logic
│   ├── base.py              # Base interface for pattern logic
│   ├── one_candle/          # Implementation for 1-candle patterns
│   ├── two_candle/          # Implementation for 2-candle patterns
│   ├── three_candle/        # Implementation for 3-candle patterns
│   ├── distribution/        # Volume-based patterns
│   └── __init__.py
│
├── utils/                   # Shared utility functions
│   └── logger.py
│
├── main.py                  # Entrypoint (threaded executor kicks off here)
└── requirements.txt         # Python dependencies

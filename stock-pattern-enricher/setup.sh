mkdir -p config dao models services pattern_helpers/{one_candle,two_candle,three_candle,distribution} utils

# Config
touch config/config.py

# DAO
touch dao/elastic_interface.py
touch dao/elastic_impl.py

# Models
touch models/stock_data.py

# Services
touch services/pipeline.py
touch services/service.py
touch services/thread_executor.py

# Pattern Helpers
touch pattern_helpers/__init__.py
touch pattern_helpers/base.py
touch pattern_helpers/one_candle/__init__.py
touch pattern_helpers/two_candle/__init__.py
touch pattern_helpers/three_candle/__init__.py
touch pattern_helpers/distribution/__init__.py

# Utils
touch utils/logger.py

# Root-level files
touch main.py
touch requirements.txt

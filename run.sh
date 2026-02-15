#!/bin/bash
set -e

# Add project root to PYTHONPATH
export PYTHONPATH=$(pwd)

echo "Waiting for Elasticsearch..."

until curl -s http://elasticsearch:9200 > /dev/null; do
  sleep 2
done

echo "Elasticsearch is ready!"


echo "Deleting Elasticsearch index..."
curl -XDELETE http://elasticsearch:9200/nifty_data_weekly || true

echo "Running Technical Full Indexing..."
python technical/technicalCharts/fullIndexing.py

echo "Running Pattern Enricher..."
python stock-pattern-enricher/main.py

echo "Running Fundamental Module..."
python fundamental/main.py

echo "Running Filter Stocks..."
python filter_stocks/resistance_support_fundamental_roce.py

echo "Pipeline Completed Successfully!"

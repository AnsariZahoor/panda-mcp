"""
Data Export Utilities
Handles exporting cryptocurrency data to CSV and JSON formats
"""

import csv
import json
import logging
from pathlib import Path
from typing import List, Dict, Optional, Union
from datetime import datetime

logger = logging.getLogger(__name__)


class DataExporter:
    """Utility class for exporting cryptocurrency data to various formats"""

    @staticmethod
    def export_to_json(
        data: Union[List[Dict], Dict],
        file_path: str,
        pretty: bool = True,
        create_dirs: bool = True
    ) -> Dict[str, Union[str, int]]:
        """
        Export data to JSON file

        Args:
            data: Data to export (list of dicts or single dict)
            file_path: Output file path (relative or absolute)
            pretty: Pretty print JSON with indentation (default: True)
            create_dirs: Create parent directories if they don't exist (default: True)

        Returns:
            Dictionary with export details:
                - status: 'success' or 'error'
                - file_path: Full path to exported file
                - records_exported: Number of records exported
                - file_size_bytes: Size of exported file in bytes
                - error: Error message (if status is 'error')

        Example:
            result = DataExporter.export_to_json(
                klines_data,
                'exports/btc_klines.json'
            )
        """
        try:
            # Convert Path object for easier manipulation
            output_path = Path(file_path)

            # Create parent directories if requested
            if create_dirs:
                output_path.parent.mkdir(parents=True, exist_ok=True)

            # Determine record count
            if isinstance(data, list):
                record_count = len(data)
            elif isinstance(data, dict):
                record_count = 1
            else:
                raise ValueError(f"Unsupported data type: {type(data)}")

            # Write JSON file
            with open(output_path, 'w', encoding='utf-8') as f:
                if pretty:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                else:
                    json.dump(data, f, ensure_ascii=False)

            # Get file size
            file_size = output_path.stat().st_size

            logger.info(f"Exported {record_count} records to {output_path} ({file_size} bytes)")

            return {
                "status": "success",
                "file_path": str(output_path.absolute()),
                "records_exported": record_count,
                "file_size_bytes": file_size,
                "format": "json"
            }

        except Exception as e:
            logger.error(f"Failed to export to JSON: {str(e)}")
            return {
                "status": "error",
                "error": str(e),
                "error_type": type(e).__name__
            }

    @staticmethod
    def export_to_csv(
        data: List[Dict],
        file_path: str,
        fieldnames: Optional[List[str]] = None,
        create_dirs: bool = True
    ) -> Dict[str, Union[str, int]]:
        """
        Export data to CSV file

        Args:
            data: List of dictionaries to export
            file_path: Output file path (relative or absolute)
            fieldnames: List of field names to include (default: auto-detect from first record)
            create_dirs: Create parent directories if they don't exist (default: True)

        Returns:
            Dictionary with export details:
                - status: 'success' or 'error'
                - file_path: Full path to exported file
                - records_exported: Number of records exported
                - file_size_bytes: Size of exported file in bytes
                - columns: List of column names
                - error: Error message (if status is 'error')

        Example:
            result = DataExporter.export_to_csv(
                funding_rates,
                'exports/btc_funding.csv'
            )
        """
        try:
            # Validate input
            if not isinstance(data, list):
                raise ValueError("Data must be a list of dictionaries for CSV export")

            if not data:
                raise ValueError("Cannot export empty data to CSV")

            # Convert Path object for easier manipulation
            output_path = Path(file_path)

            # Create parent directories if requested
            if create_dirs:
                output_path.parent.mkdir(parents=True, exist_ok=True)

            # Auto-detect fieldnames from first record if not provided
            if fieldnames is None:
                if not isinstance(data[0], dict):
                    raise ValueError("First element must be a dictionary to auto-detect fieldnames")
                fieldnames = list(data[0].keys())

            # Write CSV file
            with open(output_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(data)

            # Get file size
            file_size = output_path.stat().st_size

            logger.info(f"Exported {len(data)} records to {output_path} ({file_size} bytes)")

            return {
                "status": "success",
                "file_path": str(output_path.absolute()),
                "records_exported": len(data),
                "file_size_bytes": file_size,
                "columns": fieldnames,
                "format": "csv"
            }

        except Exception as e:
            logger.error(f"Failed to export to CSV: {str(e)}")
            return {
                "status": "error",
                "error": str(e),
                "error_type": type(e).__name__
            }

    @staticmethod
    def generate_filename(
        exchange: str,
        data_type: str,
        symbol: Optional[str] = None,
        extension: str = "json",
        include_timestamp: bool = True
    ) -> str:
        """
        Generate a standardized filename for exports

        Args:
            exchange: Exchange name (e.g., 'binance', 'bybit')
            data_type: Type of data (e.g., 'klines', 'funding_rate', 'open_interest')
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            extension: File extension without dot (default: 'json')
            include_timestamp: Include timestamp in filename (default: True)

        Returns:
            Generated filename string

        Example:
            filename = DataExporter.generate_filename(
                'binance', 'klines', 'BTCUSDT', 'csv'
            )
            # Returns: 'binance_klines_BTCUSDT_20250113_143022.csv'
        """
        parts = [exchange, data_type]

        if symbol:
            parts.append(symbol)

        if include_timestamp:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            parts.append(timestamp)

        filename = "_".join(parts) + f".{extension}"
        return filename

    @staticmethod
    def export_auto(
        data: Union[List[Dict], Dict],
        file_path: str,
        create_dirs: bool = True
    ) -> Dict[str, Union[str, int]]:
        """
        Automatically detect format from file extension and export

        Args:
            data: Data to export
            file_path: Output file path with extension
            create_dirs: Create parent directories if they don't exist

        Returns:
            Dictionary with export details (see export_to_json/csv for structure)

        Raises:
            ValueError: If file extension is not supported

        Example:
            result = DataExporter.export_auto(data, 'exports/btc_data.csv')
        """
        file_path_obj = Path(file_path)
        extension = file_path_obj.suffix.lower()

        if extension == '.json':
            return DataExporter.export_to_json(data, file_path, create_dirs=create_dirs)
        elif extension == '.csv':
            if not isinstance(data, list):
                raise ValueError("CSV export requires data to be a list of dictionaries")
            return DataExporter.export_to_csv(data, file_path, create_dirs=create_dirs)
        else:
            raise ValueError(
                f"Unsupported file extension '{extension}'. "
                f"Supported formats: .json, .csv"
            )

import os
import pyodbc
import logging
import time
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


class SQLBaseConnector:
    """Optimized SQL Server database connector with connection pooling and retry logic."""

    def __init__(self):
        """Initialize SQL Server connection parameters from environment variables."""
        # Get and clean server address (remove tcp: prefix if present)
        self.server = os.getenv('DB_SERVER', '').replace('tcp:', '').strip()
        self.database = os.getenv('DB_DATABASE')
        self.username = os.getenv('DB_USER')
        self.password = os.getenv('DB_PASSWORD')

        # Connection settings
        self.driver = '{ODBC Driver 17 for SQL Server}'
        self.connection_timeout = 30  # seconds
        self.command_timeout = 300  # 5 minutes for long queries
        self.max_retries = 3
        self.retry_delay = 1  # seconds

        # Connection state
        self.connection = None
        self._connection_count = 0

        # Validate required parameters
        if not all([self.server, self.database, self.username, self.password]):
            missing = []
            if not self.server: missing.append('DB_SERVER')
            if not self.database: missing.append('DB_DATABASE')
            if not self.username: missing.append('DB_USER')
            if not self.password: missing.append('DB_PASSWORD')
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

        logger.info(f"SQL connector initialized for server: {self.server.split(',')[0]}...")

    def _get_connection_string(self) -> str:
        """Build optimized SQL Server connection string."""
        return (
            f"DRIVER={self.driver};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout={self.connection_timeout};"
        )

    def _ensure_connection(self) -> None:
        """Ensure we have a valid connection, creating one if needed."""
        if self.connection:
            try:
                # Quick check if connection is alive
                cursor = self.connection.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
                return
            except:
                # Connection is dead, close it properly
                try:
                    self.connection.close()
                except:
                    pass
                self.connection = None

        # Create new connection with retry logic
        last_error = None
        for attempt in range(self.max_retries):
            try:
                if attempt > 0:
                    time.sleep(self.retry_delay * attempt)
                    logger.info(f"Retrying connection (attempt {attempt + 1}/{self.max_retries})")

                self.connection = pyodbc.connect(self._get_connection_string())
                self.connection.timeout = self.command_timeout
                self._connection_count += 1
                logger.info(f"Connected to SQL Server (connection #{self._connection_count})")
                return

            except Exception as e:
                last_error = e
                logger.warning(f"Connection attempt {attempt + 1} failed: {str(e)}")

        raise Exception(f"Failed to connect after {self.max_retries} attempts: {last_error}")

    def close(self) -> None:
        """Close the database connection."""
        if self.connection:
            try:
                self.connection.close()
                logger.info("Closed SQL Server connection")
            except:
                pass
            finally:
                self.connection = None

    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute a SELECT query and return results as list of dictionaries."""
        self._ensure_connection()

        with self.connection.cursor() as cursor:
            cursor.execute(query, params or ())

            # Get column names
            columns = [column[0] for column in cursor.description]

            # Fetch all rows and convert to dictionaries
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def execute_update(self, query: str, params: Optional[tuple] = None) -> int:
        """Execute an UPDATE/INSERT/DELETE query and return affected rows."""
        self._ensure_connection()

        with self.connection.cursor() as cursor:
            cursor.execute(query, params or ())
            rowcount = cursor.rowcount
            self.connection.commit()
            return rowcount

    def execute_batch(self, query: str, params_list: List[tuple]) -> int:
        """Execute a query multiple times with different parameters in a single transaction."""
        if not params_list:
            return 0

        self._ensure_connection()

        with self.connection.cursor() as cursor:
            # Use fast_executemany for better performance
            cursor.fast_executemany = True
            cursor.executemany(query, params_list)
            rowcount = cursor.rowcount
            self.connection.commit()
            return rowcount


    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensure connection is closed."""
        self.close()
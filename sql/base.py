# import os
# import logging
# import pyodbc
# from typing import List, Dict, Optional, Any, Tuple
# from contextlib import contextmanager

# logger = logging.getLogger(__name__)


# class SQLBaseConnector:
#     def __init__(self):
#         self.server = os.getenv("DB_SERVER")
#         self.database = os.getenv("DB_DATABASE")
#         self.username = os.getenv("DB_USER")
#         self.password = os.getenv("DB_PASSWORD")
#         self._connection = None

#         if not all([self.server, self.database, self.username, self.password]):
#             raise ValueError(
#                 "Missing required database configuration. Please set DB_SERVER, DB_DATABASE, DB_USER, and DB_PASSWORD environment variables."
#             )

#     def _get_connection_string(self) -> str:
#         return (
#             f"DRIVER={{ODBC Driver 17 for SQL Server}};"
#             f"SERVER={self.server};"
#             f"DATABASE={self.database};"
#             f"UID={self.username};"
#             f"PWD={self.password};"
#             f"Encrypt=yes;"
#             f"TrustServerCertificate=no;"
#             f"Connection Timeout=30;"
#         )

#     def _get_connection(self) -> pyodbc.Connection:
#         if self._connection is None or not self._is_connection_alive():
#             max_retries = 3
#             retry_count = 0

#             while retry_count < max_retries:
#                 try:
#                     self._connection = pyodbc.connect(self._get_connection_string())
#                     logger.debug(f"Connected to SQL Server database: {self.database}")
#                     return self._connection
#                 except pyodbc.Error as e:
#                     retry_count += 1
#                     logger.warning(f"Connection attempt {retry_count} failed: {str(e)}")
#                     if retry_count >= max_retries:
#                         logger.error(
#                             f"Failed to connect to database after {max_retries} attempts"
#                         )
#                         raise

#         return self._connection

#     def _is_connection_alive(self) -> bool:
#         if self._connection is None:
#             return False

#         try:
#             cursor = self._connection.cursor()
#             cursor.execute("SELECT 1")
#             cursor.close()
#             return True
#         except:
#             return False

#     @contextmanager
#     def _get_cursor(self):
#         connection = self._get_connection()
#         cursor = connection.cursor()
#         try:
#             yield cursor
#             connection.commit()
#         except Exception as e:
#             connection.rollback()
#             logger.error(f"Transaction rolled back: {str(e)}")
#             raise
#         finally:
#             cursor.close()

#     def execute_query(
#         self, query: str, params: Optional[Tuple] = None
#     ) -> List[Dict[str, Any]]:
#         logger.debug(f"Executing query: {query[:100]}...")

#         with self._get_cursor() as cursor:
#             if params:
#                 cursor.execute(query, params)
#             else:
#                 cursor.execute(query)

#             columns = [column[0] for column in cursor.description]
#             results = []

#             for row in cursor.fetchall():
#                 results.append(dict(zip(columns, row)))

#             logger.debug(f"Query returned {len(results)} rows")
#             return results

#     def execute_update(self, query: str, params: Optional[Tuple] = None) -> int:
#         logger.debug(f"Executing update: {query[:100]}...")

#         with self._get_cursor() as cursor:
#             if params:
#                 cursor.execute(query, params)
#             else:
#                 cursor.execute(query)

#             rowcount = cursor.rowcount
#             logger.debug(f"Update affected {rowcount} rows")
#             return rowcount

#     def close(self):
#         if self._connection:
#             try:
#                 self._connection.close()
#                 logger.debug("Database connection closed")
#             except Exception as e:
#                 logger.error(f"Error closing database connection: {str(e)}")
#             finally:
#                 self._connection = None

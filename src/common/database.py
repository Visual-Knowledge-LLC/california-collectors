"""
Database connection and utilities for Visual Knowledge collectors.
"""

import os
import logging
from typing import Optional, Dict, Any, List
from contextlib import contextmanager
from datetime import datetime

from sqlalchemy import create_engine, text, pool
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)


class DatabaseConfig:
    """Database configuration management."""

    def __init__(self):
        """Initialize database configuration from environment variables."""
        self.host = os.getenv('DB_HOST', 'localhost')
        self.port = os.getenv('DB_PORT', '5432')
        self.database = os.getenv('DB_NAME', 'vk_production')
        self.username = os.getenv('DB_USER', 'postgres')
        self.password = os.getenv('DB_PASSWORD', '')
        self.pool_size = int(os.getenv('DB_POOL_SIZE', '5'))
        self.max_overflow = int(os.getenv('DB_MAX_OVERFLOW', '10'))

    @property
    def connection_string(self) -> str:
        """Get the database connection string."""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


class DatabaseManager:
    """Manages database connections and operations for collectors."""

    def __init__(self, config: Optional[DatabaseConfig] = None):
        """
        Initialize database manager.

        Args:
            config: Database configuration object
        """
        self.config = config or DatabaseConfig()
        self._engine = None
        self._session_factory = None

    @property
    def engine(self):
        """Get or create the database engine."""
        if self._engine is None:
            self._engine = create_engine(
                self.config.connection_string,
                poolclass=pool.QueuePool,
                pool_size=self.config.pool_size,
                max_overflow=self.config.max_overflow,
                pool_pre_ping=True,  # Verify connections before using
                echo=False  # Set to True for SQL debugging
            )
            logger.info("Database engine created")
        return self._engine

    @property
    def session_factory(self):
        """Get or create the session factory."""
        if self._session_factory is None:
            self._session_factory = sessionmaker(bind=self.engine)
        return self._session_factory

    @contextmanager
    def get_session(self) -> Session:
        """
        Context manager for database sessions.

        Yields:
            SQLAlchemy session object

        Example:
            with db_manager.get_session() as session:
                session.execute(text("SELECT 1"))
        """
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            session.close()

    def test_connection(self) -> bool:
        """
        Test the database connection.

        Returns:
            True if connection is successful, False otherwise
        """
        try:
            with self.get_session() as session:
                result = session.execute(text("SELECT 1"))
                return result.scalar() == 1
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Execute a SQL query.

        Args:
            query: SQL query string
            params: Query parameters

        Returns:
            Query result
        """
        with self.get_session() as session:
            result = session.execute(text(query), params or {})
            return result.fetchall()

    def bulk_insert(
        self,
        table: str,
        records: List[Dict[str, Any]],
        batch_size: int = 1000,
        on_conflict: Optional[str] = None
    ) -> int:
        """
        Perform bulk insert with batching.

        Args:
            table: Table name
            records: List of dictionaries to insert
            batch_size: Number of records per batch
            on_conflict: Optional ON CONFLICT clause

        Returns:
            Number of records inserted
        """
        if not records:
            return 0

        # Get column names from first record
        columns = list(records[0].keys())

        # Build the INSERT query
        placeholders = ", ".join([f":{col}" for col in columns])
        column_names = ", ".join(columns)

        query = f"INSERT INTO {table} ({column_names}) VALUES ({placeholders})"

        if on_conflict:
            query += f" {on_conflict}"

        inserted = 0
        with self.get_session() as session:
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                try:
                    session.execute(text(query), batch)
                    inserted += len(batch)
                    logger.debug(f"Inserted batch of {len(batch)} records")
                except SQLAlchemyError as e:
                    logger.error(f"Batch insert failed: {e}")
                    raise

        logger.info(f"Bulk inserted {inserted} records into {table}")
        return inserted

    def clear_delta_table(self) -> None:
        """Clear the delta_bbb_uploaded_data table."""
        with self.get_session() as session:
            session.execute(text("DELETE FROM delta_bbb_uploaded_data"))
            logger.info("Delta table cleared")

    def merge_delta_to_main(self) -> int:
        """
        Merge delta_bbb_uploaded_data into bbb_uploaded_data.

        Returns:
            Number of records merged
        """
        merge_query = """
        INSERT INTO bbb_uploaded_data (
            uuid, bbb_id, agency_id, business_name, street, city, zip,
            state_established, date_established, license_nbr, agency_url,
            phone_number, license_expiration, license_status, reportable_data,
            agency_name, category
        )
        SELECT
            uuid, bbb_id, agency_id, business_name, street, city, zip,
            state_established, date_established, license_nbr, agency_url,
            phone_number, license_expiration, license_status, reportable_data,
            agency_name, category
        FROM delta_bbb_uploaded_data
        ON CONFLICT (uuid)
        DO UPDATE SET
            bbb_id = EXCLUDED.bbb_id,
            agency_id = EXCLUDED.agency_id,
            business_name = EXCLUDED.business_name,
            street = EXCLUDED.street,
            city = EXCLUDED.city,
            zip = EXCLUDED.zip,
            state_established = EXCLUDED.state_established,
            date_established = EXCLUDED.date_established,
            license_nbr = EXCLUDED.license_nbr,
            agency_url = EXCLUDED.agency_url,
            phone_number = EXCLUDED.phone_number,
            license_expiration = EXCLUDED.license_expiration,
            license_status = EXCLUDED.license_status,
            reportable_data = EXCLUDED.reportable_data,
            agency_name = EXCLUDED.agency_name,
            category = EXCLUDED.category,
            updated_at = CURRENT_TIMESTAMP
        """

        with self.get_session() as session:
            result = session.execute(text(merge_query))
            merged_count = result.rowcount
            logger.info(f"Merged {merged_count} records from delta to main table")
            return merged_count

    def get_record_count(self, table: str, where_clause: Optional[str] = None) -> int:
        """
        Get record count from a table.

        Args:
            table: Table name
            where_clause: Optional WHERE clause

        Returns:
            Record count
        """
        query = f"SELECT COUNT(*) FROM {table}"
        if where_clause:
            query += f" WHERE {where_clause}"

        with self.get_session() as session:
            result = session.execute(text(query))
            return result.scalar()

    def close(self):
        """Close database connections."""
        if self._engine:
            self._engine.dispose()
            logger.info("Database connections closed")


# Singleton instance
_db_manager: Optional[DatabaseManager] = None


def get_db_manager() -> DatabaseManager:
    """
    Get the singleton database manager instance.

    Returns:
        DatabaseManager instance
    """
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager
from typing import Any, Sequence
from sqlalchemy.engine.row import Row
from utils.denodo_connection import DenodoConnection
from datetime import datetime, timedelta, timezone
import logging


class TableStatistics:
    def __init__(self, table_name: str, db_name: str, con: DenodoConnection) -> None:
        """Initialize TabelStatistics instance.

        Args:
            table_name (str): name of table
            db_name (str): denodo vdb name
        """
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.db_name: str = db_name
        self.table_name: str = table_name
        self.con: DenodoConnection = con
        self.last_update: datetime.datetime | None = None
        if self._enabled_statistics():
            self.last_update = self._get_last_updated()
            print(self.table_name, self.last_update)

    def _enabled_statistics(self) -> bool:
        """Check if the table has statistics enabled

        Returns:
            bool: return True if statistics are enabled
        """
        query: str = f"SELECT count(1) from GET_VIEW_STATISTICS() where input_database_name = '{
            self.db_name}' and input_name = '{self.table_name}'"

        results: Sequence[Row[Any]] = self.con.execute_query(query)
        if results[0][0] > 0:
            result: bool = True
        else:
            result: bool = False
        return result

    def _get_last_updated(self) -> datetime | None:
        """Get last updated date for table statistics.

        Args:
            connection (DenodoConnection): DenodoConnection instance

        Returns:
            datetime.datetime | None: last updated statistics
        """
        query: str = f"SELECT min(last_updated) FROM GET_VIEW_STATISTICS() where input_database_name = '{
            self.db_name}' and input_name = '{self.table_name}'"

        results: Sequence[Row[Any]] = self.con.execute_query(query)

        return results[0][0]

    def check_freshness(self, freshness_days: int) -> bool:
        """Check if the table statistics are fresh"""

        if self._enabled_statistics():
            timezone_utc: timezone = timezone.utc
            freshness: bool = (self.last_update.astimezone(timezone_utc) +
                               timedelta(days=freshness_days)) > datetime.now(timezone_utc)
            return freshness

        return True

    def update_statistics(self):
        query: str = f"call get_stats_for_fields('ATSOURCE_THROUGH_VDP_ONLY', null, '{
            self.table_name}', null, True, False, False, True)"
        results: Sequence[Row[Any]] = self.con.execute_query(query)

        self.logger.info(results)

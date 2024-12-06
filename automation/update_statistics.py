from typing import Any, Sequence
from sqlalchemy.engine.row import Row
from utils.denodo_connection import DenodoConnection
from .table_statistics import TableStatistics
import yaml
import logging


def load_config(config_path, logger: logging.Logger):
    """
    Load configuration from a YAML file with detailed error handling.

    :param config_path: Path to the YAML configuration file
    :return: List of database configurations
    """
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)

        # Ensure we have a list of databases
        databases = config.get('databases', [])
        return databases

    except yaml.YAMLError as e:
        logger.error(f"YAML Parsing Error: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error reading configuration: {e}")
        return []


def main() -> None:
    config_path = 'config/statistics.yaml'
    logger: logging.Logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    databases_conf = load_config(config_path, logger)

    if not databases_conf:
        logger.error("No databases found in configuration")
        return

    for db_conf in databases_conf:
        freshness_days: int = db_conf['freshness_in_days']
        excluded_tables = db_conf['exclude_tables']

        # list to comma separated string with quotes for IN clause
        quoted_list: list[str] = [f"'{item}'" for item in excluded_tables]
        excluded_tables: str = ",".join(quoted_list)

    con = DenodoConnection("172.17.0.2", "admin", "admin")
    table_list: list[TableStatistics] = []
    results: Sequence[Row[Any]] = con.execute_query(
        f"select database_name, name from get_views() where name not in ({excluded_tables})")
    for row in results:
        table = TableStatistics(row[1], row[0], con)

        if not table.check_freshness(freshness_days):
            table.update_statistics()
            print(f"Updated statistics for {table.table_name}")
        table_list.append(table)


if __name__ == "__main__":
    main()

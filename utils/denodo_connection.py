import sqlalchemy as con
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import logging


class DenodoConnection:
    def __init__(self, vdp_server: str, user: str, password: str):
        self.vdp_server = vdp_server
        self._connection_string = f"denodo://{user}:{password}@{vdp_server}:9996/admin"

        self.logger = logging.getLogger(__name__)
        self._connect()

    def _connect(self):
        """
    Establish database connection and create session factory.

    Raises:
        SQLAlchemyError: If connection fails
    """
        try:
            self.engine = con.create_engine(self._connection_string)
            self.Session = sessionmaker(bind=self.engine)

            # Test connection
            with self.engine.connect() as connection:
                connection.execute(con.text("SELECT 1"))

                self.logger.info(f"Successfully connected to Denodo database: {self.vdp_server}")

        except SQLAlchemyError as e:
            self.logger.error(f"Connection error: {e}")
            raise

    def execute_query(self, query):
        """
    Execute a SQL query and return results.

    :param query: SQL query string or SQLAlchemy text object
    :param params: Optional dictionary of query parameters
    :return: Query results
    """
        try:
            with self.Session() as session:
                if isinstance(query, str):
                    query = con.text(query)

                result = session.execute(query)
                return result.fetchall()

        except SQLAlchemyError as e:
            self.logger.error(f"Query execution error: {e}")
            raise

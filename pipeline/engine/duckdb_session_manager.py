import logging
import duckdb

log = logging.getLogger(__name__)


class DuckDBSessionManager:

    def __init__(self, db_path: str = ":memory:"):
        self.connection = duckdb.connect(database=db_path)
        self.connection.execute("SET TimeZone = 'UTC'")

    def close(self):
        self.connection.close()

    def query(self, query: str):
        log.debug("running query: %s", query)
        if not self.connection:
            log.warning("no connection")
        else:
            log.debug("connection exists")
        return self.connection.execute(query)

    def get_connection(self):
        return self.connection

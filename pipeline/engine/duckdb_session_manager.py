import duckdb


class DuckDBSessionManager:

    def __init__(self, db_path: str = ":memory:"):
        self.connection = duckdb.connect(database=db_path)
        self.connection.execute("SET TimeZone = 'UTC'")

    def close(self):
        self.connection.close()

    def query(self, query: str):
        print("running query: ", query)
        if not self.connection:
            print("no connection")
        else:
            print("connection exists")
        return self.connection.execute(query)

    def get_connection(self):
        return self.connection

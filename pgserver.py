import psycopg2
from psycopg2.extras import RealDictCursor
import logging

# Class to run a PostgreSQL query and return results as a dictionary
class PostgresQueryRunner:
    def __init__(self, creds, port=5432, logger=None):
       
        self.host = creds.get("host")
        self.database = creds.get("database")
        self.user = creds.get("user")
        self.password = creds.get("password")
        self.port = port
        self.logger = logger

    def run_query(self, select_statement: str, key: str) -> dict:
        """
        Executes a SELECT statement and returns a dictionary with the specified key and list of row values as value.
        """
        conn = None
        result = {}
        try:
            conn = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port
            )
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(select_statement)
                rows = cur.fetchall()
                for row in rows:
                    row_dict = dict(row)
                    row_id = row_dict.get(key)
                    # Remove the specified key from the values list
                    values = [v for k, v in row_dict.items() if k != key]
                    result[row_id] = values
        except Exception as e:
            if self.logger:
                self.logger.error(f"Postgres query failed: {e}")
            else:
                logging.error(f"Postgres query failed: {e}")
        finally:
            if conn:
                conn.close()
        return result
    
    @staticmethod
    def load_db_creds_from_file(file_path: str) -> dict:
    
        import json
        try:
            with open(file_path, 'r') as f:
                creds = json.load(f)
                return creds
        except Exception as e:
            logging.error(f"Failed to load database credentials from file: {e}")
            return {}
    

if __name__ == "__main__":
     # Example usage
    creds = PostgresQueryRunner.load_db_creds_from_file(".DBCreds.json")
    runner = PostgresQueryRunner(creds, logger=logging.getLogger(__name__))
    query = "SELECT title, instance_type, tags FROM steampipe_cache.aws_ec2_instance WHERE title LIKE 'TA-%' AND instance_state = 'running' AND title IN ('TA-SEO-B-07');"
    result = runner.run_query(query, key='title')
    for key, values in result.items():
        print(f"{key}: {values}") 
        for item, value in values[1].items():
            print(f"  - {item}: {value}")  
    
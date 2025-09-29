"""
author: Albert
"""
import psycopg2

class ModuleManager:
    def __init__(self,dbname:str, user:str):
        self.dbname = dbname
        self.user = user
        self.host = "34.73.180.136"
        self.port = 5432
        self.password="Solar"

        try:
            conn = psycopg2.connect(f"dbname={self.dbname} user={self.user} password={self.password} host={self.host} port={self.port}")
            self.cur = conn.cursor()
            print("Connection Successful")
        except psycopg2.DatabaseError as e:
            raise e
    
    def query_modules(self, sql_query):
        try:
            self.cur.execute(sql_query)
            results = self.cur.fetchall() #Gets all rows based on the query
            return results
        except psycopg2.Error as e:
            raise e
    def close(self):
        self.cur.close()
        self.conn.close()

#### Testing
def main():
    module_access = ModuleManager(dbname="fsecdatabase",user="albejojo" )
    query = "SELECT * FROM instrument_data.ir_indoor_metadata LIMIT 8"
    results = module_access.query_modules(query)
    print(row  for row in results)



if __name__ == "__main__":
    main()

"""
--Basic Plan
1. Establish a connection between postgres database ### Connction works
2. Process queries (refer to read_records_from files) ### Currently working on this need to complete a function to process queries.
3. Return subset of data based on identifiers like (module_ids)### Need to add this feature
4. Store the recieved modules to look in an efficient data structure
5. Request the OSN database to return the requested moduele for analysis
"""
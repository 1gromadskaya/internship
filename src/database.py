import mysql.connector
from mysql.connector import Error
import time
import os
from typing import List, Dict, Any, Optional


class Database:
    def __init__(self, host='mysql', database='student_rooms',
                 user='root', password='root'):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.connection = None

    def connect(self) -> bool:
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                charset='utf8mb4',
                collation='utf8mb4_unicode_ci'
            )
            print("Connection to the database is established")
            return True
        except Error as e:
            print(f"Connection error: {e}")
            return False

    def disconnect(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("The connection is closed")

    def execute_sql_file(self, file_path: str) -> bool:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()

            queries = [q.strip() for q in sql_content.split(';') if q.strip()]

            cursor = self.connection.cursor()
            for query in queries:
                if query:
                    try:
                        cursor.execute(query)
                    except Error as e:
                        if e.errno == 1061:
                            print(f"Info: {e.msg} (skipping)")
                else:

                    raise e

            self.connection.commit()
            cursor.close()
            print(f"The SQL file has been executed: {os.path.basename(file_path)}")
            return True

        except Error as e:
            print(f"Error when executing SQL file {file_path}: {e}")
            return False

    def execute_query(self, query: str, params: tuple = None) -> Optional[List[Dict]]:
        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(query, params or ())
            result = cursor.fetchall()
            cursor.close()
            return result
        except Error as e:
            print(f"Request execution error: {e}")
            return None

    def load_data_from_json(self, rooms_data, students_data):
        try:
            cursor = self.connection.cursor()

            print("Clearing existing data...")
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            cursor.execute("DELETE FROM students")
            cursor.execute("DELETE FROM rooms")
            cursor.execute("ALTER TABLE rooms AUTO_INCREMENT = 1")
            cursor.execute("ALTER TABLE students AUTO_INCREMENT = 1")
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

            room_mapping = {}

            for room in rooms_data:
                if 'id' not in room or 'name' not in room:
                    continue

                cursor.execute(
                    "INSERT INTO rooms (name) VALUES (%s)",
                    (room['name'],)
                )
                new_id = cursor.lastrowid
                room_mapping[room['id']] = new_id

            for student in students_data:
                required_fields = ['name', 'sex', 'birthday', 'room']
                if not all(field in student for field in required_fields):
                    continue

                old_room_id = student['room']
                new_room_id = room_mapping.get(old_room_id)

                if new_room_id is None:
                    continue

                cursor.execute(
                    "INSERT INTO students (name, sex, birthday, room) VALUES (%s, %s, %s, %s)",
                    (
                        student['name'],
                        str(student['sex']).upper(),
                        student['birthday'],
                        new_room_id
                    )
                )

            self.connection.commit()
            cursor.close()

            return True

        except Error as e:
            print(f"data loading error: {e}")
            self.connection.rollback()
            return False

    def run_queries_from_file(self, queries_file: str) -> Dict[str, List[Dict]]:
        try:
            with open(queries_file, 'r', encoding='utf-8') as f:
                content = f.read()

            queries = [q.strip() for q in content.split(';') if q.strip()]

            results = {}
            query_counter = 1

            for sql in queries:
                if sql.upper().startswith('SELECT'):
                    query_name = f'query{query_counter}'
                    print(f"execute {query_name}...")

                    result = self.execute_query(sql)

                    if result is not None:
                        results[query_name] = result
                        print(f"Received {len(result)} records")
                        query_counter += 1
                    else:
                        print(f"Error during execution")
            return results

        except Exception as e:
            print(f"Error when reading the query file: {e}")
            return {}
import argparse
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database import Database
from loader import JSONLoader
from exporter import Exporter


def main():
    parser = argparse.ArgumentParser(
        description='load student and room data from json files into mysql and execute queries'
    )

    parser.add_argument('--students', required=True,
                        help='path to json file with student data')
    parser.add_argument('--rooms', required=True,
                        help='path to json file with room data')
    parser.add_argument('--format', choices=['json', 'xml'], default='json',
                        help='output format for results')
    parser.add_argument('--sql-dir', default='/app/sql',
                        help='directory containing sql files')
    parser.add_argument('--output', default='/output/result',
                        help='output file path without extension')

    args = parser.parse_args()

    for file_path in [args.students, args.rooms]:
        if not os.path.exists(file_path):
            print(f"file not found: {file_path}")
            sys.exit(1)

    output_dir = os.path.dirname(args.output)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    try:
        rooms_data = JSONLoader.load(args.rooms)
        students_data = JSONLoader.load(args.students)
        print(f"loaded: {len(rooms_data)} rooms, {len(students_data)} students")
    except Exception as e:
        print(f"error loading json data: {e}")
        sys.exit(1)

    db = Database()
    if not db.connect():
        sys.exit(1)

    try:
        schema_file = os.path.join(args.sql_dir, 'create_tables.sql')
        if os.path.exists(schema_file):
            if not db.execute_sql_file(schema_file):
                sys.exit(1)
        else:
            print(f"sql schema file not found: {schema_file}")
            sys.exit(1)

        if not db.load_data_from_json(rooms_data, students_data):
            sys.exit(1)

        indexes_file = os.path.join(args.sql_dir, 'indexes.sql')
        if os.path.exists(indexes_file):
            db.execute_sql_file(indexes_file)

        queries_file = os.path.join(args.sql_dir, 'queries.sql')
        if not os.path.exists(queries_file):
            print(f"queries file not found: {queries_file}")
            sys.exit(1)

        results = db.run_queries_from_file(queries_file)

        if not results:
            sys.exit(1)

        output_path = f"{args.output}.{args.format}"

        if args.format == 'json':
            Exporter.to_json(results, output_path)
        else:
            Exporter.to_xml(results, output_path)

        print(f"queries executed: {len(results)}")
        print(f"results saved to: {output_path}")

    except Exception as e:
        print(f"error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        db.disconnect()


if __name__ == '__main__':
    main()
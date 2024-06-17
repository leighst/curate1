import argparse

from db import Database


def main():
  parser = argparse.ArgumentParser(description="Hacker News Data Ingestion Tool")
  subparsers = parser.add_subparsers(dest="command", help="Available commands")

  # Command 'db'
  db_parser = subparsers.add_parser('db', help="Database operations")
  db_subparsers = db_parser.add_subparsers(dest="db_command", help="Database commands")

  # Command 'db apply'
  db_push_parser = db_subparsers.add_parser('apply', help="Push data to the database")
  db_push_parser.add_argument('--db-path', type=str, required=False, default="curate1.db", help="Path to the database")
  args = parser.parse_args()

  if args.command == 'db':
    handle_db_command(args.db_command, args)
  else:
    print(f"Invalid command: {args.command}")

  
def handle_db_command(db_command, args): 
  db = Database(args.db_path)
  if db_command == 'apply':
    db.recreate_db()

if __name__ == "__main__":
  main()

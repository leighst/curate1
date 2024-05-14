import argparse
from ingestion import Pipeline
from db import Database

def main():
  parser = argparse.ArgumentParser(description="Hacker News Data Ingestion Tool")
  subparsers = parser.add_subparsers(dest="command", help="Available commands")

  # Command 'start'
  start_parser = subparsers.add_parser('start', help="Start the ingestion process")
  start_parser.add_argument('--start-index', type=int, required=True, help="Start index for ingestion")
  start_parser.add_argument('--end-index', type=int, required=True, help="End index for ingestion")
  start_parser.add_argument('--batch-size', type=int, default=100, help="Batch size for ingestion")
  start_parser.add_argument('--overwrite', action='store_true', help="Overwrite existing data")
  start_parser.add_argument('--db-path', type=str, required=False, default="curate1.db", help="Path to the database")

  # Command 'db'
  db_parser = subparsers.add_parser('db', help="Database operations")
  db_subparsers = db_parser.add_subparsers(dest="db_command", help="Database commands")
  db_push_parser = db_subparsers.add_parser('apply', help="Push data to the database")
  db_push_parser.add_argument('--db-path', type=str, required=False, default="curate1.db", help="Path to the database")

  args = parser.parse_args()

  if args.command == 'start':
    handle_ingest_command(args)
  elif args.command == 'db':
    handle_db_command(args.db_command, args)
  else:
    print(f"Invalid command: {args.command}")

def handle_ingest_command(args):
  db = Database(args.db_path)
  p = Pipeline(db)
  p.load_hackernews_posts(args.start_index, args.end_index, args.batch_size, args.overwrite)

def handle_db_command(db_command, args): 
  db = Database(args.db_path)
  if db_command == 'apply':
    db.recreate_db()

if __name__ == "__main__":
  main()

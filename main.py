import argparse
from ingestion import Pipeline
from db import Database

def main():
  parser = argparse.ArgumentParser(description="Hacker News Data Ingestion Tool")
  subparsers = parser.add_subparsers(dest="command", help="Available commands")

  # Command 'filter'
  filter_parser = subparsers.add_parser('filter', help="Apply filter annotation")
  filter_parser.add_argument('--start-index', type=int, required=True, help="Start index for ingestion")
  filter_parser.add_argument('--end-index', type=int, required=True, help="End index for ingestion")
  filter_parser.add_argument('--batch-size', type=int, default=100, help="Batch size for ingestion")
  filter_parser.add_argument('--parallelism', type=int, default=100, help="Parallelism for ingestion")
  filter_parser.add_argument('--overwrite', action='store_true', help="Overwrite existing data")
  filter_parser.add_argument('--db-path', type=str, required=False, default="curate1.db", help="Path to the database")
  filter_parser.add_argument('--spec', type=str, required=True, help="Filter spec")

  # Command 'load'
  load_parser = subparsers.add_parser('load', help="Start the ingestion process")
  load_subparsers = load_parser.add_subparsers(dest="load_command")
  
  # Command 'load range'
  load_range_parser = load_subparsers.add_parser('range', help="Load range of posts")
  load_range_parser.add_argument('--start-index', type=int, required=True, help="Start index for ingestion")
  load_range_parser.add_argument('--end-index', type=int, required=True, help="End index for ingestion")
  load_range_parser.add_argument('--batch-size', type=int, default=100, help="Batch size for ingestion")
  load_range_parser.add_argument('--parallelism', type=int, default=100, help="Parallelism for ingestion")
  load_range_parser.add_argument('--overwrite', action='store_true', help="Overwrite existing data")
  load_range_parser.add_argument('--db-path', type=str, required=False, default="curate1.db", help="Path to the database")
  load_range_parser.add_argument('--stage', type=str, required=False, default="posts", help="Path to the database")
  
  # Command 'load ids'
  load_ids_parser = load_subparsers.add_parser('ids', help="Load list of post ids")
  load_ids_parser.add_argument('--ids', type=lambda x: [int(i) for i in x.split(',')], help="Comma separated list of post IDs for ingestion")
  load_ids_parser.add_argument('--batch-size', type=int, default=100, help="Batch size for ingestion")
  load_ids_parser.add_argument('--overwrite', action='store_true', help="Overwrite existing data")
  load_ids_parser.add_argument('--db-path', type=str, required=False, default="curate1.db", help="Path to the database")

  # Command 'db'
  db_parser = subparsers.add_parser('db', help="Database operations")
  db_subparsers = db_parser.add_subparsers(dest="db_command", help="Database commands")

  # Command 'db apply'
  db_push_parser = db_subparsers.add_parser('apply', help="Push data to the database")
  db_push_parser.add_argument('--db-path', type=str, required=False, default="curate1.db", help="Path to the database")
  args = parser.parse_args()

  if args.command == 'load':
    handle_ingest_command(args)
  elif args.command == 'filter':
    handle_filter_command(args)
  elif args.command == 'db':
    handle_db_command(args.db_command, args)
  else:
    print(f"Invalid command: {args.command}")

def handle_ingest_command(args):
  db = Database(args.db_path)
  p = Pipeline(db)
  if args.load_command == 'range':
    if args.stage == 'content':
      p.load_hackernews_contents_range(args.start_index, args.end_index, args.batch_size, args.parallelism, args.overwrite)
    elif args.stage == 'posts':
      p.load_hackernews_posts_range(args.start_index, args.end_index, args.batch_size, args.overwrite)
    else:
      print("Invalid stage")  
  elif args.load_command == 'ids':
    p.load_hackernews_posts_by_ids(args.ids, args.batch_size, args.overwrite)

def handle_filter_command(args):
  db = Database(args.db_path)
  p = Pipeline(db)
  p.load_filter_spec(args.spec, args.start_index, args.end_index, args.batch_size, args.parallelism, args.overwrite)
  
def handle_db_command(db_command, args): 
  db = Database(args.db_path)
  if db_command == 'apply':
    db.recreate_db()

if __name__ == "__main__":
  main()

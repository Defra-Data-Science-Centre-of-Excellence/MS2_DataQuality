import main_aux
import json

# Parse Command Line Arguments
args = main_aux.parse_args()
mode = args.mode
configPath = args.config

logger = main_aux.create_logger()

# Load config file
logger.info("Loading config file...")
config = main_aux.load_json_file(args.config)

# Connect to S3

# Traverse S3 directories

# Ingest manifest

# Determine type of file

# Run analysis on file type - compute data quality

# Join with manifest data

# Export CSV locally + upload to S3?


# if __name__ == '__main__':
#     pass

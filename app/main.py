import main_aux
from Crawler import crawler
import os
import pandas as pd
from datetime import datetime

# Parse Command Line Arguments
args = main_aux.parse_args()
mode = args.mode
configPath = args.config

# Configure Logger
logger = main_aux.create_logger()

# Load config file
logger.info("Loading config file...")
config = main_aux.load_json_file(args.config)

# Load companion file
logger.info("Loading companion file...")
companion = main_aux.load_json_file(f"{os.getcwd()}/app/script_companion.json")

# Connect to S3 & compute metadata
logger.debug("Instantiating S3 crawler object...")
s3_crawler = crawler.Crawler(credentials_fp=config['aws_credentials_json_location'],
                             companion=companion)
logger.info("Starting S3 bucket crawler...")
metadata = s3_crawler.create_metadata_for_buckets(config['buckets_to_read'])

# Build metadata Dataframe
logger.debug("Building export Dataframe...")
export_columns = companion["metadata_columns"].values()
export_df = pd.DataFrame(columns=export_columns, data=metadata)

# Export metadata to local file system
if not os.path.exists("./output"):
    os.mkdir("./output")
filename = f"elm-metadata-{datetime.now()}.csv"
logger.info(f"Exporting metadata to local file system as {os.getcwd()}/output/{filename}...")
export_df.to_csv(f"./output/{filename}", index=False)

# TODO Export metadata to S3

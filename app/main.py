import main_aux
from Crawler import crawler
import os
import pandas as pd
from datetime import datetime
from io import StringIO

# Parse Command Line Arguments
args = main_aux.parse_args()
mode = args.mode
configPath = args.config

# Configure Logger
if not os.path.exists("./logs"):
    os.mkdir("./logs")
logger = main_aux.create_logger()

# Load config file
logger.info("Loading config file...")
config = main_aux.load_json_file(args.config)

# Load companion file
logger.info("Loading companion file...")
companion = main_aux.load_json_file(f"{os.getcwd()}/app/script_companion.json")

# Connect to S3 & compute metadata
logger.debug("Instantiating S3 crawler object...")
s3_crawler = crawler.Crawler(logger=logger,
                             credentials_fp=config['aws_credentials_json_location'],
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
filename = f"{config['metadata_file_name']}-{datetime.now()}.csv"
logger.info(f"Exporting metadata to local file system as {os.getcwd()}/output/{filename}...")
export_df.to_csv(f"./output/{filename}", index=False)

# Export metadata to S3
csv_buffer = StringIO()
export_df.to_csv(csv_buffer)
s3_crawler.export_file(bucket=config['bucket_to_write_to'],
                       export_directory=config['metadata_destination_directory'],
                       export_file_name=f"{config['metadata_file_name']}.csv",
                       file_data=csv_buffer)

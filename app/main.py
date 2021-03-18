from main_aux import *
from Crawler import crawler
import os
import pandas as pd
from io import StringIO

# Parse Command Line Arguments
args = parse_args()
mode = args.mode
configPath = args.config

# Configure Logger
logger = create_logger()

# Load config file
logger.info("Loading config file...")
config = load_json_file(args.config)
# Load companion file
logger.info("Loading companion file...")
companion = load_json_file(f"{os.getcwd()}/app/script_companion.json")

# Connect to S3 & compute metadata
logger.debug("Instantiating S3 crawler object...")
s3_crawler = crawler.Crawler(logger, companion)
logger.info("Starting S3 bucket crawler...")
metadata = s3_crawler.create_metadata_for_buckets(config['buckets_to_read'])

# Build metadata Dataframe
logger.debug("Building export Dataframe...")
export_columns = companion["metadata_columns"].values()
export_df = pd.DataFrame(columns=export_columns, data=metadata)
export_df['ID'] = export_df.index + 1

# Export metadata to local file system
write_csv_out(logger, config, export_df)

# Export metadata to S3
csv_buffer = StringIO()
export_df.to_csv(csv_buffer, index=False)
logger.info(f"Exporting metadata to AWS S3...")
s3_crawler.export_file(bucket=config['bucket_to_write_to'],
                       export_directory=config['metadata_destination_directory'],
                       export_file_name=f"{config['metadata_file_name']}.csv",
                       file_data=csv_buffer.getvalue())
logger.info(f"Metadata file successfully uploaded, address of file will be s3://"
            f"{config['bucket_to_write_to']}/"
            f"{config['metadata_destination_directory']}/"
            f"{config['metadata_file_name']}.csv.")

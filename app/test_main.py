from app.Crawler import Crawler
from app.main _aux import create_logger, load_json_file
import os

config = load_json_file(file_path = "./script_companion.json")

logger = create_logger()

c = Crawler(logger = logger, credentials_fp = "C:/Users/beellis/aws_creds.json",
            companion = config)
buckets = ["elms-test-2", "elms-test-1"]
c.create_data_quality_for_buckets(buckets = buckets)
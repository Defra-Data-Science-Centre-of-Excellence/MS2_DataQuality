from app.main_aux import create_logger, load_json_file
from app.Crawler import Crawler

logger = create_logger()

config = load_json_file("./script_companion.json")

c = Crawler(logger = logger, credentials_fp = "C:/Users/beellis/aws_creds.json",
            companion = config)

a = c.create_data_quality_for_bucket(bucket = "elms-test-2")
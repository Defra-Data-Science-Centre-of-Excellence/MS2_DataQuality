import time
from app.Crawler import Crawler

if __name__ == "__main__":
    s = time.time()
    bucket = "elms-test-2"
    c = Crawler(credentials_fp = "C:/Users/beellis/aws_creds.json")
    c.create_metadata_for_bucket(bucket = bucket)
    e = time.time() - s
    print(e)
import time
from app.Crawler import Crawler

if __name__ == "__main__":
    s = time.time()
    c_fp = "some/credentials/file/path.json"
    bucket = "elms-test-2"
    c = Crawler(credentials_fp = c_fp)
    c.create_metadata_for_bucket(bucket = bucket)
    e = time.time() - s
    print(e)
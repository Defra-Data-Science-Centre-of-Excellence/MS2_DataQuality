from app.data_quality import create_dq_reports
import boto3
import json
import geopandas as gpd

with open("C:/Users/beellis/aws_creds.json") as cfp:
    creds = json.load(cfp)

client = boto3.client("s3", aws_access_key_id =creds["aws_access_key_id"],
                      aws_secret_access_key = creds["aws_secret_access_key"])

Bucket = "elms-test-1"
Key = "animal_test/ANIMAL_MOV_CATTLE_DATA.gpkg"

obj = client.get_object(Bucket = Bucket, Key = Key)

gdf = gpd.read_file(obj["Body"])
gdf.to_csv("check.csv")

cols = [col for col in gdf.columns]

a = create_dq_reports(gdf_list = [gdf], file_dict = {"Key": "test/test.csv",
                                                     "LastModified": "01/01/21"})

"""
paginator = client.get_paginator('list_objects')

page_iterator = paginator.paginate(Bucket = "elms-test-1")

contents = [page['Contents'] for page in page_iterator]

for cont in contents:
    for c in cont:
        print(c)
"""
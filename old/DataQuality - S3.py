import boto3
from zipfile import ZipFile
from io import BytesIO
from pandas import read_excel, read_csv
from geopandas import read_file as read_geo

from pprint import pprint


# Params
bucket = 's3-ranch-020'
Bucket = boto3.resource('s3').Bucket(bucket)


# Functions
def obj_is_data(obj):
  s = obj.key.split('/')
  if 2 < len(s):
    if s[1] == 'data' and s[2] != '':
      if obj.key.split('.')[-1] in ['xlsx', 'xls', 'csv', 'gpkg', 'shp']:
        return True
  return False
def _download_folderobj(obj, buffer):
  # zip all files, with the same name and different extensions, from a folder into a memory bytes buffer
  path = '/'.join(obj.key.split('/')[:-1])
  name = obj.key.split('/')[-1].split('.')[0]
  with ZipFile(buffer, 'a') as zf:
    for obj in Bucket.objects.filter(Prefix=path):
      file = obj.key.split("/")[-1]
      if file.startswith(name) and not file.endswith('.zip'):
        with BytesIO() as buffer0:
          Bucket.Object(obj.key).download_fileobj(buffer0)
          zf.writestr(file, buffer0.getvalue())
def obj_to_df(obj, buffer):
  ext = obj.key.split('.')[-1]
  if ext in ['xlsx', 'xls']:
    df = read_excel(buffer.read())
  elif ext in ['csv']:
    df = read_csv(buffer)
  elif ext in ['gpkg']:
    df = read_geo(buffer)
  elif ext in ['shp']:
    with BytesIO as buffer:
      _download_folderobj(obj, buffer)
      df = read_geo(buffer, driver='ESRI Shapefile')
  return df




# Main
i = 0
for obj in Bucket.objects.all():
  if i==10: break
  if not obj_is_data(obj):
    #print(0, f's://{bucket}/{obj.key}')
    pass
  else:
    try:
      data = obj.get()
      df = obj_to_df(obj, data['Body'])
      #output = get_metadata(obj, data, df)
      #open(outputfile, 'a').write(output)
      print(1, f's://{bucket}/{obj.key}', data.ContentLength, sep='\t')
      i += 1
    except:
      print(2, f's://{bucket}/{obj.key}', sep='\t')


# Tests
#exec(f'aws s3 cp {obj} {path} --recursive --acl bucket-owner-full-control')
f = 'Agreement_level_option_uptake/data/Agreeement_level_option_uptake.xls'
f = 'ANIMAL_MOV_CATTLE_DATA/data_dictionary/ANIMAL_MOV_CATTLE_DATA.csv'
f = 'Agricultural_Greenhouse_Gas_concentrations/data/naei_ukdata_20210709132802.csv'
f = 'CSF_Holdingslist/Data/CSFR01_Holdings_list.csv'
f = 'ANIMAL_MOV_CATTLE_DATA/data/ANIMAL_MOV_CATTLE_DATA.gpkg'
f = 'Ancient Woodland England/data/Ancient_Woodland_England.shp'
obj = Bucket.Object(f)
buffer = obj.get()['Body']
buffer = _download_folderobj(obj)
df = read_geo(buffer, driver='')



with BytesIO as buffer:
  _download_folderobj(obj, buffer)
  df = read_geo(buffer, driver='ESRI Shapefile')



import numpy as np

x = np.random.randint(10, size=50)
y = x.unique().sort()

diff = lambda x: x[:-1]-x[1:]
z = diff(y)

# Databricks notebook source
# MAGIC %pip install -q numexpr bottleneck xlrd openpyxl pyreadr geopandas
# MAGIC # numexpr, bottleneck:  speed up numpy.
# MAGIC # xlrd, openpyxl, pyreadr:  read xls, xlsx, rds files.
# MAGIC # geopandas:  [Geo]DataFrames, and installs pandas too.

# COMMAND ----------

root = 's3://s3-ranch-020/'
out = 'data/DataQuality'
#root = '/dbfs/mnt/landingr/General Access/'
#out = '/dbfs/mnt/labr/DSET/DataQuality'
s3 = root.startswith('s3')

path_limit = 0  # max paths to walk through
refresh = False  # don't check old metadata output
recency_timeout = 1  # min mtime-now in hours
max_filesize = 0 * 1024**3  # maximum file size in bytes

banned = [
  '/dbfs/mnt/landingr/General Access/EATrialData/HEM_Tool/renv/',  # Unnecessary Data
  '/dbfs/mnt/landingr/General Access/TheSolent/TheSolent.geojson',  # TypeError len(ds.unique)
]
exts = [
  '.csv',
  '.xls',
  '.xlsx', '.xlsm', '.xltx', '.xltm',
  '.rds',
  '.geojson', '.gpkg',
  '.gml', '.gdb', '.shp', '.zip',
]

# COMMAND ----------

import os, json
from datetime import datetime, timedelta
import pandas as pd
import geopandas as gpd
import pyreadr
if s3:
  from zipfile import ZipFile
  # from fiona.io import ZipMemoryFile
  from io import BytesIO
  import boto3
  bucket = root.split('/')[-2]
  Bucket = boto3.resource('s3').Bucket(bucket)
  S3 = boto3.client('s3')

# COMMAND ----------

def _download_folderobj(path):
  # zip all files, with the same name and different extensions, from a folder into a memory bytes buffer
  obj = Bucket.Object(path)
  root = '/'.join(obj.key.split('/')[:-1])
  name = obj.key.split('/')[-1].split('.')[0]
  buffer = BytesIO()
  with ZipFile('a') as zf:
    for obj in Bucket.objects.filter(Prefix=root):
      file = obj.key.split("/")[-1]
      if file.startswith(name) and not file.endswith('.zip'):
        with BytesIO() as buffer0:
          Bucket.Object(obj.key).download_fileobj(buffer0)
          zf.writestr(file, buffer0.getvalue())
  return buffer

def get_s3buffer(path):
  ext = os.path.splitext(path)[1].lower()
  if ext in ['.gml', '.gdb', '.shp', '.zip']:
    buffer = _download_folderobj(path)
  else:
    buffer = BytesIO(Bucket.Object(path).get()['Body'].read())
  return buffer

# COMMAND ----------

def _paths_generator(root):
  if s3:
    for obj in Bucket.objects.all():
      yield obj.key
  else:
    for path, dirs, files in os.walk(root):
      for file in files:
        yield os.path.join(path, file)

def get_paths(root, exts, path_limit, banned):
  paths_kept = list()
  exts_skipped = set()
  for path in _paths_generator(root):
    if path_limit and path_limit <= len(paths_kept):
      break
    ext = os.path.splitext(path)[1].lower()
    if not ext in exts:
      exts_skipped.add(ext)
    elif path.split('/')[-1].startswith('~$'):
      pass
    elif any(path.startswith(ban) for ban in banned):
      pass
    elif s3 and path.split('/')[1]!='data':
      pass
    else:
      paths_kept.append(path)
  return paths_kept, exts_skipped

# COMMAND ----------

class _s3stat:
  # imitate os.stat
  def __init__(self, path):
    obj = S3.get_object(Bucket=bucket, Key=path)
    self.st_size = obj['ContentLength']
    self.st_mtime = obj['LastModified'].timestamp()

def path2meta(path):
  if s3:
    r = _s3stat(path)
  else:
    r = os.stat(path)
  file = path.replace(root, '')
  return {
    'Dataset Name': file.split('/')[0],
    'Filepath': root + file,
    'File Extension': os.path.splitext(file)[1].lower(),
    'File Size (Bytes)': r.st_size,
    'Date Modified': datetime.fromtimestamp(r.st_mtime).isoformat(' ', 'minutes'),
  }

# COMMAND ----------

def get_df(path):
  ext = os.path.splitext(path)[1].lower()
  # S3 Support by loading file/folder into memory
  if s3:
    obj = get_s3buffer(path)
  else:
    obj = path
  # Load Data
  if ext in ['.csv']:
    df = pd.read_csv(obj, engine='python')
  elif ext in ['.xls']:
    df = pd.read_excel(obj, engine='xlrd')
  elif ext in ['.xlsx', '.xlsm', '.xltx', '.xltm']:
    df = pd.read_excel(obj, engine='openpyxl')
  elif ext in ['.rds']:
    df = pyreadr.read_r(obj)[None]
  elif ext in ['.geojson', '.gpkg']:
    df = gpd.read_file(obj)
  elif ext in ['.gml']:
    df = gpd.read_file(obj, driver='GML')
  elif ext in ['.gdb']:
    df = gpd.read_file(obj, driver='OpenFileGDB')
  elif ext in ['.shp', '.zip']:
    df = gpd.read_file(obj, driver='ESRI Shapefile')
  return df

# COMMAND ----------

def df2meta(df):
  return {
    'Number of Columns': len(df.columns),
    'Number of Rows': len(df),
    'Coordinate Reference System': str(df.crs) if hasattr(df, 'crs') else None,
    'COLUMNS': list(),
  }

# COMMAND ----------

def flatten(obj, yieldIterables=True, Origin=True):
  if yieldIterables and not Origin:
    yield obj
  if hasattr(obj, '__iter__'):
    for o in obj:
      yield from flatten(o, yieldIterables, False)
  elif not yieldIterables:
    yield obj

def ds2meta(ds):
  return {
    'Column Name': ds.name,
    'Data Type':  str(ds.dtype),
    'Unique': len(ds.unique()),
    'Complete': len(ds.dropna()),
    'Contains AlphaNumeric': sum(any(char.isalnum() for char in str(v)) for v in ds),
    'Geometry Types':  None if str(ds.dtype) != 'geometry'
      else ', '.join(set(
        g.type if hasattr(g, 'type')
        else 'unknown'
        for g in flatten(ds, True)
      )),
    'Geometry Validity': None if str(ds.dtype) != 'geometry'
      else sum(
        g.is_valid if hasattr(g, 'exterior')
        else 0
        for g in ds
      ),
    'Geometry Points': None if str(ds.dtype) != 'geometry'
      else sum(
        len(g.exterior.coords.xy[0]) if hasattr(g, 'exterior')
        else 1
        for g in flatten(ds, False)
      ),
  }

# COMMAND ----------

def meta2meta(meta):
  # missing some DAMA dimensions
  n = meta['Number of Rows'] * meta['Number of Columns']
  norm = lambda x: x / n if n != 0 else 1
  unique = 0
  complete = 0
  for row in meta['COLUMNS']:
    unique += row['Unique']
    complete += row['Complete']
  return {
    'Completeness': round(norm(complete), 3),
    'Uniqueness': round(norm(unique), 3),
    #Validity
    #Accuracy
    #Consistency
    #Timeliness
    'Report Time': datetime.now().isoformat(' ', 'minutes'),
  }

# COMMAND ----------

out_root = '/'.join(out.split('/')[:-1])
if not os.path.exists(out_root):
  os.mkdir(out_root)
meta = json.load(open(out+'.json', 'r')) if os.path.exists(out+'.json') and not refresh else dict()
paths, exts_skipped = get_paths(root, exts, path_limit, banned)
fails = []
for i, path in enumerate(paths, 1):
  print(f'{i:>3}/{len(paths)}\t{path}')
  m1 = path2meta(path)
  try:
    if path in meta.keys() and meta[path]['Date Modified'] == m1['Date Modified']:
      raise Exception(f'Not Modified')
    if max_filesize and max_filesize < m1['File Size (Bytes)']:
      raise Exception(f'Too Large: {m1["File Size (Bytes)"]}')
    if timedelta(hours=recency_timeout) < datetime.fromisoformat(m1['Date Modified']) - datetime.now():
      raise Exception(f'Recently Modified: {m1["Date Modified"]}')
    df = get_df(path)
  except Exception as e:
    fails.append((e, path))
  else:
    m1.update(df2meta(df))
    for col in df.columns:
      m1['COLUMNS'].append(ds2meta(df[col]))
    m1.update(meta2meta(m1))
    meta[path] = m1
json.dump(meta, open(out+'.json', 'w'))

# COMMAND ----------

df = pd.merge(
  pd.json_normalize(meta.copy().values()).drop(columns='COLUMNS', axis=1),  # file meta
  pd.json_normalize(meta.values(), record_path='COLUMNS', meta='Filepath'),  # column meta
  on = 'Filepath'
)
first_cols = ['Dataset Name', 'Column Name', 'Filepath', 'File Extension', 'File Size (Bytes)', 'Date Modified', 'Report Time', 'Data Type', 'Number of Columns', 'Number of Rows', 'Completeness', 'Uniqueness', 'Complete', 'Unique', 'Contains AlphaNumeric', 'Coordinate Reference System', 'Geometry Types', 'Geometry Validity', 'Geometry Points']
df = df[first_cols + [col for col in df if col not in first_cols]]
pyreadr.write_rds(out+'.rds', df)

# COMMAND ----------

print( f'\nLengths  Paths:{len(paths)}  Meta:{len(meta)}  Fails:{len(fails)}' )
print( exts_skipped, file=open(out+'_exts.txt', 'w') )
print( *fails, sep='\n', file=open(out+'_fails.txt', 'w') )

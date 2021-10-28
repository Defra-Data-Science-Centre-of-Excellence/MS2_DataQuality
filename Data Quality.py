# Databricks notebook source
! pip install geopandas xlrd openpyxl pyreadr
import os, gc, json
from datetime import datetime, timedelta
import pandas as pd
import geopandas as gpd
import pyreadr

# COMMAND ----------

def _paths_generator(path):
  for root, dirs, files in os.walk(path):
    for file in files:
      yield os.path.join(root, file)

def get_paths(root, exts, path_limit, banned):
  paths_kept = list()
  exts_skipped = set()
  for i, path in enumerate(_paths_generator(root)):
    if path_limit and path_limit < i:
      break
    ext = os.path.splitext(path)[1].lower()
    if not ext in exts:
      exts_skipped.add(ext)
    elif path.split('/')[-1].startswith('~$'):
      pass
    elif any(path.startswith(ban) for ban in banned):
      pass
    else:
      paths_kept.append(path)
  return paths_kept, exts_skipped

# COMMAND ----------

def path2meta(path):
  r = os.stat(path)
  return {
    'Dataset Name': path.replace(root, '').split('/')[0],
    'Filepath': path,
    'File Extension': os.path.splitext(path)[1].lower(),
    'File Size (Bytes)': r.st_size,
    'Date Modified': datetime.fromtimestamp(r.st_mtime).isoformat(' ', 'minutes'),
  }

# COMMAND ----------

def get_df(path):
  # No S3 support, use buffer and zipbuffer
  ext = os.path.splitext(path)[1].lower()
  if ext in ['.csv']:
    df = pd.read_csv(path, engine='python')
  elif ext in ['.xls']:
    df = pd.read_excel(path, engine='xlrd')
  elif ext in ['.xlsx', '.xlsm', '.xltx', '.xltm']:
    df = pd.read_excel(path, engine='openpyxl')
  elif ext in ['.rds']:
    df = pyreadr.read_r(path)[None]
  elif ext in ['.geojson', '.gpkg']:
    df = gpd.read_file(path)
  elif ext in ['.gdb', '.shp', '.zip']:
    df = gpd.read_file(path, driver='ESRI Shapefile')
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

# Params
path_limit = None  # max paths to walk through
recency_timeout = 1  # min mtime-now in hours
max_filesize = 10 * 1024**3  # maximum file size in bytes
refresh = False  # don't check old metadata output
out = '/dbfs/mnt/labr/DSET/DataQuality'  # adds .json and .csv
root = '/dbfs/mnt/landingr/General Access/'
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
  '.gdb', '.shp', '.zip',
]

# Main
meta = json.load(open(out+'.json', 'r')) if os.path.exists(out+'.json') and not refresh else dict()
paths, exts_skipped = get_paths(root, exts, path_limit, banned)
fails = []
for i, path in enumerate(paths, 1):
  break
  if path_limit and path_limit < i:
    break
  print(f'{i:>3}/{len(paths)}\t{path}')
  m1 = path2meta(path)
  try:
    if path in meta.keys() and meta[path]['Date Modified'] == m1['Date Modified']:
      raise Exception(f'Not Modified')
    if max_filesize < m1['File Size (Bytes)']:
      raise Exception(f'Too Large: {m1["File Size (Bytes)"]}')
    if timedelta(hours=recency_timeout) < datetime.fromisoformat(m1['Date Modified']) - datetime.now():
      raise Exception(f'Recently Modified: {m1["Date Modified"]}')
    df = get_df(path)
  except Exception as e:
    fails.append((path, e))
  else:
    m1.update(df2meta(df))
    for col in df.columns:
      m1['COLUMNS'].append(ds2meta(df[col]))
    m1.update(meta2meta(m1))
    meta[path] = m1
json.dump(meta, open(out+'.json', 'w'))

# CSV Output
df = pd.merge(
  pd.json_normalize(meta.copy().values()).drop('COLUMNS', 1),  # file meta
  pd.json_normalize(meta.values(), record_path='COLUMNS', meta='Filepath'),  # column meta
  on = 'Filepath'
)
first_cols = ['Dataset Name', 'Column Name', 'Filepath', 'File Extension', 'File Size (Bytes)', 'Date Modified', 'Report Time', 'Data Type', 'Number of Columns', 'Number of Rows', 'Completeness', 'Uniqueness', 'Complete', 'Unique', 'Contains AlphaNumeric', 'Coordinate Reference System', 'Geometry Types', 'Geometry Validity', 'Geometry Points']
df = df[first_cols + [col for col in df if col not in first_cols]]
df.to_csv(out+'.csv', index=False)

# Output
print( f'\nLengths  Paths:{len(paths)}  Meta:{len(meta)}  Fails:{len(fails)}' )
print( exts_skipped )
#print( *fails, sep='\n' )

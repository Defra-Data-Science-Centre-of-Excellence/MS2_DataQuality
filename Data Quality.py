# Databricks notebook source
# DBTITLE 1,Data Qualities Tool for CDAP
# read metadata.json?
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
    'name': path.replace(root, '').split('/')[0],
    'path': path,
    'ext': os.path.splitext(path)[1].lower(),
    'size': r.st_size,
    'mtime': datetime.fromtimestamp(r.st_mtime).isoformat(' ', 'minutes'),
    'ctime': datetime.fromtimestamp(r.st_ctime).isoformat(' ', 'minutes'),
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
    'ncol': len(df.columns),
    'nrow': len(df),
    'crs': str(df.crs) if hasattr(df, 'crs') else None,
    'Columns': list(),
  }

# COMMAND ----------

def flatten(obj, yieldIterables=False, Origin=True):
  if yieldIterables and not Origin:
    yield obj
  if hasattr(obj, '__iter__'):  # isinstance(obj, collections.Iterable) might be better.
    for o in obj:
      yield from flatten(o, yieldIterables, False)
  elif not yieldIterables:
    yield obj

def ds2meta(ds):
  return {
    'name': ds.name,
    'dtype':  str(ds.dtype),
    'unique': len(ds.unique()),
    'complete': len(ds.dropna()),
    'char1': sum(any(char.isalnum() for char in str(v)) for v in ds),
    'geotypes':  None if str(ds.dtype)!='geometry' else ', '.join(set(g.type for g in flatten(ds, True))),
    'geovalids': None if str(ds.dtype)!='geometry' else sum(g.is_valid for g in ds),
    'geopoints': None if str(ds.dtype)!='geometry' else sum(len(g.exterior.coords.xy[0]) for g in flatten(ds, False)),
  }

# COMMAND ----------

def meta2meta(meta):
  # missing some DAMA dimensions
  n = meta['nrow'] * meta['ncol']
  norm = lambda x: x / n if n != 0 else 1
  unique = 0
  complete = 0
  for row in meta['Columns']:
    unique += row['unique']
    complete += row['complete']
  return {
    'Completeness': norm(complete),
    'Uniqueness': norm(unique),
    #Validity
    #Accuracy
    #Consistency
    #Timeliness
    'Report Time': datetime.now().isoformat(' ', 'minutes'),
  }

# COMMAND ----------

# DBTITLE 1,Main
# Params
path_limit = None  # max paths to walk through
recency_timeout = 0  # min mtime-now in hours
max_filesize = .5 * 1024**3  # maximum file size in bytes
refresh = False  # don't check old metadata output
out = '/dbfs/tmp/dq'  # adds .json and .csv
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
fails, i = [], 0
for path in paths:
  if path_limit and path_limit < i:
    break
  print(f'{i:>3}/{len(paths)}\t{path}')
  m1 = path2meta(path)
  try:
    if not hasattr(meta, m1['path']):
      meta[path] = dict()
    elif meta[path]['mtime'] == m1['mtime']:
      raise Exception(f'Not Modified')
    if max_filesize < m1['size']:
      raise Exception(f'Too Large: {m1["size"]}')
    if timedelta(hours=recency_timeout) < datetime.fromisoformat(m1['mtime']) - datetime.now():
      raise Exception(f'Recently Modified: {m1["mtime"]}')
    df = get_df(path)
  except Exception as e:
    fails.append((path, e))
  else:
    i += 1
    m1.update(df2meta(df))
    for col in df.columns:
      m1['Columns'].append(ds2meta(df[col]))
    m1.update(meta2meta(m1))
    meta[path].update(m1)
json.dump(meta, open(out+'.json', 'w'))

# CSV Output
pd.merge(
  pd.json_normalize(meta.values()).drop('Columns', 1),  # file meta
  pd.json_normalize(meta.values(), record_path='Columns', meta='path'),  # column meta
  on = 'path'
).to_csv(out+'.csv')

# Output
print( f'Lengths  Paths:{len(paths)}  Meta:{len(meta)}  Fails:{len(fails)}' )
print( exts_skipped )
print( *fails, sep='\n' )

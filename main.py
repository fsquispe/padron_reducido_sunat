#!/usr/bin/env python

import os
import shutil
import MySQLdb
from tqdm import tqdm
from local import *

def main():
  filename = os.path.join(CONFIG_TEMP_DIR, 'padron_reducido_ruc.txt')
  filename_zip = os.path.join(CONFIG_TEMP_DIR, 'padron_reducido_ruc.zip')

  if (os.path.exists(CONFIG_TEMP_DIR)):
    shutil.rmtree(CONFIG_TEMP_DIR)

  os.mkdir(CONFIG_TEMP_DIR)
  os.system('wget ' + CONFIG_PADRON_REDUCIDO_URL + ' -O ' + filename_zip)
  os.system('unzip ' + filename_zip + ' -d ' + CONFIG_TEMP_DIR)
  os.remove(filename_zip)

  num_records = sum(1 for line in open(filename, 'r', encoding='ISO-8859-1'))
  num_records -= 1
  db=MySQLdb.connect(
    host=CONFIG_MYSQL_HOST,
    user=CONFIG_MYSQL_USER,
    password=CONFIG_MYSQL_PASS,
    db=CONFIG_MYSQL_DB,
    autocommit=True,
  )

  query_params = "('%s','%s','%s','%s','%s','%s', NOW(), NOW())"

  i = 0
  n = 0
  query_block = ''
  domicilio = ''
  domicilio_lst = []
  with open(filename, 'r', encoding='ISO-8859-1') as f:
    f.readline()
    db.query(CONFIG_MYSQL_DROP_TABLE)
    db.query(CONFIG_MYSQL_CREATE_TABLE)
    for line in tqdm(f, total=num_records):
      i += 1
      n += 1
      l = line.replace("\\", "")
      l = l.replace("'", "\\'")
      lst = l.split('|')
      lst.pop()

      domicilio_lst = [lst[7], lst[8], lst[5], lst[6], lst[13], lst[11], lst[9], lst[10], lst[12], lst[14]]
      domicilio_lst = [w.replace('-', '') for w in domicilio_lst]
      domicilio = ' '.join(domicilio_lst).strip()
      domicilio = ' '.join(domicilio.split())

      if i == 1:
        query_block += (query_params % (lst[0], lst[1], lst[2], lst[3], lst[4], domicilio))
      else:
        query_block += ',' + (query_params % (lst[0], lst[1], lst[2], lst[3], lst[4], domicilio))

      if i == CONFIG_MYSQL_INSERT_GROUP or n == num_records:
        db.query(CONFIG_MYSQL_INSERT_QUERY % query_block)
        i = 0
        query_block = ''
  
  shutil.rmtree(CONFIG_TEMP_DIR)

if __name__ == "__main__":
  main()
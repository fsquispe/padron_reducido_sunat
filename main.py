#!/usr/bin/env python

import sys
import os
import time
from .config import *
from MySQLdb import _mysql
from tqdm import tqdm

def main():
    os.system('mkdir ' + CONFIG_TEMP_DIR)
    filename_zip = CONFIG_TEMP_DIR + 'padron_reducido_ruc.zip'
    with open(filename_zip, "wb") as file:
        response = get(CONFIG_PADRON_REDUCIDO_URL)
        file.write(response.content)

    filename = str(sys.argv[1])
    num_records = sum(1 for line in open(filename, 'r', encoding='ISO-8859-1'))
    num_records -= 1
    db=_mysql.connect(
        host=CONFIG_MYSQL_HOST,
        user=CONFIG_MYSQL_USER,
        passwd=CONFIG_MYSQL_PASS,
        db=CONFIG_MYSQL_DB,
    )
 
    group_insert = CONFIG_MYSQL_INSERT_GROUP

    query_base = """
        INSERT INTO sunat_contribuyente (
            id,
            razon_social,
            estado,
            condicion,
            ubigeo,
            domicilio_fiscal,
            creado,
            modificado)
        VALUES %s;
    """

    query_params = "('%s','%s','%s','%s','%s','%s', NOW(), NOW())"

    i = 0
    n = 0
    query_block = ''
    domicilio = ''
    domicilio_lst = []
    with open(filename, 'r', encoding='ISO-8859-1') as f:
        f.readline()
        db.query("TRUNCATE TABLE sunat_contribuyente;")
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
            query_block += (query_params % (lst[0], lst[1], lst[2], lst[3], lst[4], domicilio))
            if i < group_insert and n < num_records: query_block += ','
            if i == group_insert or n == num_records:
                db.query(query_base % query_block)
                i = 0
                query_block = ''

if __name__ == "__main__":
    main()

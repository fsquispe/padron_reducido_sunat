#!/usr/bin/env python

import sys
import time
import _mysql
from tqdm import tqdm

def main():
    filename = str(sys.argv[1])
    num_records = sum(1 for line in open(filename, 'r', encoding='ISO-8859-1'))
    
    db=_mysql.connect(
        host="localhost",
        user="root",
        passwd="pwd123",
        db="sunat"
    )
    
    group_insert = 5000
    
    query_base = """
        INSERT INTO padron (
            ruc,
            razon_social,
            estado_contribuyente,
            codicion_domicilio,
            ubigeo,
            tipo_via,
            nombre_via,
            codigo_zona,
            tipo_zona,
            numero,
            interior,
            lote,
            departamento,
            manzana,
            kilometro,
            _fts)
        VALUES %s;
    """
    
    query_params = "('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
    
    i = 0
    n = 0
    query_block = ''
    with open(filename, 'r', encoding='ISO-8859-1') as f:
        for line in tqdm(f, total=num_records):
            i += 1
            n += 1
            l = line.replace("\\", "")
            l = l.replace("'", "\\'")
            lst = l.split('|')
            lst.pop()
            query_block += (query_params % (lst[0], lst[1], lst[2], lst[3], lst[4], lst[5], lst[6], lst[7], lst[8], lst[9], lst[10], lst[11], lst[12], lst[13], lst[14], " ".join(lst)))
            if i < group_insert and n < num_records: query_block += ','
            if i == group_insert or n == num_records:
                db.query(query_base % query_block)
                i = 0
                query_block = ''

if __name__ == "__main__":
    main()

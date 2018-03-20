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
    
    query_template = """
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
            kilometro)
        VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');
    """
    
    lst = []
    l = ''
    with open(filename, 'r', encoding='ISO-8859-1') as f:
        for line in tqdm(f, total=num_records):
            l = line.replace("'", "\\'")
            lst = l.split('|')
            db.query(query_template % (lst[0], lst[1], lst[2], lst[3], lst[4], lst[5], lst[6], lst[7], lst[8], lst[9], lst[10], lst[11], lst[12], lst[13], lst[14]))


if __name__ == "__main__":
	main()

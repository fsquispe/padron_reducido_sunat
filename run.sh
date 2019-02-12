#!/bin/sh

source /root/bin/padron_sql/env/bin/activate
mkdir /tmp/padron_sql/

cd /tmp/padron_sql/
wget http://www2.sunat.gob.pe/padron_reducido_ruc.zip
unzip ./padron_reducido_ruc.zip
rm ./padron_reducido_ruc.zip

cd /root/bin/padron_sql/
./main.py /tmp/padron_sql/padron_reducido_ruc.txt

rm -rf /tmp/padron_sql
deactivate
#EOF

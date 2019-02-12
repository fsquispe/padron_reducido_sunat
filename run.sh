#!/bin/sh
mkdir /tmp/padronsql/
cd /tmp/padronsql/
wget http://www2.sunat.gob.pe/padron_reducido_ruc.zip
unzip ./padron_reducido_ruc.zip
rm ./padron_reducido_ruc.zip
#EOF

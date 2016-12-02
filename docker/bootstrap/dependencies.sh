#!/bin/bash -ex

apt-get -y update
apt-get -y install software-properties-common apt-transport-https wget git sudo

# Maven
apt-get -y install maven

# Postgres
add-apt-repository "deb https://apt.postgresql.org/pub/repos/apt/ trusty-pgdg main"
wget --quiet -O - https://postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
apt-get -y update
apt-get -y install postgresql-9.5 postgresql-client-9.5
cp pg_hba.conf /etc/postgresql/9.5/main/
/etc/init.d/postgresql start
sudo -u postgres createuser -s digdag_test
sudo -u postgres createdb -O digdag_test digdag_test

# Python
apt-get -y install python python-pip python-dev
pip install sphinx recommonmark sphinx_rtd_theme

# Minio (S3)
wget -O /usr/local/bin/minio https://dl.minio.io/server/minio/release/linux-amd64/minio
chmod 777 /usr/local/bin/minio

rm -rf /var/lib/apt/lists/*

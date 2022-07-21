# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
import os
from datadiscoverybench.utils import dir_path
import urllib.request
from datadiscoverybench.create_db.create_big_table_from_dresden_parallel import create_db

with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()



setup(
    name='datadiscoverybench',
    version='0.0.1',
    description='DataDiscoveryBenchmark',
    long_description=readme,
    author='Felix Neutatz',
    author_email='neutatz@gmail.com',
    url='https://github.com/LUH-DBS/DataDiscoveryBenchmark',
    license=license,
    install_requires=["numpy",
                      "pandas",
                      "duckdb",
                      "pyarrow",
                      "autogluon"
                      ],
    packages=find_packages(exclude=('tests', 'docs'))
)



if not os.path.isdir(dir_path + '/data'):
    os.mkdir(dir_path + '/data')

if not os.path.isdir(dir_path + '/data/dresden'):
    os.mkdir(dir_path + '/data/dresden')

if not os.path.isdir(dir_path + '/data/dresden/data'):
    os.mkdir(dir_path + '/data/dresden/data')

if not os.path.isdir(dir_path + '/data/dresden/db'):
    os.mkdir(dir_path + '/data/dresden/db')


print("starting download")
if not os.path.isfile(dir_path + '/data/dresden/data/' + 'dwtc-000.json.gz'):
    urllib.request.urlretrieve("http://wwwdb.inf.tu-dresden.de/misc/dwtc/data_feb15/dwtc-000.json.gz", dir_path + '/data/dresden/data/' + 'dwtc-000.json.gz')
print("finished download")
if not os.path.isfile(dir_path + '/data/dresden/db/' + 'alltables.parquet'):
    create_db(dir_path)
print("created duckdb")

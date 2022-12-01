# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

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
    packages=find_packages(exclude=('tests', 'docs'), include=('datadiscoverybench', 'dataxformer_groupc'))
)




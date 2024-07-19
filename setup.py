from setuptools import setup
from db_helper import db_helper

setup(
    name='dbhelper',
    version=db_helper.VERSAO,
    packages=['db_helper'],
    install_requires=['criadorjson', 'pymysql'],
    author='João Guilherme de Oliveira Júnior',
    author_email='joaogojunior@gmail.com',
    description='Classe singleton para conexão em serviços mysql / mariadb.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/joaogojunior/db_helper',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)

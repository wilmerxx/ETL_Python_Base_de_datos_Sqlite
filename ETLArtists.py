import sqlite3
import numpy
import glob
from datetime import datetime
import pandas as pd
import sqlalchemy
import matplotlib.pyplot as plt


'''Procesos de ETL'''

def extraer_database(path):

    motorDB = sqlalchemy.create_engine(path)
    conectarDB = motorDB.connect()

    return motorDB, conectarDB

def extraer_database_nueva(path):
    
    motorDB_nueva = sqlalchemy.create_engine(path)
    conectarDB_nueva = motorDB_nueva.connect()

    return motorDB_nueva, conectarDB_nueva

def extraer_tabla_a_pandas(conectarDB):

    query = '''SELECT Name AS Name_artists 
                FROM artists;'''
    result = conectarDB.execute(query)

    df = pd.DataFrame(result.fetchall())
    df.columns = result.keys()

    return df



def cargar_a_sql(datos, connectar, tabla_sqlite):

    # Procesamiento de completar los valores faltantes
    datos.to_sql(tabla_sqlite, connectar, if_exists='append', index=False)
    connectar.close()
    termino = print("carga terminada")
    return termino

if __name__ == '__main__':
    path = "sqlite:///chinook.db"
    path2 = "sqlite:///DW_sale_Music.db"

    # Extracción
    extraerBD = extraer_database(path)

    #nombre_de_tabla = 'Invoices'
    engine = extraerBD[0]
    extraer = extraer_tabla_a_pandas(engine)


    # carga de los datos
    extraerNueva = extraer_database_nueva(path2)
    datos = extraer
    conectarNuevo = extraerNueva[1]
    tabla_sqlite = "dim_artists"
    cargar_a_sql(datos, conectarNuevo, tabla_sqlite)
    print(extraer)

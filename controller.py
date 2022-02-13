import sqlite3
import numpy
import glob
from datetime import datetime
import pandas as pd
import sqlalchemy
import matplotlib.pyplot as plt


'''Procesos de ETL'''
count =0

def extraer_database(path):

    motorDB = sqlalchemy.create_engine(path)
    conectarDB = motorDB.connect()

    return motorDB, conectarDB

def extraer_database_nueva(path):
    
    motorDB_nueva = sqlalchemy.create_engine(path)
    conectarDB_nueva = motorDB_nueva.connect()

    return motorDB_nueva, conectarDB_nueva

def extraer_tabla_a_pandas(conectarDB):

    query = '''select Name AS Name_genres from genres;'''
    result = conectarDB.execute(query)

    df = pd.DataFrame(result.fetchall())
    df.columns = result.keys()

    return df

def transformar_facturacion_promedio(datos):

    # Cálculo de promedio por País
    df_g = datos.groupby(['BillingCountry'])[['Total']].mean()
    df_g = df_g.reset_index()
    df_g.rename(columns={"Total": "Promedio"}, inplace=True)

    df = datos.merge(df_g, how="left", left_on="BillingCountry",
                     right_on="BillingCountry")

    return df

def transformar_rellenar_nulo(datos):

    # Procesamiento de completar los valores faltantes
    datos = datos.fillna({"BillingState": "NA", "BillingPostalCode": "99999"})

    return datos

def transformar_formato(datos):
    #df = pd.DataFrame({'InvoiceDate': '%d-%m-%Y'})

    datetime.strftime(datos.InvoiceDate, format='%d-%m-%Y')
    return datos

def exportar_csv(archivo_de_destino,df):
  df.to_csv(archivo_de_destino)

def cargar_a_sql(datos, connectar, tabla_sqlite):

    # Procesamiento de completar los valores faltantes

    datos.to_sql(tabla_sqlite, connectar, if_exists='append', index=False)
    connectar.close()
    return 'La carga ha terminado'

if __name__ == '__main__':
    path = "sqlite:///chinook.db"
    path2 = "sqlite:///DW_sale_Music.db"
    #ruta_destino = r'C:\Users\Usuario\Desktop\PROYECTO_MDB\consulta1.csv'
    # Extracción
    extraerBD = extraer_database(path)

    #nombre_de_tabla = 'Invoices'
    engine = extraerBD[0]
    extraer = extraer_tabla_a_pandas(engine)

    # Transformación
    #transformar = transformar_facturacion_promedio(extraer)
    #transformar = transformar_rellenar_nulo(transformar)

    # carga de los datos
    extraerNueva = extraer_database_nueva(path2)
    datos = extraer
    conectarNuevo = extraerNueva[1]
    tabla_sqlite = "dim_genres"
    nombre_tabla = "ETL.csv"
    #exportar_csv(nombre_tabla,datos)
    cargar_a_sql(datos, conectarNuevo, tabla_sqlite)
    print(extraer)

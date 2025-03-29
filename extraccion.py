import pyodbc
import pandas as pd


class Extraccion:
    def __init__(self, server, database, username, password):
        self.conn_str = f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    
    def conectar(self):
        return pyodbc.connect(self.conn_str)
    
    # Cargar las tablas de sql server a un dataframe
    def cargar_tabla_sql(self,tabla):
        conn = self.conectar()#Se conecta a la base
        cur = conn.cursor() # Se crea el cursor    
        # Se verifica si la tabla existe
        cur.execute(f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{tabla}'")
        existe = cur.fetchone()[0]
  
        if existe:
            # Se extrae la tabla en un DataFrame
            query = f"SELECT * FROM {tabla}"
            df = pd.read_sql(query, conn)
            print(f"Tabla '{tabla}' cargada exitosamente.")
            return df
        else:
            print(f"La tabla '{tabla}' no existe en la base de datos.")
            return None

    def extraccion(self):
        print("Iniciando extracción de datos...")
        # Se carga la tabla estudiantes en el dataframe estudiantes
        tabla = 'estudiantes'
        df_estudiantes= self.cargar_tabla_sql(tabla)

        # Se cargar la tabla programas en el dataframe estudiantes
        tabla = 'programas'
        df_programas= self.cargar_tabla_sql(tabla)
        
        print("Extracción completada.")
        return df_estudiantes, df_programas

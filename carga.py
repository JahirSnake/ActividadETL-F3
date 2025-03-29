import pyodbc
import pandas as pd
import transformacion as tra

class Carga:
    def __init__(self, server, database, username, password):
        self.conn_str = f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    
    def conectar(self):
        return pyodbc.connect(self.conn_str)
    
    #FUNCIONES PARA CARGA
    def crear_tabla_sql(self,nombre_tabla, columnas):
        try:
            conn = self.conectar()#Se conecta a la base
            cur = conn.cursor()

            # Verificar si la tabla existe
            cur.execute(f"SELECT COUNT(*) FROM sys.tables WHERE name = '{nombre_tabla}'")
            resultado = cur.fetchone()

            if resultado[0] > 0:
                print(f"La tabla '{nombre_tabla}' ya existe.")
            else:
                # Generar la sentencia SQL para crear la tabla
                columnas_sql = ", ".join([f"[{col}] {tipo}" for col, tipo in columnas.items()])
                sql = f"CREATE TABLE {nombre_tabla} ({columnas_sql})"

                cur.execute(sql)
                print(f"Tabla '{nombre_tabla}' creada exitosamente.")

            conn.commit()
            cur.close()
        except Exception as e:
            print(f"Error al verificar o crear la tabla: {e}")
            conn.rollback()

    #Ahora se deben crear las llaves foraneas para conectar las tablas entre ellas
    def crear_llave_foranea(self, tabla_origen, columna_origen, tabla_destino, columna_destino, nombre_fk=None): 
        try:
            conn = self.conectar()#Se conecta a la base
            cur = conn.cursor()

            # Generar un nombre para la llave foránea si no se proporciona
            if nombre_fk is None:
                nombre_fk = f"FK_{tabla_origen}_{columna_origen}_{tabla_destino}_{columna_destino}"

        
            cur.execute(f"""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
                WHERE CONSTRAINT_TYPE = 'FOREIGN KEY' AND TABLE_NAME = '{tabla_origen}' AND CONSTRAINT_NAME = '{nombre_fk}'
            """)
            existe_fk = cur.fetchone()[0]

            if existe_fk > 0:
                print(f"La llave foránea '{nombre_fk}' ya existe entre '{tabla_origen}.{columna_origen}' y '{tabla_destino}.{columna_destino}'.")
            else:
                # Crear la llave foránea
                sql = f"""
                    ALTER TABLE {tabla_origen}
                    ADD CONSTRAINT {nombre_fk}
                    FOREIGN KEY ({columna_origen})
                    REFERENCES {tabla_destino}({columna_destino})
                """
                cur.execute(sql)
                conn.commit()
                print(f"Llave foránea '{nombre_fk}' creada exitosamente entre '{tabla_origen}.{columna_origen}' y '{tabla_destino}.{columna_destino}'.")

            cur.close()
        except Exception as e:
            print(f"Error al crear la llave foránea: {e}")
            conn.rollback()

    def insertar_datos_sql(self, tabla, df):
        try:
            conn = self.conectar()#Se conecta a la base
            cur = conn.cursor()

            # Verificar si la tabla existe
            cur.execute(f"SELECT COUNT(*) FROM sys.tables WHERE name = '{tabla}'")
            existe_tabla = cur.fetchone()[0]

            if existe_tabla == 0:
                print(f"La tabla '{tabla}' no existe en la base de datos.")
                return

            # Reemplazar valores NaN por None (para SQL NULL)
            df = df.where(pd.notna(df), None)

            # Generar la consulta SQL de inserción con placeholders (?)
            columnas = ", ".join(df.columns)
            placeholders = ", ".join(["?" for _ in df.columns])
            sql = f"INSERT INTO {tabla} ({columnas}) VALUES ({placeholders})"

            # Insertar los datos fila por fila
            for index, row in df.iterrows():
                cur.execute(sql, tuple(row))

            conn.commit()
            print(f"{len(df)} registros insertados en la tabla '{tabla}'.")
    
        except Exception as e:
            print(f"Error al insertar datos en '{tabla}': {e}")
            conn.rollback()

    def filtrar_registrosBD(self, df, tabla_bd, columna_clave):
        #Elimina registros de df que ya existen en la tabla de la base de datos.
        try:
            conn = self.conectar()
            query = f"SELECT {columna_clave} FROM {tabla_bd}"
            df_existentes = pd.read_sql(query, conn)  # Obtener registros existentes
            conn.close()

            # Filtrar registros nuevos
            df_filtrado = df[~df[columna_clave].isin(df_existentes[columna_clave])]

            print(f"Se eliminaron {len(df) - len(df_filtrado)} registros duplicados en '{tabla_bd}'.")
            return df_filtrado
        except Exception as e:
            print(f"Error al filtrar registros en '{tabla_bd}': {e}")
            return df  # En caso de error, retorna el df sin cambios



    def carga(self):
        print("Iniciando carga de datos...")
        transformador = tra.Transformacion()  # Crear una instancia de Transformacion
        df_estudiantes_UAO, df_estudiantes_matriculados, df_programas = transformador.transformacion()

        #df_estudiantes_UAO, df_estudiantes_matriculados, df_programas = tra.transformacion()
        # Se crea la tabla estudiantes_matriculados en la Base de Datos
        tabla = 'estudiantes_matriculados'
        columnas_estudiantesMAT = {
            "Periodo_Reg": "VARCHAR(100) NOT NULL",
            "cohorte": "VARCHAR(100) NOT NULL",
            "Periodo_Academico": "VARCHAR(100) NOT NULL",
            "Tipo_periodo": "VARCHAR(100) NOT NULL",
            "Año": "INT NOT NULL",
            "Codigo_Estudiante": "INT NOT NULL",
            "Cohorte1": "VARCHAR(100) NOT NULL",
            "Tipo_Acceso": "VARCHAR(100) NOT NULL",
            "percod":"INT NOT NULL",
            "persnies": "VARCHAR(100) NOT NULL"      
        }
        self.crear_tabla_sql(tabla, columnas_estudiantesMAT)

        # Se crea la tabla estudiantes_limpio en la BD
        tabla = 'estudiantes_limpio'

        columnas_estudiantes = {
            "Codigo_Estudiante": "INT PRIMARY KEY NOT NULL",
            "Numero_Identificacion": "VARCHAR(100) NOT NULL",
            "Nombre": "VARCHAR(100) NOT NULL",
            "Estrato_Economico": "VARCHAR(100) NOT NULL",
            "Genero": "VARCHAR(100) NOT NULL",
            "Estado_Civil": "VARCHAR(100) NOT NULL"    
        }
        self.crear_tabla_sql(tabla, columnas_estudiantes)

        #Se crea la tabla programas_limpio en la BD				
        tabla = 'programas_limpio'

        columnas_programas = {
            "Periodo_Academico": "VARCHAR(100) NOT NULL",
            "Programa": "VARCHAR(100) NOT NULL",
            "Codigo_SNIES": "INT NOT NULL",
            "Ciclo": "VARCHAR(100) NOT NULL",
            "Facultad": "VARCHAR(100) NOT NULL",
            "Nivel_de_Formacion": "VARCHAR(100) NOT NULL",
            "persnies": "VARCHAR(100) PRIMARY KEY NOT NULL"    
        }
        self.crear_tabla_sql(tabla, columnas_programas)

        # Se crean las llaves entre estudiantes_,impio y estudiantes matriculados
        tabla_origen = 'estudiantes_Matriculados'
        columna_origen = 'persnies'
        tabla_destino = 'programas_limpio'
        columna_destino = 'persnies'
        self.crear_llave_foranea(tabla_origen, columna_origen, tabla_destino, columna_destino)

        # Crear las llaves entre estudiantes_,impio y estudiantes matriculados
        tabla_origen = 'estudiantes_Matriculados'
        columna_origen = 'Codigo_Estudiante'
        tabla_destino = 'estudiantes_limpio'
        columna_destino = 'Codigo_Estudiante'
        self.crear_llave_foranea(tabla_origen, columna_origen, tabla_destino, columna_destino)

        df_estudiantes_UAO = self.filtrar_registrosBD(df_estudiantes_UAO, "estudiantes_limpio", "Codigo_Estudiante")
        df_programas = self.filtrar_registrosBD(df_programas, "programas_limpio", "persnies")
        df_estudiantes_matriculados = self.filtrar_registrosBD(df_estudiantes_matriculados, 'estudiantes_matriculados','percod')

       
        # Ingresar los datos de los dataframes a SQL server por medio de la funcion insertar datos SQL
        tabla1= 'estudiantes_limpio'
        tabla2= 'programas_limpio'
        tabla3= 'estudiantes_matriculados'

        self.insertar_datos_sql(tabla1, df_estudiantes_UAO)
        self.insertar_datos_sql(tabla2, df_programas)
        self.insertar_datos_sql(tabla3, df_estudiantes_matriculados)
   
        print("Carga completada.")

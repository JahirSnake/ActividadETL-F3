import pandas as pd
import extraccion as extr

class Transformacion:
    # Funciones para transformacion:
    def verificar_datos(self, df):
        # se cuentan los valores nulos, duplicados y vacios en el dataframe
        nulos = df.isnull().sum()
        vacios = (df.applymap(lambda x: isinstance(x, str) and x.strip() == '')).sum()
        duplicados = df.apply(lambda col: col.duplicated(keep=False).sum())
   
        # Filtrar solo las columnas con valores nulos o vacíos
        columnas_con_problemas = pd.DataFrame({"Nulos": nulos, "Vacios": vacios, "Duplicados": duplicados})
        columnas_con_problemas = columnas_con_problemas[(columnas_con_problemas["Nulos"] > 0) | (columnas_con_problemas["Vacios"] > 0) |
                                                    (columnas_con_problemas["Duplicados"] > 0)]
    
        if columnas_con_problemas.empty:
            print(f"No hay columnas con valores nulos o vacíos.")
        else:
            print(f"Columnas con valores nulos o vacíos:")
        return columnas_con_problemas

    #Funcion para concatenar dos columnas en una nueva
    def concatenar_col(self,df, col1, col2, nueva_columna):   
        if col1 in df.columns and col2 in df.columns:
            df[nueva_columna] = df[col1].astype(str) + "_" + df[col2].astype(str)
            print(f"Columna '{nueva_columna}' creada exitosamente.")
        else:
            print("Error: Una o ambas columnas no existen en el DataFrame.")
        return df

    # Funcion para eliminar duplicados
    def eliminar_duplicados(self,df, columna):
    
        if columna in df.columns:
            df_limpio = df.drop_duplicates(subset=[columna], keep="first").reset_index(drop=True)
            print(f"Se eliminaron {len(df) - len(df_limpio)} filas duplicadas basadas en la columna '{columna}'.")
            return df_limpio
        else:
            print(f"Error: La columna '{columna}' no existe en el DataFrame.")
            return df


    def transformacion(self):
        #df_estudiantes, df_programas = extr.extraccion()
        extractor = extr.Extraccion("127.0.0.1", "actividadETL", "sa", "***********")
        df_estudiantes, df_programas = extractor.extraccion()
        print("Iniciando transformación de datos...")
    
        # Se verifican los datos de los DF
        self.verificar_datos(df_estudiantes)
        self.verificar_datos(df_programas)

        # Se crea la columna percod que es la llave entre periodo academico y codigo del estudiante en estudiantes
        col1 = 'Periodo_Academico'
        col2= 'Codigo_Estudiante'
        col_nueva = 'percod'
        df_estudiantes = self.concatenar_col(df_estudiantes, col1, col2, col_nueva)

        # Eliminar duplicados en la columna percod, en percod no debe haber duplicados
        columna = 'percod'
        df_estudiantes = self.eliminar_duplicados(df_estudiantes, columna)

        # Se crea la columna persnies que es la llave entre periodo academico y snies del programa en programas 
        col1 = 'Periodo_Academico'
        col2= 'Codigo_SNIES'
        col_nueva = 'persnies'
        df_programas = self.concatenar_col(df_programas, col1, col2, col_nueva)

        # Eliminar duplicados en la columna persnies, en persnies no deben haber duplicados
        columna = 'persnies'
        df_programas = self.eliminar_duplicados(df_programas, columna)

        # Se crea el dataframe estudiantes_UAO a partir de dataframe df_estudiantes con los estudiantes por codigo, 
        # de forma descendente, para eliminar duplicados y que los datos de estudiantes queden los mas actuales 

        df_estudiantes_UAO = df_estudiantes[['Codigo_Estudiante','Numero_Identificacion','Nombre',
                                        'Estrato_Economico','Genero','Estado_Civil']].sort_index(ascending=False)

        # Eliminar duplicados en la columna codigo_estudiante
        columna = 'Codigo_Estudiante'
        df_estudiantes_UAO = self.eliminar_duplicados(df_estudiantes_UAO, columna)

        #crear columna persnies en estudiantes
        col1 = 'Periodo_Academico'
        col2= 'programa'
        col_nueva = 'persnies'
        df_estudiantes = self.concatenar_col(df_estudiantes,col1,col2,col_nueva)

        #Se crea el datafrae estudiantes matriculados
        df_estudiantes_matriculados = df_estudiantes[['Periodo Reg','cohorte','Periodo_Academico','Tipo_periodo',
                                                  'Año','Codigo_Estudiante','Cohorte1','Tipo_Acceso','percod','persnies']]
    
        #Para poder ingresar los datos en el sql, los nombres en las columnas de los dataframes no deben tener 
        # espacios por lo que se reemplazan las columnas con espacios en el nombre 
        df_estudiantes_matriculados.rename(columns = {'Periodo Reg':'Periodo_Reg'}, inplace=True)

        print("Transformación completada.")
        return df_estudiantes_UAO, df_estudiantes_matriculados, df_programas


  

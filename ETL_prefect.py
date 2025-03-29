from prefect import flow, task
from extraccion import Extraccion
from transformacion import Transformacion
from carga import Carga

# Configuración de la base de datos
SERVER = "127.0.0.1"
DATABASE = "actividadETL"
USERNAME = "sa"
PASSWORD = "************"

# Tarea de Extracción
@task
def extraccion():
    print("Iniciando extracción de datos...")
    extractor = Extraccion(SERVER, DATABASE, USERNAME, PASSWORD)
    extractor.extraccion()
    print("***Extracción completada***")
    return

# Tarea de Transformación
@task
def transformacion():
    print("Iniciando transformación de datos...")
    trf= Transformacion()
    trf.transformacion()
    print("***Transformación completada***")
    return 

# Tarea de Carga
@task
def carga():
    print("Iniciando carga de datos...")
    cargador = Carga(SERVER, DATABASE, USERNAME, PASSWORD)
    cargador.carga()
    
    print("***Carga completada***")

# Definir Flujo Principal
@flow
def etl():
    extraccion()
    transformacion()
    carga()

# Ejecutar el flujo
if __name__ == "__main__":
    etl()

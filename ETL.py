import os
import csv
import pandas as pd
import psycopg2

# Probar version de pandas: print(pd.version)
#Datos del ususario al que me voy a conectar
dbname = "Syslog"
user = "postgres"
password = "1234"
host = "127.0.0.1"
port = "5432"


#Establecer la conexion:
try:
 conn = psycopg2.connect(
  dbname=dbname,
  user=user,
  password=password,
  host=host,
  port=port, 
 )
 
 print ("Conexion exitosa")

 #Proceso de querys(consultas):
 #cur se utiliza para generar comandos sql
 cur = conn.cursor()

 #Ejecutar la consulta sql para filtrar los datos:
 query_connection = "SELECT id, receivedat, message FROM systemevents WHERE message LIKE %s OR message LIKE %s;"
 pattern_connection = ('%Built inbound TCP connection%',)  # Patrón para buscar en el mensaje

 cur.execute(query_connection, (pattern_connection,))

 #Obtener los resultados
 rows_connection = cur.fetchall()

 #Ejecutar la consulta sql para filtrar los datos:
 query_desconnection = "SELECT id, receivedat, message FROM systemevents WHERE message LIKE %s OR message LIKE %s;"
 pattern_desconnection = ('%Teardown TCP connection%',)  # Patrón para buscar en el mensaje

 cur.execute(query_desconnection, (pattern_desconnection,))

 rows_disconnection = cur.fetchall()

 # Combinar los resultados de eventos "Built" y "Teardown"
 all_rows = []
 all_rows.extend(rows_connection)
 all_rows.extend(rows_disconnection)

 # Escribir los resultados en un archivo CSV
 with open('resultados_filtrados.csv', 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
        
    # Escribir el encabezado del archivo CSV
    csv_writer.writerow(['Usuario', 'Fecha', 'Mensaje', 'Tipo de evento'])
        
    # Escribir los datos filtrados en el archivo CSV
    for row in all_rows:
        csv_writer.writerow(row)
 

 #Cerrar conexiòn
 cur.close()
 conn.close()

 print("Datos filtrados guardados en resultados_filtrados.csv")

except psycopg2.Error as e:
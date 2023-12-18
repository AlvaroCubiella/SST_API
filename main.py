import os
import sys
import re
import time
import threading
import datetime
import schedule

from fastapi import FastAPI
import sst_server
global carpetas

estaciones_pub = {}
estaciones = {}

app = FastAPI()
ftp = sst_server.SST_Servidor(
    servidor='ftp.inidep.edu.ar',
    usuario='oceanografia',
    psw='oceanus',
    root='/TSM'
)


# Función para actualizar los datos en el servidor FTP
def actualizar_ftp_publico():
    global estaciones_pub
    # Conectar al servidor FTP
    ftp.Conectar()
    fechahora = datetime.datetime.now()
    fechahoy = datetime.date.today()
    # Lógica para actualizar la estructura de carpetas
    carpetas = ftp.GetFolders()
    ftp_file_json = ftp.ReadFile(ftp.root, file='folders.json')
    archivos_estaciones = {}
    for i in set(ftp_file_json['Publicas']):
        archivos_estacion = ftp.GetFiles(i)
        estacion = ftp_file_json['Publicas'][i]
        sensor = estacion.get('Sensor')
        ultimo_archivo = archivos_estacion[-1]
        fecha_ultimo_archivo = archivos_estacion[-1].replace(i, '')
        fecha_ultimo_archivo = fecha_ultimo_archivo.replace(sensor, '')
        fecha_ultimo_archivo = fecha_ultimo_archivo.replace('.txt', '')
        scans = ftp.ReadFiletxt(
            f"{ftp.root}/{i}", file=ultimo_archivo)
        ultimo_scan = scans[-1]
        # (?P<hora>\d{2}):(?P<minuto>\d{2}):(?P<segundo>\d{2})")
        patron = re.compile(
            "(?P<scan>\d{1,4}),(?P<dia>\d{2})[/](?P<mes>\d{2})[/](?P<anio>\d{4}) (?P<hora>\d{2}):(?P<minuto>\d{2}):(?P<segundo>\d{2},(?P<temp>\d{1,2}).(?P<temp_dec>\d{1,3}))")
        scan = re.search(patron, ultimo_scan)
        if scan:
            # Convertir la cadena a objeto de fecha
            fecha_cadena = datetime.datetime.strptime(
                fecha_ultimo_archivo, "%Y%m%d").date()
            # Verificar si es el mismo día que hoy
            if fecha_cadena == fechahoy:
                update_scan = True
                fecha_scan = f"{scan.groupdict().get('dia')}/{scan.groupdict().get('mes')}/{scan.groupdict().get('anio')}"
                hora_scan = f"{scan.groupdict().get('hora')}:{scan.groupdict().get('minuto')}"
                dato_scan = f"{scan.groupdict().get('temp')}.{scan.groupdict().get('temp_dec')}"
            else:
                update_scan = False
                fecha_scan = f"{scan.groupdict().get('dia')}/{scan.groupdict().get('mes')}/{scan.groupdict().get('anio')}"
                hora_scan = f"{scan.groupdict().get('hora')}:{scan.groupdict().get('minuto')}"
                dato_scan = f"{scan.groupdict().get('temp')}.{scan.groupdict().get('temp_dec')}"
        else:
            update_scan = False
            fecha_scan = f"NaN"
            hora_scan = f"NaN"
            dato_scan = f"NaN"

        scan = {
            "estado": update_scan,
            "fecha": fecha_scan,
            "hora": hora_scan,
            "dato": dato_scan
        }

        estaciones_pub[i] = estacion
        estaciones_pub[i]['date_update'] = f"{fechahora.day}/{fechahora.month}/{fechahora.year} {fechahora.hour}:{fechahora.minute}"
        estaciones_pub[i]['info'] = scan

    # Cerrar la conexión FTP
    ftp.close()
    print(f"{fechahora}: Public Actualizado")


def actualizar_ftp_admins():
    global estaciones
    # Conectar al servidor FTP
    ftp.Conectar()
    fechahora = datetime.datetime.now()
    fechahoy = datetime.date.today()
    # Lógica para actualizar la estructura de carpetas
    carpetas = ftp.GetFolders()
    ftp_file_json = ftp.ReadFile(ftp.root, file='folders.json')
    archivos_estaciones = {}
    for k in set(ftp_file_json):
        for i in set(ftp_file_json[k]):
            archivos_estacion = ftp.GetFiles(i)
            estacion = ftp_file_json[k][i]
            sensor = estacion.get('Sensor')
            ultimo_archivo = archivos_estacion[-1]
            fecha_ultimo_archivo = archivos_estacion[-1].replace(i, '')
            fecha_ultimo_archivo = fecha_ultimo_archivo.replace(sensor, '')
            fecha_ultimo_archivo = fecha_ultimo_archivo.replace('.txt', '')
            scans = ftp.ReadFiletxt(
                f"{ftp.root}/{i}", file=ultimo_archivo)
            ultimo_scan = scans[-1]
            patron = re.compile(
                "(?P<scan>\d{1,4}),(?P<dia>\d{2})[/](?P<mes>\d{2})[/](?P<anio>\d{4}) (?P<hora>\d{2}):(?P<minuto>\d{2}):(?P<segundo>\d{2},(?P<temp>\d{1,2}).(?P<temp_dec>\d{1,3}))")
            scan = re.search(patron, ultimo_scan)
            if scan:
                # Convertir la cadena a objeto de fecha
                fecha_cadena = datetime.datetime.strptime(
                    fecha_ultimo_archivo, "%Y%m%d").date()
                # Verificar si es el mismo día que hoy
                if fecha_cadena == fechahoy:
                    update_scan = True
                    fecha_scan = f"{scan.groupdict().get('dia')}/{scan.groupdict().get('mes')}/{scan.groupdict().get('anio')}"
                    hora_scan = f"{scan.groupdict().get('hora')}:{scan.groupdict().get('minuto')}"
                    dato_scan = f"{scan.groupdict().get('temp')}.{scan.groupdict().get('temp_dec')}"
                else:
                    update_scan = False
                    fecha_scan = f"{scan.groupdict().get('dia')}/{scan.groupdict().get('mes')}/{scan.groupdict().get('anio')}"
                    hora_scan = f"{scan.groupdict().get('hora')}:{scan.groupdict().get('minuto')}"
                    dato_scan = f"{scan.groupdict().get('temp')}.{scan.groupdict().get('temp_dec')}"
            else:
                update_scan = False
                fecha_scan = f"NaN"
                hora_scan = f"NaN"
                dato_scan = f"NaN"

            scan = {
                "estado": update_scan,
                "fecha": fecha_scan,
                "hora": hora_scan,
                "dato": dato_scan
            }

            estaciones[i] = estacion
            estaciones[i]['date_update'] = f"{fechahora.day}/{fechahora.month}/{fechahora.year} {fechahora.hour}:{fechahora.minute}"
            estaciones[i]['info'] = scan
    # Cerrar la conexión FTP
    ftp.close()
    print(f"{fechahora}: Admins Actualizado")

# Actualizar cada hora


def programar_actualizacion():
    schedule.every(60).minutes.do(actualizar_ftp_publico)
    schedule.every(10).minutes.do(actualizar_ftp_admins)
    while True:
        schedule.run_pending()


# Ruta para acceder manualmente y actualizar las estaciones publicas FTP
@app.get("/actualizar-ftp")
async def manual_update():
    global estaciones_pub
    return {
        "message": f"Estado actualizado de las estaciones SST en el servidor FTP",
        "estaciones": f"{estaciones_pub}",
    }


# Ruta para acceder manualmente y actualizar las estaciones FTP
@app.get("/actualizar-ftp_admins")
async def admins_update():
    global estaciones
    return {
        "message": f"Estado actualizado de las estaciones SST en el servidor FTP",
        "estaciones": f"{estaciones}",
    }


@app.get("/")
async def root():
    return {"message": "Bienvenido!!!"}

# Ejecutar la función de actualización cada hora en un hilo separado
actualizar_ftp_publico()
actualizar_ftp_admins()
t = threading.Thread(target=programar_actualizacion)
t.start()

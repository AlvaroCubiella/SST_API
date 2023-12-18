import os
import io
import sys
import logging
import datetime
import json

from ftplib import FTP


class SST_Servidor():
    def __init__(self, servidor, usuario, psw, root):
        self.servidor = servidor
        self.usuario = usuario
        self.psw = psw
        self.root = root
        self._status = False

    def Conectar(self):
        try:
            self._ftp = FTP(self.servidor)  # Configuro direccion fttp
            # Realizo el login al servidor fttp
            self._ftp.login(self.usuario, self.psw, self.servidor)
            self._ftp.cwd(self.root)
        except Exception as e:
            print(e)
            logging.critical(f"{e}")
            self._status = False  # Imprimo el error que dió
        else:
            self._status = True
            logging.info('Conexion exitosa')

    def __chk_carpeta(self, carpeta):
        self._ftp.cwd(self.estacion)

    @property
    def GetStatus(self):
        return self._status

    def close(self):
        self._ftp.quit()

    def GetFolders(self):
        if not self._status:  # Devuelve lista de las carpetas en el FTTP
            self.Conectar()
        root_list = self._ftp.nlst()
        return root_list

    def GetFiles(self, path='MdP', ext_file='txt'):
        if not self._status:  # Devuelve lista de las carpetas en el FTTP
            self.Conectar()
        self._ftp.cwd(f"/{self.root}/{path}")
        file_list = self._ftp.nlst()
        file_list = [file for file in file_list if file.endswith(ext_file)]
        file_list = sorted(file_list)
        return file_list

    def GetFile(self, path, file):
        if not self._status:  # Devuelve lista de las carpetas en el FTTP
            self.Conectar()
        if not path.startswith("/"):
            path = f"/{path}"
        self._ftp.cwd(path)
        file_ftp = self._ftp.nlst(file)
        return file_ftp

    def ReadFile(self, path, file):
        if not self._status:  # Devuelve lista de las carpetas en el FTTP
            self.Conectar()
        if not path.startswith("/"):
            path = f"/{path}"
        ruta_archivo = f"{path}/{file}"
        # Crear un objeto en memoria para guardar el contenido del archivo
        contenido = io.BytesIO()

        # Descargar el contenido del archivo JSON en el objeto en memoria
        self._ftp.retrbinary('RETR ' + ruta_archivo, contenido.write)

        # Convertir el contenido descargado a un diccionario Python
        contenido.seek(0)  # Posicionar el puntero al principio del archivo
        contenido_str = contenido.read().decode(
            'utf-8')  # Decodificar el contenido a texto
        diccionario_json = json.loads(contenido_str)
        return diccionario_json

    def ReadFiletxt(self, path, file):
        if not self._status:  # Devuelve lista de las carpetas en el FTTP
            self.Conectar()
        if not path.startswith("/"):
            path = f"/{path}"
        ruta_archivo = f"{path}/{file}"
        # Crear un objeto en memoria para guardar el contenido del archivo
        contenido = io.BytesIO()

        # Descargar el contenido del archivo JSON en el objeto en memoria
        self._ftp.retrbinary('RETR ' + ruta_archivo, contenido.write)

        # Convertir el contenido descargado a un diccionario Python
        contenido.seek(0)  # Posicionar el puntero al principio del archivo
        contenido_txt = contenido.read().decode(
            'utf-8')  # Decodificar el contenido a texto
        lineas = contenido_txt.strip().split('\n')
        return lineas


"""
# ARchivo a buscar
fecha = datetime.datetime.now().date()

file_search = f"{estacion}{sensor}{fecha.year}{fecha.month}{fecha.day}.txt"
#-------------------------------------------------------------------------------
#-- Inicio la conexión al FTP
#-------------------------------------------------------------------------------

    #Subo el arvhivo
    ftp.cwd(carpeta)                                                  #Sí existe entonces entro a ella
    folder_list = ftp.nlst()
    if not(estacion in folder_list):                                  #Verificio que existe la carpeta 'SCT'
        logging.critical(f"No se encontro {estacion} en la carpeta {carpeta} del {servidor}")
        ftp.quit()
        logging.info('Desconectado del servidor')
        sys.exit(0)
    ftp.cwd(estacion)    
    file_list = ftp.nlst() 
    if not (file_search in file_list):                                         #Sí no existe entonces manod mensaje
        logging.error(f"No se encontro el archivo {file_search} en {carpeta}")
        ftp.quit()
        print('Envio mensaje bot')
    ftp.quit()
    logging.info('Desconectado del servidor')
"""

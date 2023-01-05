#!/usr/bin/python

from datetime import datetime
import re
import os
import psycopg2.extras
import xlsxwriter
import urllib.request
import urllib.parse
import urllib.error
from requests.auth import HTTPBasicAuth
import requests
import json
import base64
from io import BytesIO
from PIL import Image


class GerarLotesExcel:

    dbcursor = None
    list_db = {}

    lote_dir = None

    def setListConnections(self):
        """ Azure CFT """

        self.list_db['CFT'] = {"user": "",
                               "password": "",
                               "host": "",
                               "port": "",
                               "database": ""}

    def connectionDB(self, name_connection):

        connection = psycopg2.connect(**self.list_db[name_connection])
        self.dbcursor = connection.cursor()

        # Print PostgreSQL version
        self.dbcursor.execute("SELECT version();")
        record = self.dbcursor.fetchone()
        print("You are connected to - ", record, "\n")

        # Print PostgreSQL version
        self.dbcursor.execute(
            "BEGIN; SET SESSION STATEMENT_TIMEOUT TO 0; COMMIT; ")

    def getFoto(self, cpf):
        try:
            if os.path.exists(f"{self.lote_dir}/fotos/{cpf}.jpeg"):
                with open(f"{self.lote_dir}/duplicados.txt", 'a', encoding='utf-8') as f:
                    f.write(f"{cpf}\n")
                return None

            urllib.request.urlretrieve(
                f"https:url/{cpf}.jpeg", filename=f"{self.lote_dir}/fotos/{cpf}.jpeg")

            return True
        except Exception as e:
            with open(f"{self.lote_dir}/log_sem_foto.txt", 'a', encoding='utf-8') as f:
                    f.write(f"{cpf}\n")
            return None

    def getQRCode(self, pessoa_id, cpf):
        # Baixando a foto
        try:
            request = requests.get('https://url{}'.format(
                pessoa_id),  auth=HTTPBasicAuth('user', 'password'), timeout=60000)
            todos = json.loads(request.content)

            base64_img = todos['qrcode']

            name_file_jpg = f"{self.lote_dir}/qrcodes/{cpf}.jpg"

            # Base64 para Arquivo
            base64_img_bytes = base64_img.encode('utf-8')
            decoded_image_data = base64.decodebytes(base64_img_bytes)
            Image.open(BytesIO(decoded_image_data)).convert(
                'RGB').save(name_file_jpg)

            print("QrCode Gerado {}".format(pessoa_id))
        except:
            print("Erro ao gerar o QrCode")

    def getProfissional(self, cpf):

        sql = """
        SELECT 
            rep.filial AS "REGIONAL", 
            rep.nome AS "NOME", 
            to_char(rep.data_ini_registro, 'DD/MM/YYYY') AS "DATA DE REGISTRO", 
            rep.titulos AS "TITULO", 
            rep.ultimoexerciciopago AS "ULTIMO EXERCÌCIO PAGO",
            rep.rnp AS "REGISTRO",
            rep.cpf AS "CPF",
            pro.identidade AS "RG",
            pro.nome_mae AS "NOME DA MÃE",
            pro.nome_pai AS "NOME DO PAI",
            to_char(pro.data_nascimento, 'DD/MM/YYYY') AS "DATA DE NASCIMENTO", 
            pro.nacionalidade AS "NACIONALIDADE",
            pro.naturalidade AS "NATURALIDADE",
            pro.uf_naturalidade AS "UF NATURALIDADE",
            rep.pessoa_id
            
        FROM relatorios.tb_profissional_report rep
        INNER JOIN	public.tb_profissional pro ON pro.pessoa_id	= rep.pessoa_id

        WHERE rep.cpf = '{}'
        """.format(cpf)

        self.dbcursor.execute(sql)
        fetch = self.dbcursor.fetchone()
        colnames = [desc[0] for desc in self.dbcursor.description]
        return {'columns': colnames, 'fetch': fetch}

    def clearCPF(self, cpf):
        regex = re.compile("[^\d]")
        return re.sub(regex, "", cpf)

    def listCPFLote(self):

        with open("lote_cpf.txt") as f:
            content_file = f.read()
            content_file = content_file.split("\n")
            return content_file

    def writeXLS(self, contentList):
        # Gerar o XLS
        workbook = xlsxwriter.Workbook(f'{self.lote_dir}/Lote.xlsx')
        worksheet = workbook.add_worksheet()

        for n_row, data in enumerate(contentList):
            worksheet.write_row(n_row, 0, data)
        workbook.close()

        print("\n# Arquivo XLS Gravado")

    def checkPaths(self):

        if not os.path.exists('qrcodes'):
            os.makedirs(f'{self.lote_dir}/qrcodes')

        if not os.path.exists('fotos'):
            os.makedirs(f'{self.lote_dir}/fotos')

    def __init__(self):

        # Diretorio Lote
        self.lote_dir = "lote_sgd_4887"

        # Conectando ao banco
        self.setListConnections()
        self.connectionDB("CFT")
        self.checkPaths()

        # Variaveis
        listaProfData = []
        listaProfLote = self.listCPFLote()

        # Varrer Lista de CPFs e Obter dados do Banco
        for num, prof in enumerate(listaProfLote):

            # Limpando CPF
            cpf = self.clearCPF(prof)

            # Obtendo Dados
            profData = self.getProfissional(cpf)

            if profData["fetch"]:

                print("#{} - Verificando o CPF: {}".format(num+1, cpf))

                # A partir do primeiro resultado - Obtendo o Nomes da Colunas e adiciono como Resultado
                if(num == 0):
                    colNames = list(profData["columns"])
                    colNames.pop()
                    listaProfData.append(colNames)

                # linha com dados
                data = list(profData["fetch"])
                pessoa_id = data.pop()

                # Baixando foto, se nao tiver foto pular
                if not self.getFoto(cpf):
                  
                    continue

                 # Adicionando em uma lista os profissionais
                listaProfData.append(data)

                # Gerando QRcode
                self.getQRCode(pessoa_id, cpf)
            else:
                with open(f"{self.lote_dir}/nao_encontrato.txt", 'a', encoding='utf-8') as f:
                    f.write(f"{cpf}\n")

        # Gravar em Arquivo XLS
        self.writeXLS(listaProfData)

        print("\n--- FIM DO PROCESSO ---")


GerarLotesExcel()

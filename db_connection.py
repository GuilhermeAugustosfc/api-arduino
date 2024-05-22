# db_connection.py

import mysql.connector
from mysql.connector import Error


def get_db_connection():
    config = {
        "host": "database-1.cpigwcyuk6vv.us-east-2.rds.amazonaws.com",
        "user": "admin",
        "password": "SO9yvDX5GwQfF1EWEM5u",
        "database": "mqtt1",
    }
    try:
        connection = mysql.connector.connect(**config)
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Erro ao conectar ao MySQL: {e}")
        return None


def close_db_connection(connection):
    if connection.is_connected():
        connection.close()
        print("Conexão ao MySQL foi fechada")

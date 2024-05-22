import mysql.connector
from mysql.connector import Error

config = {
    "host": "database-1.cpigwcyuk6vv.us-east-2.rds.amazonaws.com",
    "user": "admin",
    "password": "SO9yvDX5GwQfF1EWEM5u",
    "database": "mqtt1",
}
db_connection = mysql.connector.connect(**config)

cursor = db_connection.cursor()


def calcular_diferenca_tempo(dados):
    total = 0
    for item in dados:
        total += item[3].seconds

    return total


def get_disponibilidade(timestamp_inicial, timestamp_final):

    # Questão 1: Gráfico de disponibilidade
    intervalos = get_intervalo_disponibilidade(timestamp_inicial, timestamp_final)
    tempo_trabalhando = calcular_diferenca_tempo(intervalos)

    cursor.execute(
        """
        SELECT total_horas_trabalho
        FROM maquina
    """
    )
    config_maquina = cursor.fetchone()
    cursor.close()
    db_connection.close()
    porc_tempo_trabalhado = (tempo_trabalhando / config_maquina[0]) * 100
    return {
        "porc_tempo_trabalhado": porc_tempo_trabalhado,
        "tempo_trabalhando_segundos": tempo_trabalhando,
        "total_horas_trabalho_segundos": config_maquina[0],
    }


def get_produtividade(timestamp_inicio, timestamp_fim):
    cursor.execute(
        """
        SELECT s.maquina_id, s.producao, m.total_produto, (s.producao / m.total_produto) * 100 AS produtividade
        FROM sensordata s
        JOIN maquina m ON s.maquina_id = m.id
        WHERE s.producao = (SELECT MAX(producao) FROM sensordata WHERE status = 1 and sensordata.timestamp BETWEEN %s AND %s)
        """,
        (timestamp_inicio, timestamp_fim),
    )
    produtividade_resultado = cursor.fetchall()
    cursor.close()
    db_connection.close()
    if len(produtividade_resultado) > 0:

        return produtividade_resultado[0]

    return []


def total_produtos_produzidos(inicial, final):

    cursor.execute(
        """
        SELECT MAX(producao)
        FROM sensordata
        WHERE status = 1 AND
        timestamp BETWEEN %s AND %s
        ORDER BY timestamp DESC
        LIMIT 1
    """,
        (inicial, final),
    )
    dados = cursor.fetchone()
    cursor.close()
    db_connection.close()
    if dados[0] is not None:
        return dados[0]

    return []


def get_intervalo_disponibilidade(timestamp_inicio, timestamp_fim):
    query = """
            SELECT 
            MIN(timestamp) AS inicio_intervalo,
            MAX(timestamp) AS fim_intervalo,
            status,
            sec_to_time(TIMESTAMPDIFF(SECOND, MIN(timestamp), MAX(timestamp))) AS duracao_intervalo
            FROM (
                SELECT
                    timestamp,
                    status,
                    @group := IF(@prev_status = status, @group, @group + 1) AS grp,
                    @prev_status := status
                FROM
                    sensordata,
                    (SELECT @group := 0, @prev_status := NULL) AS vars
                ORDER BY
                    timestamp
            ) AS grouped
            WHERE 
                status = 1 and timestamp BETWEEN %s AND %s
            GROUP BY 
                grp;
        """
    cursor.execute(query, (timestamp_inicio, timestamp_fim))
    dados = cursor.fetchall()
    cursor.close()
    db_connection.close()
    return dados


def get_intervalos_falhas(timestamp_inicio, timestamp_fim):

    query = """
            SELECT 
            MIN(timestamp) AS inicio_intervalo,
            MAX(timestamp) AS fim_intervalo,
            status,
            sec_to_time(TIMESTAMPDIFF(SECOND, MIN(timestamp), MAX(timestamp))) AS duracao_intervalo
            FROM (
                SELECT
                    timestamp,
                    status,
                    motivo,
                    @group := IF(@prev_status = status, @group, @group + 1) AS grp,
                    @prev_status := status
                FROM
                    sensordata,
                    (SELECT @group := 0, @prev_status := NULL) AS vars
                ORDER BY
                    timestamp
            ) AS grouped
            WHERE 
                status = 0 and motivo is null and timestamp BETWEEN %s AND %s
            GROUP BY 
                grp;
        """
    cursor.execute(query, (timestamp_inicio, timestamp_fim))
    dados = cursor.fetchall()
    cursor.close()
    db_connection.close()
    return dados


def get_historico(timestamp_inicio, timestamp_fim):
    query = """
            SELECT timestamp, status, producao, motivo from sensordata where timestamp BETWEEN %s AND %s
        """
    cursor.execute(query, (timestamp_inicio, timestamp_fim))
    dados = cursor.fetchall()
    cursor.close()
    db_connection.close()
    return dados


def get_config_maquina(id):
    query = """
        SELECT total_produto, horario_max_manutencao, total_horas_trabalho FROM maquina WHERE id = %s
    """
    try:
        cursor.execute(query, (id,))
        resultado = cursor.fetchone()
        cursor.close()
        db_connection.close()

        if resultado:
            return resultado
        else:
            return None  # ou outra forma de indicar que não há resultado
    except Exception as e:
        print(f"Erro ao buscar configuração da máquina: {e}")
        return None


def update_motivo(timestamp_inicio, timestamp_fim, motivo):
    try:
        # Conexão com o banco de dados
        if db_connection.is_connected():
            cursor = db_connection.cursor()

            # Query de atualização
            query = """
                UPDATE sensordata 
                SET motivo = %s 
                WHERE timestamp BETWEEN %s AND %s
            """

            # Executa a query
            cursor.execute(query, (motivo, timestamp_inicio, timestamp_fim))

            # Confirma a transação
            db_connection.commit()

            print(f"Total de registros atualizados: {cursor.rowcount}")

            # Fecha o cursor
            updated_rows = cursor.rowcount
            cursor.close()
            db_connection.close()
            return updated_rows

    except Error as e:
        print(f"Erro ao conectar ao MySQL: {e}")
        return -1
    finally:
        if db_connection.is_connected():
            # Fecha a conexão
            db_connection.close()
            print("Conexão ao MySQL foi fechada")


def config_maquina(total_produto, horario_max_manutencao, total_horas_trabalho):
    try:
        # Conexão com o banco de dados
        if db_connection.is_connected():
            cursor = db_connection.cursor()

            # Query de atualização
            query = """
                UPDATE maquina 
                SET total_produto = %s, horario_max_manutencao = %s, total_horas_trabalho = %s  
                WHERE id = 1 LIMIT 1
            """

            # Executa a query
            cursor.execute(
                query, (total_produto, horario_max_manutencao, total_horas_trabalho)
            )

            # Confirma a transação
            db_connection.commit()

            print(f"Total de registros atualizados: {cursor.rowcount}")

            # Fecha o cursor
            updated_rows = cursor.rowcount
            cursor.close()
            db_connection.close()

            return updated_rows

    except Error as e:
        print(f"Erro ao conectar ao MySQL: {e}")
        return -1
    finally:
        if db_connection.is_connected():
            # Fecha a conexão
            db_connection.close()
            print("Conexão ao MySQL foi fechada")

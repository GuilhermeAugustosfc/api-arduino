from mysql.connector import Error

from db_connection import get_db_connection, close_db_connection


def calcular_diferenca_tempo(dados):
    total = 0
    for item in dados:
        total += item[3].seconds
    return total


def get_disponibilidade(timestamp_inicial, timestamp_final):
    connection = get_db_connection()
    if connection:
        cursor = connection.cursor()
        intervalos = get_intervalo_disponibilidade(
            cursor, timestamp_inicial, timestamp_final
        )
        tempo_trabalhando = calcular_diferenca_tempo(intervalos)

        cursor.execute("SELECT total_horas_trabalho FROM maquina")
        config_maquina = cursor.fetchone()
        cursor.close()
        close_db_connection(connection)

        porc_tempo_trabalhado = (tempo_trabalhando / config_maquina[0]) * 100
        return {
            "porc_tempo_trabalhado": porc_tempo_trabalhado,
            "tempo_trabalhando_segundos": tempo_trabalhando,
            "total_horas_trabalho_segundos": config_maquina[0],
        }
    return None


def get_produtividade(timestamp_inicio, timestamp_fim):
    connection = get_db_connection()
    if connection:
        cursor = connection.cursor()
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
        close_db_connection(connection)
        if produtividade_resultado:
            return produtividade_resultado[0]
    return []


def total_produtos_produzidos(inicial, final):
    connection = get_db_connection()
    if connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT MAX(producao)
            FROM sensordata
            WHERE status = 1 AND timestamp BETWEEN %s AND %s
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (inicial, final),
        )
        dados = cursor.fetchone()
        cursor.close()
        close_db_connection(connection)
        if dados[0] is not None:
            return dados[0]
    return []


def get_intervalo_disponibilidade(cursor, timestamp_inicio, timestamp_fim):
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
    return dados


def get_intervalos_falhas(timestamp_inicio, timestamp_fim):
    connection = get_db_connection()
    if connection:
        cursor = connection.cursor()
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
        close_db_connection(connection)
        return dados
    return []


def get_historico(timestamp_inicio, timestamp_fim):
    connection = get_db_connection()
    if connection:
        cursor = connection.cursor()
        query = """
            SELECT timestamp, status, producao, motivo from sensordata where timestamp BETWEEN %s AND %s
        """
        cursor.execute(query, (timestamp_inicio, timestamp_fim))
        dados = cursor.fetchall()
        cursor.close()
        close_db_connection(connection)
        return dados
    return []


def get_config_maquina(id):
    connection = get_db_connection()
    if connection:
        cursor = connection.cursor()
        query = "SELECT total_produto, horario_max_manutencao, total_horas_trabalho FROM maquina WHERE id = %s"
        cursor.execute(query, (id,))
        resultado = cursor.fetchone()
        cursor.close()
        close_db_connection(connection)
        return resultado if resultado else None


def update_motivo(timestamp_inicio, timestamp_fim, motivo):
    connection = get_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            query = (
                "UPDATE sensordata SET motivo = %s WHERE timestamp BETWEEN %s AND %s"
            )
            cursor.execute(query, (motivo, timestamp_inicio, timestamp_fim))
            connection.commit()
            updated_rows = cursor.rowcount
            cursor.close()
            close_db_connection(connection)
            return updated_rows
        except Error as e:
            print(f"Erro ao conectar ao MySQL: {e}")
            return -1


def config_maquina(total_produto, horario_max_manutencao, total_horas_trabalho):
    connection = get_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            query = """
                UPDATE maquina 
                SET total_produto = %s, horario_max_manutencao = %s, total_horas_trabalho = %s  
                WHERE id = 1 LIMIT 1
            """
            cursor.execute(
                query, (total_produto, horario_max_manutencao, total_horas_trabalho)
            )
            connection.commit()
            updated_rows = cursor.rowcount
            cursor.close()
            close_db_connection(connection)
            return updated_rows
        except Error as e:
            print(f"Erro ao conectar ao MySQL: {e}")
            return -1

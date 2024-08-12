from mysql.connector import Error
from datetime import timedelta
from datetime import datetime

from db_connection import get_db_connection, close_db_connection


def calcular_diferenca_tempo(data, timestamp_final):
    total = 0

    for row in data:
        duracao_intervalo = row[3]
        tempo_ate_proximo_status = row[4]
        # Somando os intervalos

        if tempo_ate_proximo_status == None:
            ultimo_tempo_ate_agora = row[0]
            ultimo_tempo_do_filtro = timestamp_final
            diferenca = ultimo_tempo_do_filtro - ultimo_tempo_ate_agora
            diferenca_seconds = diferenca.total_seconds()
            total_time = duracao_intervalo.seconds + diferenca_seconds
        else:
            total_time = duracao_intervalo.seconds + tempo_ate_proximo_status.seconds

        total += total_time

    return total


def get_disponibilidade(timestamp_inicial, timestamp_final):
    connection = get_db_connection()
    if connection:
        cursor = connection.cursor()
        intervalos_disponibilidade = get_intervalo_disponibilidade(
            cursor, timestamp_inicial, timestamp_final
        )
        date_object = datetime.strptime(timestamp_final, "%Y-%m-%d %H:%M:%S")

        tempo_trabalhando = calcular_diferenca_tempo(
            intervalos_disponibilidade, date_object
        )

        cursor.execute("SELECT total_horas_trabalho, tempo_de_ciclo FROM maquina")
        config_maquina = cursor.fetchone()
        cursor.close()
        close_db_connection(connection)

        total_horas_trabalho_segundos = config_maquina[0]
        porc_tempo_trabalhado = (
            tempo_trabalhando / total_horas_trabalho_segundos
        ) * 100
        tempo_ciclo = config_maquina[1]
        total_produtos_que_deveria_produzir = (
            total_horas_trabalho_segundos / tempo_ciclo
        )
        total_produtos_que_produziu = tempo_trabalhando / tempo_ciclo
        porc_produtos_que_deveria_produzir_no_tempo_disponivel = (
            total_produtos_que_produziu / total_produtos_que_deveria_produzir
        ) * 100
        return {
            "porc_tempo_trabalhado": porc_tempo_trabalhado,
            "tempo_trabalhando_segundos": tempo_trabalhando,
            "total_horas_trabalho_segundos": total_horas_trabalho_segundos,
            "total_produtos_que_produziu": total_produtos_que_produziu,
            "total_produtos_que_deveria_produzir": total_produtos_que_deveria_produzir,
            "porc_produtos_que_deveria_produzir_no_tempo_disponivel": porc_produtos_que_deveria_produzir_no_tempo_disponivel,
            "tempo_de_ciclo": tempo_ciclo,
        }
    return None


def get_user(login, password):
    connection = get_db_connection()
    if connection:
        cursor = connection.cursor()

        cursor.execute(
            "SELECT name, permissao FROM user WHERE name = %s and password = %s",
            (login, password),
        )
        user = cursor.fetchone()
        cursor.close()
        close_db_connection(connection)

        return user


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
            WHERE timestamp BETWEEN %s AND %s
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
            next_timestamp as fim_intervalo,
            status,
            sec_to_time(TIMESTAMPDIFF(SECOND, MIN(timestamp), MAX(timestamp))) AS duracao_intervalo,
            sec_to_time(TIMESTAMPDIFF(SECOND, MAX(timestamp), next_timestamp)) AS tempo_ate_proximo_status
        FROM (
            SELECT 
                timestamp,
                status,
                motivo,
                @group := IF(@prev_status = status, @group, @group + 1) AS grp,
                @prev_status := status,
                (SELECT MIN(timestamp) FROM sensordata WHERE timestamp > sd.timestamp AND status = 0) AS next_timestamp
            FROM 
                sensordata sd,
                (SELECT @group := 0, @prev_status := NULL) AS vars
            ORDER BY 
                timestamp
        ) AS grouped
        WHERE 
            status = 1 AND motivo IS NULL and timestamp BETWEEN %s AND %s
        GROUP BY 
            grp;
    """
    cursor.execute(query, (timestamp_inicio, timestamp_fim))
    dados = cursor.fetchall()
    return dados


def segundos_para_horario(segundos):
    td = timedelta(seconds=segundos)
    horas, resto = divmod(td.seconds, 3600)
    minutos, segundos = divmod(resto, 60)
    return f"{horas:02}:{minutos:02}:{segundos:02}"


def add_time_intervals(data):
    dados = []

    def verificar_variavel_ternario(valor):
        return 0 if valor is None else valor

    for row in data:
        item = []
        duracao_intervalo = row[3]
        tempo_ate_proximo_status = row[4]
        # Somando os intervalos
        total_time = (
            verificar_variavel_ternario(duracao_intervalo.seconds) + 0
            if tempo_ate_proximo_status is None
            else tempo_ate_proximo_status.seconds
        )
        time = segundos_para_horario(total_time)
        item.append(row[0])
        item.append(row[1])
        item.append(row[2])
        item.append(time)

        dados.append(item)
    return dados


def get_quantidade_motivos(timestamp_inicio, timestamp_fim):

    connection = get_db_connection()
    if connection:
        cursor = connection.cursor()

        query = """
            select motivo, COUNT(*) AS quantidade from (SELECT 
                MIN(timestamp) AS inicio_intervalo,
                next_timestamp as fim_intervalo,
                status,
                motivo,
                sec_to_time(TIMESTAMPDIFF(SECOND, MIN(timestamp), MAX(timestamp))) AS duracao_intervalo,
                sec_to_time(TIMESTAMPDIFF(SECOND, MAX(timestamp), next_timestamp)) AS tempo_ate_proximo_status
            FROM (
                SELECT 
                    timestamp,
                    status,
                    motivo,
                    @group := IF(@prev_status = status, @group, @group + 1) AS grp,
                    @prev_status := status,
                    (SELECT MIN(timestamp) FROM sensordata WHERE timestamp > sd.timestamp AND status = 1) AS next_timestamp
                FROM 
                    sensordata sd,
                    (SELECT @group := 0, @prev_status := NULL) AS vars
                ORDER BY 
                    timestamp
            ) AS grouped
            WHERE 
                status = 0 AND motivo IS NOT NULL and timestamp BETWEEN %s AND %s
            GROUP BY 
                grp) as intervalos GROUP BY motivo
            """
        cursor.execute(query, (timestamp_inicio, timestamp_fim))
        dados = cursor.fetchall()
        cursor.close()
        close_db_connection(connection)
        return dados
    return []


def get_intervalos_falhas(timestamp_inicio, timestamp_fim):
    connection = get_db_connection()
    if connection:
        cursor = connection.cursor()

        query = """
            SELECT 
                MIN(timestamp) AS inicio_intervalo,
                next_timestamp as fim_intervalo,
                status,
                sec_to_time(TIMESTAMPDIFF(SECOND, MIN(timestamp), MAX(timestamp))) AS duracao_intervalo,
                sec_to_time(TIMESTAMPDIFF(SECOND, MAX(timestamp), next_timestamp)) AS tempo_ate_proximo_status
            FROM (
                SELECT 
                    timestamp,
                    status,
                    motivo,
                    @group := IF(@prev_status = status, @group, @group + 1) AS grp,
                    @prev_status := status,
                    (SELECT MIN(timestamp) FROM sensordata WHERE timestamp > sd.timestamp AND status = 1) AS next_timestamp
                FROM 
                    sensordata sd,
                    (SELECT @group := 0, @prev_status := NULL) AS vars
                ORDER BY 
                    timestamp
            ) AS grouped
            WHERE 
                status = 0 and timestamp BETWEEN %s AND %s
            GROUP BY 
                grp;
            """
        cursor.execute(query, (timestamp_inicio, timestamp_fim))
        dados = cursor.fetchall()
        cursor.close()
        close_db_connection(connection)

        response = add_time_intervals(dados)
        return response
    return []


def processar_timelapse(data):
    # Verificando se o parâmetro é uma lista
    if not isinstance(data, list):
        return "Erro: O parâmetro deve ser uma lista de listas."

    # Verificando se todos os elementos da lista são listas
    for item in data:
        if not isinstance(item, list):
            return "Erro: Todos os elementos da lista principal devem ser listas."

    # Verificando se cada lista interna tem exatamente dois elementos
    for item in data:
        if len(item) != 2:
            return "Erro: Cada lista interna deve conter exatamente dois elementos."

    array_simples = []

    # Adicionando a primeira linha inteira
    array_simples.extend(data[0])

    # Adicionando apenas o segundo elemento das linhas subsequentes
    for linha in data[1:]:
        array_simples.append(linha[1])

    return array_simples


def get_timelapse(timestamp_inicio, timestamp_fim):
    connection = get_db_connection()
    if connection:
        cursor = connection.cursor()
        query = """
            SELECT 
                t1.timestamp, 
                t1.status, 
                t2.timestamp, 
                t2.status
            FROM 
                (
                    SELECT 
                        @rownum := @rownum + 1 AS row_num, 
                        timestamp, 
                        status 
                    FROM 
                        sensordata, 
                        (SELECT @rownum := 0) r 
                        WHERE 
                        timestamp BETWEEN %s AND %s
                    ORDER BY 
                        timestamp ASC
                ) t1
            JOIN 
                (
                    SELECT 
                        @rownum2 := @rownum2 + 1 AS row_num, 
                        timestamp, 
                        status 
                    FROM 
                        sensordata, 
                        (SELECT @rownum2 := 0) r 
                        WHERE 
                        timestamp BETWEEN %s AND %s
                    ORDER BY 
                        timestamp ASC
                ) t2 
            ON 
                t1.row_num = t2.row_num - 1 
            WHERE 
                t1.status != t2.status;
            """
        cursor.execute(
            query, (timestamp_inicio, timestamp_fim, timestamp_inicio, timestamp_fim)
        )
        dados = cursor.fetchall()
        cursor.close()
        close_db_connection(connection)

        response = processar_timelapse(dados)
        return response
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
        query = "SELECT total_produto, horario_max_manutencao, total_horas_trabalho, tempo_de_ciclo FROM maquina WHERE id = %s"
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
            query = """
                UPDATE sensordata SET motivo = %s WHERE timestamp BETWEEN %s AND %s
            """
            cursor.execute(query, (motivo, timestamp_inicio, timestamp_fim))
            connection.commit()
            updated_rows = cursor.rowcount
            cursor.close()
            close_db_connection(connection)
            return updated_rows
        except Error as e:
            print(f"Erro ao conectar ao MySQL: {e}")
            return -1


def config_maquina(
    total_produto,
    total_horas_trabalho,
    horario_max_manutencao,
):
    connection = get_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            query = """
                UPDATE maquina 
                SET total_produto = %s, total_horas_trabalho = %s, horario_max_manutencao = %s, tempo_de_ciclo = %s   
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

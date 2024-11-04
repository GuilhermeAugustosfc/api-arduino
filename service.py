from mysql.connector import Error
from datetime import timedelta
from datetime import datetime

from db_connection import get_db_connection, close_db_connection


def calcular_hora_trabalhada_segundos(data):
    total = 0
    producao = 0

    tempo_total_trabalho_ate_o_momento_segundos = 0
    for index, item in enumerate(data):
        if index == 0:
            tempo_total_trabalho_ate_o_momento_segundos = item["hora"]

        if index == len(data) - 1:
            tempo_total_trabalho_ate_o_momento_segundos = (
                item["hora2"] - tempo_total_trabalho_ate_o_momento_segundos
            )
            tempo_total_trabalho_ate_o_momento_segundos = (
                tempo_total_trabalho_ate_o_momento_segundos.seconds
            )

        if item["status"] == 1:
            hora_inicio = (
                item["hora"]
                if isinstance(item["hora"], datetime)
                else datetime.strptime(item["hora"], "%Y-%m-%d %H:%M:%S")
            )
            hora_fim = (
                item["hora2"]
                if isinstance(item["hora2"], datetime)
                else datetime.strptime(item["hora2"], "%Y-%m-%d %H:%M:%S")
            )
            diferenca = hora_fim - hora_inicio
            total += diferenca.total_seconds()
            producao += item["producao"]
    return int(total), producao, int(tempo_total_trabalho_ate_o_momento_segundos)


def get_disponibilidade(timestamp_inicial, timestamp_final):
    connection = get_db_connection()
    if connection:
        cursor = connection.cursor()
        intervalos_disponibilidade = get_timelapse(timestamp_inicial, timestamp_final)

        (
            tempo_trabalhando_segundos,
            producao,
            tempo_total_trabalho_ate_o_momento_segundos,
        ) = calcular_hora_trabalhada_segundos(intervalos_disponibilidade)

        cursor.execute("SELECT total_horas_trabalho, tempo_de_ciclo FROM maquina")
        config_maquina = cursor.fetchone()
        cursor.close()
        close_db_connection(connection)

        total_horas_trabalho_segundos = config_maquina[0]
        porc_tempo_trabalhado = (
            tempo_trabalhando_segundos / total_horas_trabalho_segundos
        ) * 100

        tempo_ciclo = config_maquina[1]
        total_produtos_que_deveria_produzir_no_final_do_dia_de_acordo_com_o_ciclo = (
            total_horas_trabalho_segundos / tempo_ciclo
        )

        total_produtos_que_produziu_no_tempo_trabalhado_por_ciclo = (
            tempo_total_trabalho_ate_o_momento_segundos / tempo_ciclo
        )
        total_produtos_que_produziu_no_tempo_trabalhado_por_evento = producao

        porc_produtos_que_deveria_produzir_no_tempo_disponivel = (
            total_produtos_que_produziu_no_tempo_trabalhado_por_evento
            / total_produtos_que_produziu_no_tempo_trabalhado_por_ciclo
        ) * 100

        porc_tempo_trabalhado_real_time = (
            tempo_trabalhando_segundos / tempo_total_trabalho_ate_o_momento_segundos
        ) * 100
        return {
            "porc_tempo_trabalhado": int(porc_tempo_trabalhado),
            "porc_tempo_trabalhado_real_time": int(porc_tempo_trabalhado_real_time),
            "tempo_trabalhando_segundos": int(tempo_trabalhando_segundos),
            "total_horas_trabalho_segundos": int(total_horas_trabalho_segundos),
            "total_produtos_que_deveria_produzir_no_final_do_dia_de_acordo_com_o_ciclo": int(
                total_produtos_que_deveria_produzir_no_final_do_dia_de_acordo_com_o_ciclo
            ),
            "total_produtos_que_produziu_no_tempo_trabalhado_por_ciclo": int(
                total_produtos_que_produziu_no_tempo_trabalhado_por_ciclo
            ),
            "total_produtos_que_produziu_no_tempo_trabalhado_por_evento": int(
                total_produtos_que_produziu_no_tempo_trabalhado_por_evento
            ),
            "porc_produtos_que_deveria_produzir_no_tempo_disponivel": int(
                porc_produtos_que_deveria_produzir_no_tempo_disponivel
            ),
            "tempo_total_trabalho_ate_o_momento_segundos": int(
                tempo_total_trabalho_ate_o_momento_segundos
            ),
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
            SELECT s.maquina_id, s.producao, m.total_produto, (s.producao / m.total_produto) * 100 AS produtividade, m.pricePerPiece
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
        item.append(row[5])
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
                status = 0 and timestamp BETWEEN %s AND %s
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
                sec_to_time(TIMESTAMPDIFF(SECOND, MAX(timestamp), next_timestamp)) AS tempo_ate_proximo_status,
                motivo
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

    if len(data) <= 0:
        return "Erro: O parâmetro nao deve ser uma lista vazia."

    # Verificando se cada lista interna tem exatamente dois elementos
    retorno = []

    # {"hora": linha[1], "status": linha[3], "motivo": linha[5]}
    # Adicionando apenas o segundo elemento das linhas subsequentes
    status_temp = None
    hora_temp = None
    motivo_temp = None
    for i, linha in enumerate(data):
        hora = linha[0]
        status = linha[1]
        motivo = linha[2]
        producao = linha[3]

        if hora_temp is None:
            hora_temp = hora

        if status_temp is None:
            status_temp = status

        if motivo_temp is None:
            motivo_temp = motivo

        if status_temp != status:

            retorno.append(
                {
                    "hora": hora_temp,
                    "hora2": hora,
                    "motivo": motivo_temp,
                    "status": status_temp,
                    "producao": producao,
                }
            )

            hora_temp = None
            status_temp = None
            motivo_temp = None

        if i == len(data) - 1:
            retorno.append(
                {
                    "hora": hora_temp,
                    "hora2": hora,
                    "motivo": motivo_temp,
                    "status": status_temp,
                    "producao": producao,
                }
            )

    return retorno


def get_timelapse(timestamp_inicio, timestamp_fim):
    connection = get_db_connection()
    if connection:
        cursor = connection.cursor()
        query = """
                SELECT 
                    timestamp, 
                    status,
                    motivo,
                    producao
                FROM 
                    sensordata
                WHERE 
                    timestamp BETWEEN %s AND %s
                ORDER BY 
                    timestamp ASC;
            """
        cursor.execute(query, (timestamp_inicio, timestamp_fim))
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
        cursor = connection.cursor(dictionary=True)
        query = "SELECT total_produto, horario_max_manutencao, total_horas_trabalho, tempo_de_ciclo, pricePerPiece FROM maquina WHERE id = %s"
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
                UPDATE sensordata SET motivo = %s WHERE status = 0 AND timestamp BETWEEN %s AND %s
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


def save_apontamento(request):
    connection = get_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            query_verificar = """
                SELECT quantidade FROM apontamento WHERE dia = %s
            """
            cursor.execute(query_verificar, (request.data,))
            row = cursor.fetchone()
            if row:
                query_atualizar = """
                    UPDATE apontamento SET quantidade = %s WHERE dia = %s
                """
                cursor.execute(
                    query_atualizar, (request.pecasDefeituosas, request.data)
                )
            else:
                query_inserir = """
                    INSERT INTO apontamento (dia, quantidade) values (%s, %s)
                """
                cursor.execute(query_inserir, (request.data, request.pecasDefeituosas))
            connection.commit()
            updated_rows = cursor.rowcount
            cursor.close()
            close_db_connection(connection)
            return updated_rows
        except Error as e:
            print(f"Erro ao conectar ao MySQL: {e}")
            return -1


def pecas_perdidas(date):
    connection = get_db_connection()
    if connection:
        try:
            cursor = connection.cursor(dictionary=True)
            query = """
                SELECT quantidade FROM apontamento WHERE dia = %s
            """
            cursor.execute(query, (date,))
            row = cursor.fetchone()
            cursor.close()
            close_db_connection(connection)
            return row
        except Error as e:
            print(f"Erro ao conectar ao MySQL: {e}")
            return -1


def pecas_perdidas_range(date_inicial, date_final):
    connection = get_db_connection()
    if connection:
        try:
            cursor = connection.cursor(dictionary=True)
            query = """
                SELECT sum(quantidade) as quantidade FROM apontamento WHERE dia BETWEEN %s AND %s
            """
            cursor.execute(query, (date_inicial, date_final))
            row = cursor.fetchone()
            cursor.close()
            close_db_connection(connection)
            return row
        except Error as e:
            print(f"Erro ao conectar ao MySQL: {e}")
            return -1


def config_maquina(
    total_produto,
    total_horas_trabalho,
    horario_max_manutencao,
    tempo_de_ciclo,
    pricePerPiece,
):
    connection = get_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            query = """
                UPDATE maquina 
                SET total_produto = %s, total_horas_trabalho = %s, horario_max_manutencao = %s, tempo_de_ciclo = %s, pricePerPiece = %s
                WHERE id = 1 LIMIT 1
            """
            cursor.execute(
                query,
                (
                    total_produto,
                    total_horas_trabalho,
                    horario_max_manutencao,
                    tempo_de_ciclo,
                    pricePerPiece,
                ),
            )
            connection.commit()
            updated_rows = cursor.rowcount
            cursor.close()
            close_db_connection(connection)
            return updated_rows
        except Error as e:
            print(f"Erro ao conectar ao MySQL: {e}")
            return -1

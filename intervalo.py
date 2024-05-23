from datetime import timedelta


def parse_time_interval(interval):
    if interval is None or interval == "":
        return timedelta()
    hours, minutes, seconds = map(int, interval.split(":"))
    return timedelta(hours=hours, minutes=minutes, seconds=seconds)


def add_time_intervals(data):
    for row in data:
        duracao_intervalo = row[3]
        tempo_ate_proximo_status = row[4]

        # Convertendo para timedelta
        duracao_td = parse_time_interval(duracao_intervalo)
        tempo_ate_td = parse_time_interval(tempo_ate_proximo_status)

        # Somando os intervalos
        total_time = duracao_td + tempo_ate_td

        # Adicionando a soma à linha
        row.append(str(total_time))

    return data


# Dados fornecidos
data = [
    ["2024-05-22 08:00:00", "2024-05-22 08:30:00", 0, "00:00:00", "00:30:00"],
    ["2024-05-22 11:00:00", "2024-05-22 12:00:00", 0, "00:30:00", "00:30:00"],
    ["2024-05-22 12:30:00", "2024-05-22 14:00:00", 0, "01:00:00", "00:30:00"],
    ["2024-05-22 14:30:00", "2024-05-22 15:00:00", 0, "00:00:00", "00:30:00"],
    ["2024-05-22 15:30:00", "2024-05-22 16:00:00", 0, "00:00:00", "00:30:00"],
    ["2024-05-22 16:30:00", None, 0, "1:30:00", None],
]

# Somando os intervalos de tempo
result = add_time_intervals(data)

# Exibindo os resultados
for row in result:
    print(row)

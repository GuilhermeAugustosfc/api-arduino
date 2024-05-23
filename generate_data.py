import random
import datetime


# Função para gerar registros de dados fictícios
def generate_data(start_time, end_time, interval_minutes):
    current_time = start_time
    data = []

    while current_time <= end_time:
        temperatura = round(
            random.uniform(20.0, 40.0), 1
        )  # Temperatura entre 20.0 e 40.0 graus
        umidade = round(random.uniform(30.0, 70.0), 1)  # Umidade entre 30.0% e 70.0%
        status = random.choice([0, 1])  # Status aleatório entre 0 e 1
        producao = (
            random.randint(0, 100) if status == 1 else 0
        )  # Produção entre 0 e 100 se status é 1, senão 0
        maquina_id = 1  # ID da máquina
        motivo = None  # Motivo aleatório

        data.append(
            {
                "temperatura": temperatura,
                "umidade": umidade,
                "timestamp": current_time.strftime("%Y-%m-%d %H:%M:%S"),
                "status": status,
                "producao": producao,
                "maquina_id": maquina_id,
                "motivo": motivo,
            }
        )

        current_time += datetime.timedelta(minutes=interval_minutes)

    return data


# Gerar registros das 08:00:00 às 18:00:00 a cada 30 minutos
start_time = datetime.datetime(2024, 5, 22, 8, 0, 0)
end_time = datetime.datetime(2024, 5, 22, 18, 0, 0)
interval_minutes = 30

generated_data = generate_data(start_time, end_time, interval_minutes)

# Exibir os registros gerados
for record in generated_data:
    print(record)

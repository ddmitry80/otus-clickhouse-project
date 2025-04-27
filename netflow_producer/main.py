import sys
from datetime import datetime
import time
import pandas as pd
from kafka import KafkaProducer
from pathlib import Path
from loguru import logger
from typing import Any, Dict
import pytz
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


# Модель данных netflow
class Router(BaseModel):
    IdSession: int
    IdPSX: int
    IdSubscriber: int
    StartSession: datetime
    EndSesstion: datetime | None = None
    Duration: int
    UpTx: int
    DownTx: int


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_ignore_empty=True, env_file='.env', env_file_encoding='utf-8')

    current_timezone: str = 'Europe/Moscow'
    kafka_broker: str = 'localhost:9092'
    kafka_topic: str = 'csv_data_topic'
    csv_directory: str = '../data/TelecomX/telecom100k/'
    log_file: str = "logs/netflow_producer.log"
    time_pointer_file: str = 'logs/time_pointer.txt'
    wait_time: int = 10  # время ожидания перед отправкой следующей порции данных


settings = Settings()

logger.add(settings.log_file)

KAFKA_BROKER = settings.kafka_broker
KAFKA_TOPIC = settings.kafka_topic
CSV_DIRECTORY = settings.csv_directory

current_timezone = pytz.timezone(settings.current_timezone)


def get_kafka_producer() -> KafkaProducer:
    """
    Инициализируем KafkaProducer с базовыми настройками.
    В продакшене нужно добавить обработку ошибок подключения, настройки безопасности и т.д.
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            # value_serializer=lambda v: json.dumps(v, allow_nan=False).encode('utf-8'),
            value_serializer=lambda v: str(v).encode('utf-8'),
            retries=5,               # Повторная отправка при сбоях
            linger_ms=10,            # Небольшая задержка перед отправкой
            max_request_size=1048576 # Ограничение размера запроса (1MB)
        )
        logger.info("KafkaProducer успешно инициализирован!")
        return producer
    except Exception as e:
        logger.error(f"Ошибка инициализации KafkaProducer: {e}")
        raise


def wait_until(target_time: datetime):
    """Ждёт до указанного времени (формат 'HH:MM:SS')."""
    while True:
        now = datetime.now()
        if now >= target_time:
            break  # Если время уже наступило, выходим
        time_left = (target_time - now).total_seconds()
        time.sleep(min(time_left, 1))  # Ждём не более 1 секунды за раз


def send_dataframe(dataframe: pd.DataFrame, model: BaseModel, producer: KafkaProducer, headers: dict = None):
    """Отправка датафрейма Pandas в топик Kafka"""
    headers_list = [(key, str(headers[key]).encode('utf8')) for key in headers]
    for rec in dataframe.to_dict(orient='records'):
        producer.send(KAFKA_TOPIC, value=model(**rec).model_dump_json(), headers=headers_list)


def main():
    print("Hello from netflow-producer!")

    # Список фалов-источников
    r0_logs = sorted(Path(CSV_DIRECTORY).glob('psx_66.1_*.txt'))
    r1_logs = sorted(Path(CSV_DIRECTORY).glob('psx_66.2_*.txt'))
    r2_logs = sorted(Path(CSV_DIRECTORY).glob('psx_66.3_*.txt'))
    r3_logs = sorted(Path(CSV_DIRECTORY).glob('psx_62.0_*.csv'))
    r4_logs = sorted(Path(CSV_DIRECTORY).glob('psx_69.0_*.csv'))
    r5_logs = sorted(Path(CSV_DIRECTORY).glob('psx_65.0_*.csv'))

    producer = get_kafka_producer()

    # Управление задержками передачи
    start_time = datetime.now()
    current_time = start_time

    # Читаю сохраненую закладу времени
    if Path(settings.time_pointer_file).exists():
        with open(Path(settings.time_pointer_file), 'rt') as time_pointer_file:
            current_time_pointer = pd.Timestamp(time_pointer_file.readline(), tz=current_timezone)
    else:
        current_time_pointer = pd.Timestamp('1900-01-01 00:00:00', tz=current_timezone)

    # Перебираем циклом каждый временной период по всем наборам источников за все время, выбираем одно время за раз
    for i, (r0_file, r1_file, r2_file, r3_file, r4_file, r5_file) in enumerate(zip(r0_logs, r1_logs, r2_logs, r3_logs, r4_logs, r5_logs)):
        logger.info(f"Итерация: {i}, время: {current_time}")

        next_time = current_time + pd.Timedelta(minutes=settings.wait_time)

        # Берем поправку на время данных
        df_time = pd.Timestamp(r0_file.stem[9:], tz=current_timezone)
        df_delta = datetime.now(tz=current_timezone) - df_time

        # Пропускаем уже прочитанное время
        if df_time < current_time_pointer:
            logger.debug(f"Пропускаю время: {df_time.strftime('%Y-%m-%d %H:%M:%S')}")
            continue
        
        # берем txt источники
        for df_file in (r0_file, r1_file, r2_file):
            logger.debug(f"{df_file=}")
            df = pd.read_csv(df_file, sep='|', parse_dates=['StartSession','EndSession'], dayfirst=True).rename(columns={"Duartion": "Duration"})
            df['StartSession'] = df['StartSession'].dt.tz_localize('Etc/GMT-5').dt.tz_convert(current_timezone) + df_delta
            df['EndSession'] = df['EndSession'].dt.tz_localize('Etc/GMT-5').dt.tz_convert(current_timezone) + df_delta

            send_dataframe(dataframe=df, model=Router, producer=producer, headers={'df_file': df_file})
            time.sleep(settings.wait_time*60/10)
        # Берем csv источники
        for df_file in (r3_file, r4_file, r5_file):
            logger.debug(f"{df_file=}")
            df = pd.read_csv(df_file, sep=',', parse_dates=['StartSession','EndSession'], dayfirst=True).rename(columns={"Duartion": "Duration"})
            df['StartSession'] = df['StartSession'].dt.tz_localize('Etc/GMT-6').dt.tz_convert(current_timezone) + df_delta
            df['EndSession'] = df['EndSession'].dt.tz_localize('Etc/GMT-6').dt.tz_convert(current_timezone) + df_delta

            send_dataframe(dataframe=df, model=Router, producer=producer, headers={'df_file': df_file})
            time.sleep(settings.wait_time*60/10)
        producer.flush()

        # Записываем текущую временную метку
        with open(Path(settings.time_pointer_file), 'wt') as time_pointer_file:
            print((df_time + pd.Timedelta(minutes=10)).strftime('%Y-%m-%d %H:%M:%S'), file=time_pointer_file)

        # Ожидаем следующий перод времени
        wait_until(next_time)
        current_time = next_time


if __name__ == "__main__":
    main()

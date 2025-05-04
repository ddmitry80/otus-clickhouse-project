# Проектная работа на тему "Построение хранилища для онлайн аналитики траффика абонентов оператора связи"

Реализация проекта по созданию масштабируемого производственного хранилища данных на базе ClickHouse с интеграцией Apache Superset, Apache Kafka, Prometheus, Grafana и других компонентов. Ниже приведены конкретные цели реализации, подробное описание архитектуры с описанием всех контейнеров из docker-compose.

## Цели работы
- Обеспечить визуализацию траффика абонентов, получаемого с сетевого оборудования, с целью мониторинга производительности и безопасности сети
- Организовать взаимодействие между компонентами
    - Apache NiFi как producer для Kafka потока данных по абонетскому траффику
    - Apache Kafka


# Установка и запуск

## Скачать и запустить проект

```sh
# Скачать репозиторий 
git clone https://github.com/ddmitry80/otus-clickhouse-project
# Скачать репозиторий с данными TelecomX 
cd otus-clickhouse-project/data
git clone https://github.com/ddmitry80/TelecomX
# Запустить проект
cd ..
docker compose up
```

## Создать необходимые структуры данных в кластере

```sh
docker compose exec -T clickhouse1 clickhouse-client -u default --password 123456 < sql/init_pipeline.sql
```

## Запускаем поток данных NiFi из Kafka в Clickhouse

Зайти в NiFi http://localhost:18443/nifi/, загрузить и активировать ProcessGroup `NetFlow2ClickHouse' из каталога nifi.

# Конролировать процесс из DBeaver

- jdbc: `jdbc:clickhouse://localhost:8124`
- login: `default`
- password: `123456`

Пример запроса:
```sql
SELECT *
FROM dm.tc_netflow_v
WHERE endSession IS NOT NULL
ORDER BY startSession DESC
LIMIT 1000;
```

## Настраиваем отчетность в Apache Superset

### Первоначальная настройка Superset

```sh
# Подключаемся к конейнеру
docker compose exec -it superset bash
# Следующие команды выполнить внутри контейнера
superset fab create-admin --username admin --firstname Superset --lastname Admin --email admin@superset.com --password admin
superset db upgrade
superset init
```

### Подключение отчетов

Зайти http://localhost:8088/
- login: `admin'
- password: `admin'

Dasboards -> Import dashboards
Выбрать `superset/netflow_dashboard_export_?.zip`, указать пароль `123456`

Дашборд будет доступен в разделе Dashboards под именем NetFlow

## Мониторинг

### Prometheus

Запускается автоматически. Доступен по http://localhost:9090/

### Grafana

Запускается автоматически. Пароль по умолчанию admin/admin, далее потребуется сменить.

Точка входа http://localhost:3000

Рекомендованый конфиг grafana_14192_rev4.json.

Для настройки сначала подключаем prometheus: Connections -> Add new connection -> Prometheus -> Add new datasource. Connection url: http://prometheus:9090 -> Save & test

Далее - подключаем дашборд Clickhouse: Dashboards -> New -> Import -> указать содержимое файла `grafana_14192_rev4.json` -> Указать ранее подключенный Prometheus.


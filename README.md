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

### Загрузить справочники

Т.к. справочники небольшие, загружаем в полном объеме на каждую ноду кластера

```sql
CREATE DATABASE dict ON CLUSTER c2sh2rep;

-- client
--DROP TABLE IF EXISTS dict.tc_client ON CLUSTER c1sh4rep NO DELAY;
CREATE TABLE dict.tc_client ON CLUSTER c1sh4rep
(
    userId UUID
    , contract String
    , documents String
    , email String
    , idPlan Int64
    , created_at DateTime DEFAULT now()
)
ENGINE = ReplicatedMergeTree('/clickhouse/shard_{shard_c1sh4rep}/{database}/{table}','{replica_c1sh4rep}')
ORDER BY (idPlan, contract);

INSERT INTO dict.tc_client (userId, contract, documents, email, idPlan)
SELECT Id, Contract, Documents, Email, IdPlan FROM file('/var/lib/clickhouse/user_files/data/TelecomX/telecom10k/client.parquet', Parquet);

--- Company
--DROP TABLE IF EXISTS dict.tc_company ON CLUSTER c1sh4rep NO DELAY;
CREATE TABLE dict.tc_company ON CLUSTER c1sh4rep
(
    userId UUID
    , contract String
    , documents String
    , email String
    , idPlan Int64
    , name String
    , address String
    , phones String
    , contact String
    , created_at DateTime DEFAULT now()
)
ENGINE = ReplicatedMergeTree('/clickhouse/shard_{shard_c1sh4rep}/{database}/{table}','{replica_c1sh4rep}')
ORDER BY (idPlan, contract);

INSERT INTO dict.tc_company (userId, contract, documents, email, idPlan, name, address, phones, contact)
SELECT Id, Contract, Documents, Email, IdPlan, Name, Address, Phones, Contact FROM file('/var/lib/clickhouse/user_files/data/TelecomX/telecom10k/company.parquet', Parquet);

-- Physical
--DROP TABLE IF EXISTS stg.tc_physical ON CLUSTER c1sh4rep NO DELAY;
CREATE TABLE dict.tc_physical ON CLUSTER c1sh4rep
(
    userId UUID
    , contract String
    , documents String
    , email String
    , idPlan Int64
    , name String
    , address String
    , passport String
    , phones String
    , created_at DateTime DEFAULT now()
)
ENGINE = ReplicatedMergeTree('/clickhouse/shard_{shard_c1sh4rep}/{database}/{table}','{replica_c1sh4rep}')
ORDER BY (idPlan, contract);

INSERT INTO dict.tc_physical (userId, contract, documents, email, idPlan, name, address, passport, phones)
SELECT Id, Contract, Documents, Email, IdPlan, Name, Address, Passport, Phones 
FROM file('/var/lib/clickhouse/user_files/data/TelecomX/telecom10k/physical.parquet', Parquet);

-- Plan
--DROP TABLE IF EXISTS dict.tc_plan ON CLUSTER c1sh4rep NO DELAY;
CREATE TABLE dict.tc_plan ON CLUSTER c1sh4rep
(
    id int
    , name String NULL
    , description String NULL
    , createdAt DateTime NULL
    , updatedAt DateTime NULL
    , closedAt DateTime NULL
    , enabled Bool NULL
    , attrs String NULL
    , created_at DateTime DEFAULT now()
)
ENGINE = ReplicatedMergeTree('/clickhouse/shard_{shard_c1sh4rep}/{database}/{table}','{replica_c1sh4rep}')
ORDER BY (id);

INSERT INTO dict.tc_plan (id, name, description, createdAt, updatedAt, closedAt, enabled, attrs)
SELECT Id, Name, Description, CreatedAt, UpdatedAt, ClosedAt, Enabled, Attrs 
FROM file('/var/lib/clickhouse/user_files/data/TelecomX/telecom10k/plan.json', json);

-- psxattrs
-- DROP TABLE IF EXISTS dict.tc_psxattrs ON CLUSTER c1sh4rep NO DELAY;
CREATE TABLE dict.tc_psxattrs ON CLUSTER c1sh4rep
(
    id int
    , psx String
    , transmitUnits String
    , delimiter String
    , dateFormat String
    , tz String
    , created_at DateTime DEFAULT now()
)
ENGINE = ReplicatedMergeTree('/clickhouse/shard_{shard_c1sh4rep}/{database}/{table}','{replica_c1sh4rep}')
ORDER BY (id);

INSERT INTO dict.tc_psxattrs (id, psx, transmitUnits, delimiter, dateFormat, tz)
SELECT Id, PSX, TransmitUnits, Delimiter, DateFormat, TZ 
FROM file('/var/lib/clickhouse/user_files/data/TelecomX/telecom10k/psxattrs.csv', csv);
```

### Создаем слой stg и таблицы в нем

```sql
CREATE DATABASE stg ON CLUSTER c2sh2rep;

-- Реплицированная подложка
DROP TABLE IF EXISTS stg.tc_netflow_rep  ON CLUSTER c2sh2rep NO DELAY;
CREATE TABLE stg.tc_netflow_rep ON CLUSTER c2sh2rep
(
    idSession Int64
    , idPSX Int
    , idSubscriber Int
    , startSession String
    , endSession String NULL
    , duration Int
    , upTx Int64
    , downTx Int64
    , created_at DateTime DEFAULT now()
)
ENGINE = ReplicatedMergeTree('/clickhouse/shard_{shard_c2sh2rep}/{database}/{table}','{replica_c2sh2rep}')
PARTITION BY toYYYYMMDD(created_at)
ORDER BY startSession
TTL created_at + INTERVAL 3 DAY;

-- основная шардированная таблица
DROP TABLE IF EXISTS stg.tc_netflow ON CLUSTER c2sh2rep NO DELAY;
CREATE TABLE stg.tc_netflow ON CLUSTER c2sh2rep
AS stg.tc_netflow_rep
ENGINE = Distributed(c2sh2rep, stg, tc_netflow_rep, idSubscriber);
```

## Запускаем поток данных NiFi из Kafka в Clickhouse

Зайти в NiFi http://localhost:18443/nifi/, загрузить и активировать ProcessGroup `NetFlow2ClickHouse'



# Прочее


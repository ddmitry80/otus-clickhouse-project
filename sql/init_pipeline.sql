---------------------- Справочники ----------------------------
CREATE DATABASE IF NOT EXISTS dict ON CLUSTER c2sh2rep;

-- client
DROP TABLE IF EXISTS dict.tc_client_tb ON CLUSTER c1sh4rep NO DELAY;
CREATE TABLE dict.tc_client_tb ON CLUSTER c1sh4rep
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

TRUNCATE dict.tc_client_tb;
INSERT INTO dict.tc_client_tb (userId, contract, documents, email, idPlan)
SELECT Id, Contract, Documents, Email, IdPlan FROM file('/var/lib/clickhouse/user_files/data/TelecomX/telecom1000k/client.parquet', Parquet);

-- Словарь
DROP DICTIONARY IF EXISTS dict.tc_client ON CLUSTER c1sh4rep;
CREATE DICTIONARY dict.tc_client ON CLUSTER c1sh4rep (
    userId UUID
    , contract String
    , documents String
    , email String
    , idPlan Int64
)
PRIMARY KEY userId
SOURCE (CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'tc_client_tb' PASSWORD '123456' DB 'dict'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(HASHED());

-- Subscriber
DROP TABLE IF EXISTS dict.tc_subscriber_tb ON CLUSTER c1sh4rep NO DELAY;
CREATE TABLE dict.tc_subscriber_tb ON CLUSTER c1sh4rep
(
    idClient UUID
    , idOnPSX Int64
    , status LowCardinality(String)
    , created_at DateTime DEFAULT now()
)
ENGINE = ReplicatedMergeTree('/clickhouse/shard_{shard_c1sh4rep}/{database}/{table}','{replica_c1sh4rep}')
ORDER BY (status, idOnPSX, idClient);

TRUNCATE dict.tc_subscriber_tb;
INSERT INTO dict.tc_subscriber_tb (idClient, idOnPSX, status)
SELECT IdClient, IdOnPSX, Status FROM file('/var/lib/clickhouse/user_files/data/TelecomX/telecom1000k/subscribers.csv', CSV);

-- Словарь
DROP DICTIONARY IF EXISTS dict.tc_subscriber ON CLUSTER c1sh4rep;
CREATE DICTIONARY dict.tc_subscriber ON CLUSTER c1sh4rep (
    idClient UUID
    , idOnPSX Int64
    , status String
)
PRIMARY KEY idOnPSX
SOURCE (CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'tc_subscriber_tb' PASSWORD '123456' DB 'dict'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(HASHED());

--- Company
DROP TABLE IF EXISTS dict.tc_company_tb ON CLUSTER c1sh4rep NO DELAY;
CREATE TABLE dict.tc_company_tb ON CLUSTER c1sh4rep
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

TRUNCATE dict.tc_company_tb;
INSERT INTO dict.tc_company_tb (userId, contract, documents, email, idPlan, name, address, phones, contact)
SELECT Id, Contract, Documents, Email, IdPlan, Name, Address, Phones, Contact FROM file('/var/lib/clickhouse/user_files/data/TelecomX/telecom1000k/company.parquet', Parquet);

-- Словарь
DROP DICTIONARY IF EXISTS dict.tc_company ON CLUSTER c1sh4rep;
CREATE DICTIONARY dict.tc_company ON CLUSTER c1sh4rep (
    userId UUID
    , contract String
    , documents String
    , email String
    , idPlan Int64
    , name String
    , address String
    , phones String
    , contact String
)
PRIMARY KEY userId
SOURCE (CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'tc_company_tb' PASSWORD '123456' DB 'dict'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(HASHED());

-- Physical
DROP TABLE IF EXISTS dict.tc_physical_tb ON CLUSTER c1sh4rep NO DELAY;
CREATE TABLE dict.tc_physical_tb ON CLUSTER c1sh4rep
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

TRUNCATE dict.tc_physical_tb;
INSERT INTO dict.tc_physical_tb (userId, contract, documents, email, idPlan, name, address, passport, phones)
SELECT Id, Contract, Documents, Email, IdPlan, Name, Address, Passport, Phones 
FROM file('/var/lib/clickhouse/user_files/data/TelecomX/telecom1000k/physical.parquet', Parquet);

DROP DICTIONARY IF EXISTS dict.tc_physical ON CLUSTER c1sh4rep;
CREATE DICTIONARY dict.tc_physical ON CLUSTER c1sh4rep (
    userId UUID
    , contract String
    , documents String
    , email String
    , idPlan Int64
    , name String
    , address String
    , passport String
    , phones String
)
PRIMARY KEY userId
SOURCE (CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'tc_physical_tb' PASSWORD '123456' DB 'dict'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(HASHED());

-- Plan
DROP TABLE IF EXISTS dict.tc_plan_tb ON CLUSTER c1sh4rep NO DELAY;
CREATE TABLE dict.tc_plan_tb ON CLUSTER c1sh4rep
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

TRUNCATE dict.tc_plan_tb;
INSERT INTO dict.tc_plan_tb (id, name, description, createdAt, updatedAt, closedAt, enabled, attrs)
SELECT Id, Name, Description, CreatedAt, UpdatedAt, ClosedAt, Enabled, Attrs 
FROM file('/var/lib/clickhouse/user_files/data/TelecomX/telecom1000k/plan.json', json);

DROP DICTIONARY IF EXISTS dict.tc_plan ON CLUSTER c1sh4rep;
CREATE DICTIONARY dict.tc_plan ON CLUSTER c1sh4rep (
    id int
    , name String 
    , description String 
    --, createdAt DateTime 
    --, updatedAt DateTime 
    --, closedAt DateTime 
    , enabled Bool 
    , attrs String 
)
PRIMARY KEY id
SOURCE (CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'tc_plan_tb' PASSWORD '123456' DB 'dict'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(HASHED());

-- psxattrs
DROP TABLE IF EXISTS dict.tc_psxattrs_tb ON CLUSTER c1sh4rep NO DELAY;
CREATE TABLE dict.tc_psxattrs_tb ON CLUSTER c1sh4rep
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

TRUNCATE dict.tc_psxattrs_tb;
INSERT INTO dict.tc_psxattrs_tb (id, psx, transmitUnits, delimiter, dateFormat, tz)
SELECT Id, PSX, TransmitUnits, Delimiter, DateFormat, TZ 
FROM file('/var/lib/clickhouse/user_files/data/TelecomX/telecom1000k/psxattrs.csv', csv);

----------  NetFlow ---------------------------

------------------- STG -------------------------
CREATE DATABASE IF NOT EXISTS stg ON CLUSTER c2sh2rep;

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
    , sourceFile String
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

----------------- ODS -------------------
CREATE DATABASE IF NOT EXISTS ods ON CLUSTER c2sh2rep;

DROP TABLE IF EXISTS ods.tc_netflow_rep ON CLUSTER c2sh2rep NO DELAY;
CREATE TABLE ods.tc_netflow_rep ON CLUSTER c2sh2rep
(
    idSession Int64
    , idPSX Int
    , idSubscriber Int
    , startSession DateTime
    , endSession DateTime NULL
    , duration Int
    , upTx Int64
    , downTx Int64
    , sourceFile String
    , created_at DateTime
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/shard_{shard_c2sh2rep}/{database}/{table}', '{replica_c2sh2rep}')
PARTITION BY (toYYYYMMDD(startSession), idPSX)
ORDER BY (idPSX, sourceFile, idSubscriber, idSession, startSession)
TTL created_at + INTERVAL 3 DAY;

DROP TABLE IF EXISTS ods.tc_netflow ON CLUSTER c2sh2rep NO DELAY;
CREATE TABLE ods.tc_netflow ON CLUSTER c2sh2rep
AS ods.tc_netflow_rep
ENGINE = Distributed(c2sh2rep, ods, tc_netflow_rep, idSubscriber);

DROP VIEW IF EXISTS ods.tc_netflow_mv ON CLUSTER c2sh2rep;
CREATE MATERIALIZED VIEW ods.tc_netflow_mv ON CLUSTER c2sh2rep TO ods.tc_netflow_rep
AS
SELECT
    idSession
    , idPSX
    , idSubscriber
    --, parseDateTime(startSession, '%d-%m-%Y %H:%i:%s') as startSesstion
    , parseDateTimeBestEffort(startSession) as startSession
    , CASE WHEN endSession IS NULL
        THEN NULL
        ELSE parseDateTimeBestEffort(endSession)
    END AS endSession
    , duration
    , CASE WHEN idPSX IN (0, 1, 2) 
        THEN intDiv(upTx, 8)
        ELSE upTx
    END AS upTx
    , CASE WHEN idPSX IN (0, 1, 2) 
        THEN intDiv(downTx, 8)
        ELSE downTx
    END AS downTx
    , sourceFile
    , created_at
FROM stg.tc_netflow_rep;

------------ DDS ------------------
CREATE DATABASE IF NOT EXISTS dds ON CLUSTER c2sh2rep;


------------------ DDS AGG ------------------
DROP TABLE IF EXISTS dds.tc_netflow_agg_rep ON CLUSTER c2sh2rep NO DELAY;
CREATE TABLE dds.tc_netflow_agg_rep ON CLUSTER c2sh2rep
(
    idPSX Int   
    , idSession Int64
    , idSubscriber Int
    , startSession AggregateFunction(min, DateTime)
    , endSession AggregateFunction(anyLast, Nullable(DateTime))
    , duration AggregateFunction(sum, Int)
    , upTx AggregateFunction(sum, Int64)
    , downTx AggregateFunction(sum, Int64)
    --, sourceFile String
    , created_at DateTime DEFAULT now()
)
ENGINE = ReplicatedAggregatingMergeTree(
    '/clickhouse/shard_{shard_c2sh2rep}/{database}/{table}', '{replica_c2sh2rep}')
PARTITION BY (toYYYYMMDD(created_at), idPSX)
ORDER BY (idPSX, idSubscriber, idSession);

-- Распределенная обертка
DROP TABLE IF EXISTS dds.tc_netflow_agg ON CLUSTER c2sh2rep NO DELAY;
CREATE TABLE dds.tc_netflow_agg ON CLUSTER c2sh2rep
AS dds.tc_netflow_agg_rep
ENGINE = Distributed(c2sh2rep, dds, tc_netflow_agg_rep, idSubscriber);

DROP VIEW IF EXISTS dds.tc_netflow_agg_mv ON CLUSTER c2sh2rep;
CREATE MATERIALIZED VIEW dds.tc_netflow_agg_mv ON CLUSTER c2sh2rep TO dds.tc_netflow_agg_rep
AS
SELECT
    idPSX 
    , idSession
    , idSubscriber
    , minState(startSession) as startSession
    , anyLastState(endSession) as endSession 
    , sumState(duration) as duration 
    , sumState(upTx) as upTx 
    , sumState(downTx) as downTx 
    --, maxState(sourceFile)
FROM ods.tc_netflow_rep
GROUP BY idPSX, idSession, idSubscriber
;


---------- DM --------------------

CREATE DATABASE IF NOT EXISTS dm ON CLUSTER c2sh2rep;

DROP VIEW IF EXISTS dm.tc_netflow_v ON CLUSTER c2sh2rep;
CREATE OR REPLACE VIEW dm.tc_netflow_v ON CLUSTER c2sh2rep
as select
    idPSX
    , idSession
    , idSubscriber
    , minMerge(startSession) as startSession
    , anyLastMerge(endSession) as endSession 
    , sumMerge(duration) as duration 
    , sumMerge(upTx) as upTx 
    , sumMerge(downTx) as downTx 
    , dictGet(dict.tc_subscriber, 'idClient', idSubscriber) as idClient
    , dictGet(dict.tc_client, 'contract', idClient) as contract
    , dictGet(dict.tc_client, 'email', idClient) as email
    , CASE 
        WHEN dictGet(dict.tc_physical, 'name', idClient) != '' THEN 'Physical'
        WHEN dictGet(dict.tc_company, 'name', idClient) !='' THEN 'Company'
    END AS clientType
    , COALESCE(nullIf(dictGet(dict.tc_company, 'name', idClient), ''), nullIf(dictGet(dict.tc_physical, 'name', idClient), '')) as name
    , arrayMap(
        x -> trim(BOTH '"' FROM x),
        JSONExtractArrayRaw(coalesce(
            nullIf(dictGet(dict.tc_physical, 'phones', idClient), '')
            , nullIf(dictGet(dict.tc_company, 'phones', idClient), ''), ''))
    ) AS phones
    , dictGet(dict.tc_client, 'idPlan', idClient) as idPlan
    , dictGet(dict.tc_plan, 'name', idPlan) as planName
    , dictGet(dict.tc_plan, 'description', idPlan) as planDescription
from dds.tc_netflow_agg
GROUP BY idPSX, idSession, idSubscriber;

-- Data brute
create external table if not exists raw_velo_stations (
    number int,
    name string,
    address string,
    position struct<lat:double, lng:double>, --constuction de l'objet position pour les requêtes métier
    bike_stands int,
    available_bike_stands int,
    available_bikes int,
    status string,
    last_update bigint)
row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
location 'hdfs://namenode:9000/data-lake/raw/velo_lyon/';

-- DAta MR1
create external table if not exists processed_load_factor (
    station_id string,
    avg_load double,
    std_load double,
    ratio string)
row format delimited
fields terminated by '\t'
location 'hdfs://namenode:9000/data-lake/processed/load_metrics/';

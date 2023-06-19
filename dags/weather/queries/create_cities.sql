CREATE OR REPLACE TABLE WEATHER.STAGING.CITIES (CITY_ID VARCHAR, CITY VARCHAR, CITY_RAW VARCHAR) AS
SELECT *
FROM
VALUES ('001', 'Mexico City', 'ciudad-de-mexico'),
    ('002', 'Merida', 'merida'),
    ('003', 'Monterrey', 'monterrey'),
    ('004', 'Wakanda', 'wakanda');
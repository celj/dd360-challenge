CREATE OR REPLACE TABLE WEATHER.STAGING.HOURLY AS (
        WITH SUCCESSFUL_RECORDS AS (
            SELECT A.RECORD_ID,
                A.CITY_ID,
                B.CITY,
                B.CITY_RAW,
                A.REQUEST_ID
            FROM WEATHER.STAGING.REQUESTS A
                JOIN WEATHER.STAGING.CITIES B USING(CITY_ID)
            WHERE REQUEST_STATUS = 200
        )
        SELECT A.CITY_ID,
            A.CITY,
            TO_NUMBER(
                SUBSTR(B.DISTANCE, 1, LENGTH(B.DISTANCE) - 2),
                10,
                2
            ) AS DISTANCE,
            TO_NUMBER(B.HUMIDITY, 10, 2) AS HUMIDITY,
            TO_NUMBER(B.TEMPERATURE, 10, 2) AS TEMPERATURE,
            TO_TIMESTAMP(B.UPDATED_AT, 'DD/MM/YYYY HH24:MI:SS') AS UPDATED_AT,
            A.REQUEST_ID
        FROM SUCCESSFUL_RECORDS A
            JOIN WEATHER.RAW.RECORDS B USING(RECORD_ID, CITY_RAW)
    );
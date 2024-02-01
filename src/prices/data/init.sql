CREATE TABLE car_prices (
    brand VARCHAR(50),
    model VARCHAR(50),
    year INT,
    color VARCHAR(50),
    mileage INT,
    transmission BOOLEAN,
    body_health INT,
    engine_health INT,
    tires_health INT,
    price DECIMAL,
    last_update TIMESTAMP,
    CONSTRAINT unique_car_prices_constraint UNIQUE (brand, model, year, color, mileage, transmission, body_health, engine_health, tires_health)
);



-- COPY car_prices
-- FROM '/docker-entrypoint-initdb.d/used_car_data.csv'
-- DELIMITER ','
-- CSV HEADER;
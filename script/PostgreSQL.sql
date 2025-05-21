CREATE TABLE IF NOT EXISTS total_revenue
(
    timestamp     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_revenue DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS total_quantity
(
    timestamp      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_quantity INT
);

CREATE TABLE IF NOT EXISTS product_revenue
(
    product_id    VARCHAR(255),
    total_revenue DOUBLE PRECISION,
    timestamp     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS store_revenue
(
    store_name    VARCHAR(255),
    total_revenue DOUBLE PRECISION,
    timestamp     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- This file contains the SQL schema definition for the PostgreSQL source database.
-- Table 1: Customers; Table 2: Products; Table 3: Inventory; Table 4: Orders; Table 5: Order Items

CREATE DATABASE retail_db; 

-- Enter into the database
\c retail_db;

-- Once connected to a database, can execute this script from the PostgreSQL shell (psql)

-- Create schema for tables
-- Customer Table: contains information about customers
CREATE TABLE customers (customer_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, first_name VARCHAR(100), last_name VARCHAR(100), email VARCHAR(255) UNIQUE, phone VARCHAR(20), address TEXT, city VARCHAR(100), state VARCHAR(50), zipcode VARCHAR(20), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);

-- Products Table: contains information about products
CREATE TABLE products (product_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, product VARCHAR(255), category VARCHAR(100), price DECIMAL(10,2), cost DECIMAL(10,2), supplier VARCHAR(255), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);

-- Inventory: contains information about the inventory for each product
CREATE TABLE inventory (inventory_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, product_id INT REFERENCES products(product_id), warehouse_location VARCHAR(100), quantity INT, reorder_level INT, last_restocked TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);

-- Orders: contains information about each order
CREATE TABLE orders (order_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, customer_id  , order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, total_amount DECIMAL(12,2), status VARCHAR(50), shipping_address TEXT, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);

-- Order Items: Contains information about the item in a particular order
 CREATE TABLE order_items (order_item_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, order_id INT REFERENCES orders(order_id), product_id INT REFERENCES products(product_id), quantity INT, unit_price DECIMAL(12,2), subtotal DECIMAL(12,2), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);

-- Create Indexes to faster & more efficient querying
CREATE INDEX idx_customer_id ON orders(customer_id);
CREATE INDEX idx_order_date ON orders(order_date);
CREATE INDEX idx_inv_product ON inventory(product_id);
CREATE INDEX idx_order_product ON order_items(order_id);




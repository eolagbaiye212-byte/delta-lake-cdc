# This file auto-generates data for the retail database

# Import required libraries
import psycopg2 # First run "pip install psycopg2-binary" in terminal or command prompt
import random
import requests
import os
from faker import Faker
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables from env_var file
load_dotenv()

# Create Faker
fake = Faker()

# Establish connection to PostgreSQL, enable auto-commit, create cursor
def create_connection():
    conn = psycopg2.connect(host='localhost', database=os.getenv('DB_NAME'), user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD')) 
    conn.autocommit = True
    cursor = conn.cursor()
    return conn, cursor

# Define a function that seeds products (initial generation of 50 products)
def seed_products(cursor):
    # Generate suppliers for products
    suppliers = [fake.company(), fake.company(), fake.company(), fake.company(), fake.company(), fake.company(), fake.company(), fake.company(), fake.company(), fake.company()]

     # Takes products from Fakestore API and converts to JSON. FakeStore API provides 20 products.
    url_1 = "https://fakestoreapi.com/products"
    response_1 = requests.get(url_1)
    all_api_products_1 = response_1.json()

    # Takes products from DummyJson API and converts to JSON. Dummy API provides 30 products.
    url_2 = "https://dummyjson.com/products"
    response_2 = requests.get(url_2)
    data_2 = response_2.json()
    all_api_products_2 = data_2["products"] # Information about the products are nested in a list of dictionaries, so must access the data in products

    # Create a list to hold dictionaries of products
    product_list_1 = []
    product_list_2 = []

    cursor.execute('SELECT COUNT(*) FROM products')
    product_count = cursor.fetchone()[0]

    if product_count == 0:
        for product in all_api_products_1: # Seed products from Fakestore API
            # FakeStore API does not generate costs of the product, nor suppliers. Needs to be created for each product. Takes product price and multiplies by a random distribution between 0.4 and 0.7 to get the cost.
            cost = round(product['price'] * random.uniform(0.4,0.7), 2)

            # Creates a dictionary of product containing it's name, category, price, cost, and supplier
            full_product_1 = {'product':product['title'], 'category':product['category'], 'price':float(product['price']), 'cost': cost, 'supplier': random.choice(suppliers)}

            product_list_1.append(full_product_1)

        for product in all_api_products_2:
            # DummyJSON API does not generate costs of the product, nor suppliers. Needs to be created for each product. Takes product price and multiplies by a random distribution between 0.4 and 0.7 to get the cost.
            cost = round(product['price'] * random.uniform(0.4,0.7), 2)

            full_product_2 = {'product':product['title'], 'category':product['category'], 'price':float(product['price']), 'cost': cost, 'supplier': random.choice(suppliers)}

            product_list_2.append(full_product_2)

        product_list = product_list_1 + product_list_2

        # Inserts into the products table products from the product_list; %(category)s expects a dictionary key named 'category', ...
        cursor.executemany('INSERT INTO products (product, category, price, cost, supplier) VALUES (%(product)s, %(category)s, %(price)s, %(cost)s, %(supplier)s)', product_list)
        print(f"Total number of products created: {len(product_list)}")
        
    elif product_count > 0:
        print(f"Product base already created.")


# Define a function that creates an inventory of the products previously created
def seed_inventory(cursor):
    # Generate warehouse locations for inventory
    warehouse_locations = ["Dallas, TX", "Chicago, IL", "Atlanta, GA", "Long Beach, CA", "Elizabeth, NJ"]

    cursor.execute("SELECT COUNT(*) FROM inventory")
    inventory_count = cursor.fetchone()[0]

    if inventory_count == 0:
        cursor.execute("SELECT product_id FROM products")
        product_id = cursor.fetchall()
        # For each product, provide warehouse location, quantity in-stock, and re-order level
        for product in product_id:
            cursor.execute("INSERT INTO inventory (product_id, warehouse_location, quantity, reorder_level) VALUES (%s, %s, %s, %s)", (product[0], random.choice(warehouse_locations), random.randint(1500, 2500), random.randrange(100, 200, 5)))
    elif inventory_count > 0:
        print("Initial inventory already created.")


# Define a function that seeds customers (initial generations of 100 customers)
def seed_customer(cursor):
    count = 0

    cursor.execute("SELECT COUNT(*) FROM customers")
    customer_count = cursor.fetchone()[0]

    if customer_count == 0:
        while count <= 75: # Initial customer base of 100 customers
            cursor.execute("INSERT INTO customers (first_name, last_name, email, phone, address, city, state, zipcode) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (fake.first_name(), fake.last_name(), fake.email(), fake.phone_number(), fake.address(), fake.city(), fake.state(), fake.zipcode())) 
            count += 1
    elif customer_count > 0:
        print("Initial customer base already created")
        

# Define a function that generates orders
def generate_order(cursor):
    # Define available statuses for shipments
    global status
    status = ['processing', 'shipped', 'delivered']

    # Select random customer
    cursor.execute("SELECT customer_id, address FROM customers ORDER BY RANDOM() LIMIT 1")
    result_customer = cursor.fetchone()
    customer_id = result_customer[0]
    shipping_address = result_customer[1]

    # Select random product 
    cursor.execute("SELECT product_id, price FROM products ORDER BY RANDOM() LIMIT 1")
    result_product = cursor.fetchone() 
    product_id = result_product[0]
    unit_price = result_product[1]

    # Total_amount of order is price * random integer btw 1 & 20.
    quantity_order = random.randint(1,20)
    total_amount = unit_price * quantity_order

    # Insert order if the product quantity is available; Otherwise, let the customer know the item is out of stock
    cursor.execute("SELECT quantity FROM inventory WHERE product_id = %s", (product_id,))
    product_order_quantity = cursor.fetchone()[0]
 
    if product_order_quantity < quantity_order:
        print("We couldn't process your order because this item is out of stock. It'll be back soon—please check again!")
    else: # Isert order into the orders table
        cursor.execute("INSERT INTO orders (customer_id, total_amount, status, shipping_address) VALUES (%s, %s, %s, %s) RETURNING order_id", (customer_id, total_amount, status[0], shipping_address))
        order_id = cursor.fetchone()[0]

    # Update inventory
    cursor.execute("UPDATE inventory SET quantity = quantity - %s WHERE product_id = %s", (quantity_order, product_id))
    print(f"Inserted order with ID: {order_id}")

    # Update order_items table
    cursor.execute("INSERT INTO order_items (order_id, product_id, quantity, unit_price, subtotal) VALUES (%s, %s, %s, %s, %s)", (order_id, product_id, quantity_order, unit_price, total_amount))

 
# Define a function that generates customers
def generate_customer(cursor):
    # Insert new customer
    cursor.execute("INSERT INTO customers (first_name, last_name, email, phone, address, city, state, zipcode) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING customer_id", (fake.first_name(), fake.last_name(), fake.email(), fake.phone_number(), fake.address(), fake.city(), fake.state(), fake.zipcode()))
    print(f"Inserted customer with ID: {cursor.fetchone()[0]}")


# Define a function that updates the status of an order
def update_order_status(cursor):
    # Select random order
    cursor.execute("SELECT order_id, status FROM orders ORDER BY RANDOM() LIMIT 1")
    results_order = cursor.fetchone()
    order_id = results_order[0]
    order_status = results_order[1]
    
    # If order status processing, update to shipped. If status shipped, update to delivered.
    if order_status == 'processing':
        next_status = 'shipped'
        cursor.execute("UPDATE orders SET status = %s WHERE order_id = %s", (next_status, order_id))
        print(f"Updated the status of order {order_id} to {next_status}")
    elif order_status == 'shipped':
        next_status = 'delivered'
        cursor.execute("UPDATE orders SET status = %s WHERE order_id = %s", (next_status, order_id))
        print(f"Updated the status of order {order_id} to {next_status}")

    cursor.execute("UPDATE orders SET updated_at = CURRENT_TIMESTAMP WHERE order_id = %s", (order_id,))

  
# Define function that updates product inventory
def update_inventory(cursor):
    # Collect a list of tuples of all the products currently in inventory
    cursor.execute("SELECT product_id, quantity, reorder_level FROM inventory")
    products = cursor.fetchall()

    # Iterate through the products - if the product quantity is below the reorder level, order new product
    for product in products:
        product_id = product[0]
        quantity = product[1]
        reorder_level  = product[2]
        new_stock_level = random.randint(750,1000)

        if quantity < reorder_level:
            cursor.execute("UPDATE inventory SET quantity = %s, last_restocked = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE product_id = %s", (new_stock_level, product_id))
            print(f"Updated the stock of product with ID {product_id} to {new_stock_level}")


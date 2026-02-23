# This script orchestrates the retail data generations


# Importfrom generate_data.py module and other required modules
import generate_data
import sys
import time
import random

if __name__ == "__main__":
    print("=" * 60)
    print("RETAIL DATA GENERATOR")
    print("=" * 60)

    # One-time seeding of customers, products, and inventory
    conn, cursor = generate_data.create_connection()
    generate_data.seed_products(cursor)
    generate_data.seed_inventory(cursor)
    generate_data.seed_customer(cursor)

    print("=" * 60)
    print("Initial seeding complete. Generating continuous data.")
    print("=" * 60)

    possible_actions = ["generate_order", "update_order_status", "generate_customer", "update_inventory"]
    weights = [50.0, 25.0, 12.5, 12.5]
    execution_count = 0

    try:
        generate_data.generate_order(cursor)
        while True:
            action = random.choices(possible_actions, weights)[0]

            try:
                if action == "generate_order":
                    generate_data.generate_order(cursor)
                elif action == "update_order_status":
                    generate_data.update_order_status(cursor)
                elif action == "generate_customer":
                    generate_data.generate_customer(cursor)
                elif action == "update_inventory":
                    generate_data.update_inventory(cursor)
                execution_count += 1
            except Exception as e:
                print(f"Error in {action}: {e}")
                sys.exit(1)

            time.sleep(random.uniform(1,3)) 

    except KeyboardInterrupt:
        print("=" * 60)
        print("Stopping data generation...")
        print(f"{execution_count} operations executed")
        print("=" * 60)

    finally:
        cursor.close()
        conn.close()
        print("Database connection closed.")
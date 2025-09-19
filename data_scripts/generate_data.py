from faker import Faker
import random
import csv

fake = Faker()

# Generate Customers
with open('data/customers.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['customer_id', 'name', 'email', 'age', 'gender', 'loyalty_status'])
    for i in range(1, 201):
        writer.writerow([
            i,
            fake.name(),
            fake.email(),
            random.randint(18, 65),
            random.choice(['Male', 'Female']),
            random.choice(['Gold', 'Silver', 'Bronze'])
        ])

# Generate Products
with open('data/products.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['product_id', 'product_name', 'category', 'brand', 'price'])
    for i in range(1, 51):
        writer.writerow([
            i,
            fake.word().capitalize(),
            random.choice(['Electronics', 'Clothing', 'Home', 'Beauty']),
            fake.company(),
            round(random.uniform(10.0, 500.0), 2)
        ])

# Generate Stores
with open('data/stores.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['store_id', 'store_name', 'region'])
    for i in range(1, 11):
        writer.writerow([
            i,
            fake.company(),
            random.choice(['North', 'South', 'East', 'West'])
        ])

# Generate Sales
with open('data/sales_data.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['sale_id', 'product_id', 'customer_id', 'store_id', 'date', 'quantity', 'amount'])
    for i in range(1, 1001):
        quantity = random.randint(1, 5)
        price = round(random.uniform(10.0, 500.0), 2)
        writer.writerow([
            i,
            random.randint(1, 50),
            random.randint(1, 200),
            random.randint(1, 10),
            fake.date_between(start_date='-1y', end_date='today'),
            quantity,
            round(quantity * price, 2)
        ])

from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

# Kafka producer configuration
producer = KafkaProducer(
    bootstrap_servers=["localhost:29092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Sample data
customers = [f"CUST-{i:03d}" for i in range(1, 21)]
products = [f"PROD-{i:03d}" for i in range(1, 11)]
statuses = ["pending", "confirmed", "cancelled"]


def generate_order():
    """Generate a random order"""
    order_id = f"ORD-{random.randint(1000, 9999)}"
    return {
        "order_id": order_id,
        "customer_id": random.choice(customers),
        "product_id": random.choice(products),
        "quantity": random.randint(1, 10),
        "price": round(random.uniform(10.0, 500.0), 2),
        "order_timestamp": datetime.now().isoformat(),
        "status": random.choice(statuses),
    }


def send_orders(num_orders=100, delay=1):
    """Send orders to Kafka"""
    print(f"🚀 Starting to send {num_orders} orders...")

    for i in range(num_orders):
        order = generate_order()
        producer.send("orders", value=order)

        print(
            f"✅ Sent order {i + 1}/{num_orders}: {order['order_id']} - ${order['price']:.2f}"
        )
        time.sleep(delay)

    producer.flush()
    print(f"\n🎉 Successfully sent {num_orders} orders!")


if __name__ == "__main__":
    # Send 50 orders with 1 second delay
    send_orders(num_orders=50, delay=1)

from faker import Faker
import json
import random
import time
from kafka import KafkaProducer
import multiprocessing

fake = Faker()


def generate_car_data(generator_id):
    brand = fake.random_element(elements=(
        'BMW',
        'Toyota',
        # 'Honda',
        # 'Ford',
        # 'Chevrolet',
        # 'Mercedes',
        # 'Audi',
        # 'Nissan',
        # 'Hyundai',
        # 'Volkswagen'
    ))

    # Include additional details for all records
    model_number = random.randint(1, 3)

    additional_info = {
        # 'color': fake.random_element(elements=('Black', 'White', 'Silver', 'Blue', 'Red')),
        'color': fake.random_element(elements=('Black', 'White')),
        'mileage': fake.random_int(min=5000, max=50000),
        'transmission': fake.random_element(elements=('Automatic', 'Manual')),
        'body_health': random.randint(1, 5),  # Dynamic value for body health
        'engine_health': random.randint(1, 5),  # Dynamic value for engine health
        'tires_health': random.randint(1, 5),  # Dynamic value for tires health
    }

    # Introduce variability in the structure - randomly set some values to null
    if random.random() < 0.2:
        additional_info['color'] = None
    if random.random() < 0.2:
        additional_info['mileage'] = None
    if random.random() < 0.2:
        additional_info['transmission'] = None
    if random.random() < 0.2:
        additional_info['body_health'] = None
    if random.random() < 0.2:
        additional_info['engine_health'] = None
    if random.random() < 0.2:
        additional_info['tires_health'] = None

    model = f"{brand}_{model_number}"

    # year = fake.random_int(min=2000, max=2023)
    year = fake.random_int(min=2000, max=2005)
    price = random.randint(5000, 50000)

    car_data = {
        'brand': brand,
        'model': model,
        'year': year,
        'price': price,
        'additional_info': additional_info,
        'ad_publish_time': int(time.time()),
        'producer_id': f'website_{generator_id}'
    }

    return json.dumps(car_data)


def produce_car_data(producer, topic, generator_id):
    while True:
        car_data = generate_car_data(generator_id)
        producer.send(topic, car_data)
        time.sleep(random.randint(30,180))  # Add a delay to simulate real-time data


def run_data_generator(generator_id, kafka_servers, topic):
    producer = KafkaProducer(
        bootstrap_servers=kafka_servers,
        value_serializer=lambda v: str(v).encode('utf-8')
    )

    print(f"Generator {generator_id} started.")

    # Generate and produce fake car data to Kafka topic continuously
    produce_car_data(producer, topic, generator_id)


if __name__ == "__main__":
    # Configure Kafka parameters
    kafka_bootstrap_servers = 'localhost:9092'  # Update with your Kafka bootstrap servers
    kafka_topic = 'car_prices'

    # Run multiple data generators as separate processes
    num_generators = 3  # Adjust the number of generators as needed
    processes = []

    for i in range(num_generators):
        process = multiprocessing.Process(target=run_data_generator, args=(i + 1, kafka_bootstrap_servers, kafka_topic))
        processes.append(process)
        process.start()

    try:
        for process in processes:
            process.join()
    except KeyboardInterrupt:
        for process in processes:
            process.terminate()
            process.join()

    print("All generators stopped.")

import json
import random
import time
from datetime import datetime
from confluent_kafka import Producer

conf = {
    'bootstrap.servers': 'ed-kafka:29092'
}

producer = Producer(conf)
TOPIC = 'trafic-data'

def generate_vehicle_data(vehicle_id):
    base_speed = random.randint(60, 80)
    recent_speeds = [base_speed + random.randint(-3, 3) for _ in range(4)]

    nearby_sensors = [
        {
            "sensor_id": f"S{random.randint(1, 5)}",
            "signal_strength": random.randint(-80, -50)
        }
        for _ in range(2)]

    return {
        "vehicle_id": vehicle_id,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "speed_kmh": recent_speeds[-1],
        "recent_speeds": recent_speeds,
        "location": {
            "lat": round(37.7749 + random.uniform(-0.01, 0.01), 6),
            "lon": round(-122.4194 + random.uniform(-0.01, 0.01), 6)
        },
        "road_id": random.choice(["NHX1", "NHX2", "NHX3"]),
        "nearby_sensors": nearby_sensors
    }

try:
    while True:
        vehicle_id = f"V{random.randint(1000, 9999)}"
        message = generate_vehicle_data(vehicle_id)
        json_data = json.dumps(message)
        producer.produce(TOPIC, key=vehicle_id, value=json_data)
        time.sleep(1)

except KeyboardInterrupt:
    print("Producer stopped by user.")
finally:
    producer.flush()

#!/usr/bin/env python3
"""Test Phase 0 -> Phase 1 pipeline"""
import pika
import json
import uuid
from pathlib import Path
import time

# Path to auth-service.zip
sut_zip = Path.home() / "KIMVIWARE" / "KIMVIEware-System-kimvieware-sut-timetables" / "auth-service.zip"

if not sut_zip.exists():
    print(f"❌ {sut_zip} not found")
    print("Creating ZIP...")
    import subprocess
    subprocess.run([
        "zip", "-r", str(sut_zip),
        str(sut_zip.parent / "auth-service"),
        "-x", "*__pycache__/*", "*/.pytest_cache/*"
    ])

# Connect RabbitMQ
credentials = pika.PlainCredentials('admin', 'kimvie2025')
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost', credentials=credentials)
)
channel = connection.channel()
channel.queue_declare(queue='submission.new', durable=True)

# Submit
job_id = str(uuid.uuid4())
message = {
    'job_id': job_id,
    'sut_path': str(sut_zip),
    'user_id': 'test_user',
    'timestamp': '2026-01-15T12:00:00Z'
}

channel.basic_publish(
    exchange='',
    routing_key='submission.new',
    body=json.dumps(message),
    properties=pika.BasicProperties(delivery_mode=2)
)

print(f"✅ Submitted job: {job_id}")
print(f"   SUT: {sut_zip.name}")
print(f"\n⏳ Waiting for processing...")
print(f"   Watch Terminal 1 (Validator)")
print(f"   Watch Terminal 2 (Extractor)")

connection.close()

# Wait and check results
time.sleep(5)

print(f"\n🔍 Checking results...")

# Check extraction.completed queue
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost', credentials=credentials)
)
channel = connection.channel()
channel.queue_declare(queue='extraction.completed', durable=True)

method, properties, body = channel.basic_get(queue='extraction.completed', auto_ack=True)

if method:
    result = json.loads(body)
    print(f"\n✅ PIPELINE SUCCESS!")
    print(f"   Job ID: {result['job_id']}")
    print(f"   Status: {result['status']}")
    print(f"   Language: {result['sut_info']['language']}")
    print(f"   Trajectories: {result['trajectories_count']}")
    print(f"\n📊 First trajectory sample:")
    if result.get('trajectories'):
        t = result['trajectories'][0]
        print(f"   - Path ID: {t['path_id']}")
        print(f"   - Blocks: {len(t['basic_blocks'])}")
        print(f"   - Constraints: {len(t['constraints'])}")
        print(f"   - Cost: {t['cost']}")
else:
    print(f"❌ No result yet, wait more...")

connection.close()

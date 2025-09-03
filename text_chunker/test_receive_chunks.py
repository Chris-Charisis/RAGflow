import json
import pika
import os

def main():
    url = os.getenv("RABBITMQ_URL", "localhost")
    user = os.getenv("RABBITMQ_USER", "guest")
    pwd = os.getenv("RABBITMQ_PASSWORD", "guest")
    exch = os.getenv("OUTPUT_EXCHANGE", "events")
    queue = os.getenv("OUTPUT_QUEUE", "chunks")
    rk = os.getenv("OUTPUT_ROUTING_KEY", "chunks")

    creds = pika.PlainCredentials(user, pwd)
    conn = pika.BlockingConnection(pika.ConnectionParameters(host=url, credentials=creds))
    ch = conn.channel()

    ch.exchange_declare(exchange=exch, exchange_type="topic", durable=True)
    ch.queue_declare(queue=queue, durable=True)
    ch.queue_bind(queue=queue, exchange=exch, routing_key=rk)

    def callback(_ch, _method, _props, body):
        data = json.loads(body)
        chunk_index = data["chunk"]["index"]
        print(" [x] Received chunk:", chunk_index)

        # Ensure outputs directory exists
        os.makedirs("outputs", exist_ok=True)
        # Save body to file
        with open(f"outputs/{data['doc_id'].replace('/', '_')}_chunk_{chunk_index}.json", "w") as f:
            json.dump(data, f, indent=4)

    ch.basic_consume(queue=queue, on_message_callback=callback, auto_ack=True)
    print(" [*] Waiting for chunk messages. Ctrl+C to exit.")
    ch.start_consuming()

if __name__ == "__main__":
    main()
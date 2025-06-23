import sys, os, json
import pika

def main():
    credentials = pika.PlainCredentials("user", "password")
    parameters = pika.ConnectionParameters(host='localhost', credentials=credentials)
    connection = pika.BlockingConnection(parameters=parameters)
    channel = connection.channel()

    channel.exchange_declare(exchange="events", exchange_type="topic", durable=True)
    channel.queue_declare(queue="text", durable=True)
    channel.queue_bind(queue="text",exchange="events",routing_key="text")

    def callback(ch, method, properties, body):
        print(f" [x] Received {json.loads(body)}")



    channel.basic_consume(queue='text',
                        auto_ack=True,
                        on_message_callback=callback)


    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
def test_init_kafka_consumer(self):
    consumer = init_kafka_consumer('test_topic')
    self.assertIsNotNone(consumer)
    # Additional assertions can be added depending on the desired behavior

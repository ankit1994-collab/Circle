import os
import re
from kafka import KafkaConsumer

# ---------------------------------------------------------------------------- #

def create_kafka_consumer(topic='movielog7'):
    '''
    Creates a Kafka consumer

    Args:
        topic (str, optional): The topic to consume from

    Returns:
        consumer (KafkaConsumer): The Kafka consumer
    '''
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        auto_commit_interval_ms=1000
    )
    return consumer

# ---------------------------------------------------------------------------- #

def process_rating_message(message, csv_file_path_rating):
    '''
    Processes a rating message
    
    Args:
        message (str): The message to process
        csv_file_path_rating (str): The path to the csv file to write the processed message to

    Returns:
        None
    '''
    search_str = "/rate/"
    index_ = message.index(search_str)
    rating = message[-1:]
    user_id = re.sub(r"\D", "", message.split(",")[1].strip())
    movie_id = re.sub(r"[^a-zA-Z0-9+]", "", message[index_ + len(search_str):-2])

    final_message = f"{user_id},{movie_id},{rating}"
    append_to_file(csv_file_path_rating, final_message)

# ---------------------------------------------------------------------------- #

def process_time_message(message, csv_file_path_time):
    '''
    Processes a time message
    
    Args:
        message (str): The message to process
        csv_file_path_time (str): The path to the csv file to write the processed message to

    Returns:
        None
    '''
    search_str_1 = "/m/"
    index_ = message.index(search_str_1)
    time = re.sub(r"\D", "", message[message.rfind("/") + 1:-4])
    user_id = re.sub(r"\D", "", message.split(",")[1].strip())
    movie_id = re.sub(r"[^a-zA-Z0-9+]", "", message[index_ + len(search_str_1): message.rfind("/")])

    final_message = f"{user_id},{movie_id},{time}"
    append_to_file(csv_file_path_time, final_message)

# ---------------------------------------------------------------------------- #

def append_to_file(file_path, content):
    '''
    Appends a line to a file
    
    Args:
        file_path (str): The path to the file to append to
        content (str): The content to append to the file
        
    Returns:
        None
    '''
    with open(file_path, 'a') as file:
        file.write(content + "\n")

# ---------------------------------------------------------------------------- #

def telemetry_data_collection_pipeline(csv_file_path_rating='kafka_user_telemetry_rating.csv', csv_file_path_time='kafka_user_telemetry_time.csv', topic='movielog7', verbose=False):
    '''
    Collects telemetry data from Kafka and writes it to a csv file

    Args:
        csv_file_path_rating (str, optional): The path to the csv file to write the rating telemetry data to
        csv_file_path_time (str, optional): The path to the csv file to write the time telemetry data to
        topic (str, optional): The topic to consume from
        verbose (bool, optional): Whether to print verbose messages

    Returns:
        None
    '''
    
    consumer = create_kafka_consumer(topic)

    if os.path.exists(csv_file_path_rating):
        os.remove(csv_file_path_rating)

    if os.path.exists(csv_file_path_time):
        os.remove(csv_file_path_time)

    for message in consumer:
        try:
            message = message.value.decode()
            if "/rate/" in message:
                process_rating_message(message, csv_file_path_rating)

            if "/m/" in message:
                process_time_message(message, csv_file_path_time)

        except Exception as e:
            if verbose:
                print(message)
                print(f"Error occurred while processing message: {message}")
                print(f"Error message: {str(e)}")
            continue

import pika
import json

import media_downloader_pb2
import media_downloader_pb2_grpc
import rabbitMQ_helpers
import grpc

from ultralytics import YOLO

def main():
    global media_downloader_stub
    channel = grpc.insecure_channel("media-downloader:50052")
    media_downloader_stub = media_downloader_pb2_grpc.MediaDownloaderStub(channel)
    
    # Connect to rabbitMQ
    channel, connection = rabbitMQ_helpers.producer_connection_init()

    rabbitMQ_helpers.listen('media.1', callback)

    rabbitMQ_helpers.connection_end(connection, channel)


def callback(ch, method, properties, body):
    """
    Callback function to handle incoming messages from RabbitMQ.
    """
    data = json.loads(body)
    media_id = data['ID']
    media_URI = data.get('MediaURI')
    
    print(f"Received message: {media_URI}")
    
    media_response = media_downloader_stub.RequestMedia(
        media_downloader_pb2.RequestMediaRequest(media_uri=media_URI)
    )

    # Load a pretrained YOLO model
    model = YOLO("yolo11n.pt")

    # Perform object detection on an image
    results = model(media_response.media_path)
    elements = []

    # Visualize the results
    for result in results:
        boxes = result.boxes
        for box in boxes:
            
            c = box.cls
            element = model.names[int(c)]
            if element not in elements:
                elements.append(element)
                result = {
                    'taggingValue': element,
                    'mediaID': media_id
                }
                rabbitMQ_helpers.publish_message(ch, 'tagging.not_added.1.Image Classification', json.dumps(result))
    print(f"Detected elements: {elements}")
    
    media_downloader_stub.ReleaseMedia(
        media_downloader_pb2.ReleaseMediaRequest(media_uri=media_URI)
    )
        
    print("Finished processing the message")
    return

if __name__ == "__main__":
    main()
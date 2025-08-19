import pika
import json

import media_downloader_pb2
import media_downloader_pb2_grpc
import rabbitMQ_helpers
import grpc

from exif import Image
from datetime import datetime
    

def main():
    print("Starting EXIF Extractor Plugin...", flush=True)
    
    print("Connecting to Media Downloader gRPC service...", flush=True)
    global media_downloader_stub
    channel = grpc.insecure_channel("media-downloader:50052")
    media_downloader_stub = media_downloader_pb2_grpc.MediaDownloaderStub(channel)
    print("Connected to Media Downloader gRPC service.", flush=True)

    print("Connecting to RabbitMQ...", flush=True)

    # Connect to rabbitMQ
    channel, connection = rabbitMQ_helpers.producer_connection_init()

    rabbitMQ_helpers.listen('media.*', callback)

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
    try:
        image = Image(open(media_response.media_path, 'rb').read())
    except Exception as e:
        print(f"Error reading image {media_URI}: {e}")
        return
        
    if image.has_exif:  
        if 'datetime' in image.list_all():
            exif_datetime = image.datetime
            # Convert EXIF datetime format "YYYY:MM:DD HH:MM:SS" to "YYYY-MM-DD HH:MM:SS"
            exif_datetime = datetime.strptime(exif_datetime, "%Y:%m:%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
            response = {
                'taggingValue': exif_datetime,
                'mediaID': media_id
            }
            rabbitMQ_helpers.publish_message(ch, 'tagging.not_added.2.Timestamp UTC', json.dumps(response))
            print(f"Published EXIF datetime: {exif_datetime} for media ID: {media_id}")
        if 'gps_latitude' in image.list_all() and 'gps_longitude' in image.list_all() and 'gps_latitude_ref' in image.list_all() and 'gps_longitude_ref' in image.list_all():
            # Convert (deg, min, sec) to float
            def dms_to_float(dms, ref):
                deg, min_, sec = dms
                value = deg + min_ / 60 + sec / 3600
                if ref in ['S', 'W']:
                    value = -value
                return value

            gps_latitude = dms_to_float(image.gps_latitude, image.gps_latitude_ref)
            gps_longitude = dms_to_float(image.gps_longitude, image.gps_longitude_ref)
            response = {
                'taggingValue': f"{gps_latitude} {gps_longitude}",
                'mediaID': media_id
            }
            rabbitMQ_helpers.publish_message(ch, 'tagging.not_added.1.Location', json.dumps(response))
            print(f"Published GPS coordinates: {gps_latitude}, {gps_longitude} for media ID: {media_id}")
    print("EXIF data processed, releasing media...")
    media_downloader_stub.ReleaseMedia(
        media_downloader_pb2.ReleaseMediaRequest(media_uri=media_URI)
    )
    print("Finished processing the message")
    return

if __name__ == "__main__":
    main()
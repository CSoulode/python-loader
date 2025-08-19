import face_recognition
import glob
import pika
import json
import os
from threading import Thread

import media_downloader_pb2
import media_downloader_pb2_grpc
import rabbitMQ_helpers
import grpc

from PIL import Image

known_face_dir = '/app/known_faces'
known_faces_encodings = {}
    

def main():
    # Load all the pictures in the known_faces directory
    known_face_files = glob.glob(known_face_dir + '/*.jpg')
    known_face_files += glob.glob(known_face_dir + '/*.png')
    known_face_files += glob.glob(known_face_dir + '/*.jpeg')
    known_face_files += glob.glob(known_face_dir + '/*.JPG')
    known_face_files += glob.glob(known_face_dir + '/*.PNG')
    known_face_files += glob.glob(known_face_dir + '/*.JPEG')
    
    print(f"Found {len(known_face_files)} known faces in {known_face_dir}")
    
    for known_face_file in known_face_files:
        image = face_recognition.load_image_file(known_face_file)
        face_encoding = face_recognition.face_encodings(image)
        if face_encoding:
            filename = os.path.splitext(os.path.basename(known_face_file))[0]
            known_faces_encodings[filename] = face_encoding[0]
            print(f"Loaded known face {filename} from {known_face_file}")


    global media_downloader_stub
    channel = grpc.insecure_channel("media-downloader:50052")
    media_downloader_stub = media_downloader_pb2_grpc.MediaDownloaderStub(channel)

    # Connect to rabbitMQ
    channel, connection = rabbitMQ_helpers.producer_connection_init()

    tagging_updates_thread = Thread(target=rabbitMQ_helpers.listen, args=('tag_update.faces', tag_update_callback))
    tagging_updates_thread.start()

    media_thread = Thread(target=rabbitMQ_helpers.listen, args=('media.1', callback))
    media_thread.start()
    
    tagging_updates_thread.join()
    media_thread.join()

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
    
    unknown_image = face_recognition.load_image_file(media_response.media_path)
    unknown_face_locations = face_recognition.face_locations(unknown_image)
    number_of_faces = len(unknown_face_locations)
    response = {
        'taggingValue': str(number_of_faces),
        'mediaID': media_id
    }
    rabbitMQ_helpers.publish_message(ch, 'tagging.not_added.5.Number of faces', body=json.dumps(response))
    for unknown_face_location in unknown_face_locations:
        unknown_face_encoding = face_recognition.face_encodings(unknown_image, [unknown_face_location])[0]
        if len(known_faces_encodings) <= 1:
            face_name = "Unknown" + str(len(known_faces_encodings)+1)
            response = {
                'taggingValue': face_name,
                'mediaID': media_id
            }
            rabbitMQ_helpers.publish_message(ch, 'tagging.not_added.1.faces', body=json.dumps(response))
            known_faces_encodings[face_name] = unknown_face_encoding
            img = Image.open(media_response.media_path)
            top, right, bottom, left = unknown_face_location
            # Add a margin of 20% and respect image boundaries
            width, height = img.size
            face_width = right - left
            face_height = bottom - top
            margin_x = int(face_width * 0.2)
            margin_y = int(face_height * 0.2)
            new_left = max(left - margin_x, 0)
            new_top = max(top - margin_y, 0)
            new_right = min(right + margin_x, width)
            new_bottom = min(bottom + margin_y, height)
            img_cropped = img.crop((new_left, new_top, new_right, new_bottom))
            img_cropped.save(known_face_dir + '/' + face_name + '.jpg')
            print(f"Saved new face {face_name} in {known_face_dir}")
            continue
        known_faces_encodings_list = list(known_faces_encodings.values())
        results = face_recognition.compare_faces(known_faces_encodings_list, unknown_face_encoding, tolerance=0.6)
        names = list(known_faces_encodings.keys())
        for i, result in enumerate(results):
            if result:
                print(f"Found {names[i]} in {media_URI}")
                # Send the result back to RabbitMQ
                response = {
                    'taggingValue': names[i],
                    'mediaID': media_id
                }
                rabbitMQ_helpers.publish_message(ch, 'tagging.not_added.1.faces', body=json.dumps(response))
                print(f"Found new {names[i]} in {media_URI} with {len(known_faces_encodings)} known faces")
                break
        else:
            face_name = "Unknown" + str(len(known_faces_encodings)+1)
            response = {
                'taggingValue': face_name,
                'mediaID': media_id
            }
            rabbitMQ_helpers.publish_message(ch, 'tagging.not_added.1.faces', body=json.dumps(response))
            print(f"Found new {face_name} in {media_URI} with {len(known_faces_encodings)} known faces")
            known_faces_encodings[face_name] = unknown_face_encoding
            img = Image.open(media_response.media_path)
            top, right, bottom, left = unknown_face_location
            # Add a margin of 20% and respect image boundaries
            width, height = img.size
            face_width = right - left
            face_height = bottom - top
            margin_x = int(face_width * 0.2)
            margin_y = int(face_height * 0.2)
            new_left = max(left - margin_x, 0)
            new_top = max(top - margin_y, 0)
            new_right = min(right + margin_x, width)
            new_bottom = min(bottom + margin_y, height)
            img_cropped = img.crop((new_left, new_top, new_right, new_bottom))
            img_cropped.save(known_face_dir + '/' + face_name + '.jpg')
            
    media_downloader_stub.ReleaseMedia(
        media_downloader_pb2.ReleaseMediaRequest(media_uri=media_URI)
    )            
            
    print("Finished processing the message")
    return

def tag_update_callback(ch, method, properties, body):
    """
    Callback function to handle tag update messages.
    """
    
    data = json.loads(body)
    old_name = data['OldName']
    new_name = data['NewName']
    
    print(f"Received tag update: {old_name} -> {new_name}")
    
    if old_name in known_faces_encodings:
        known_faces_encodings[new_name] = known_faces_encodings.pop(old_name)
        old_path = os.path.join(known_face_dir, old_name + '.jpg')
        new_path = os.path.join(known_face_dir, new_name + '.jpg')
        if os.path.exists(old_path):
            os.rename(old_path, new_path)
        print(f"Renamed face {old_name} to {new_name}")
    else:
        print(f"Face {old_name} not found in known faces")

if __name__ == "__main__":
    main()
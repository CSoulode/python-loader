import pika
import json
import torch

import media_downloader_pb2
import media_downloader_pb2_grpc
import rabbitMQ_helpers
import grpc

from lavis.models import load_model_and_preprocess
from PIL import Image
    

def main():
    print("Starting Captioning Plugin...", flush=True)
    
    print("Loading model...", flush=True)
    global device, model, vis_processors
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model, vis_processors, _ = load_model_and_preprocess(name="blip_caption", model_type="base_coco", is_eval=True, device=device)
    print("Model loaded successfully.", flush=True)
    
    global media_downloader_stub
    print("Connecting to Media Downloader gRPC service...", flush=True)
    channel = grpc.insecure_channel("media-downloader:50052")
    media_downloader_stub = media_downloader_pb2_grpc.MediaDownloaderStub(channel)
    print("Connected to Media Downloader gRPC service", flush=True)
    
    print("Connecting to RabbitMQ...", flush=True)
    
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

    raw_image = Image.open(media_response.media_path).convert("RGB")
    # loads BLIP caption base model, with finetuned checkpoints on MSCOCO captioning dataset.
    # this also loads the associated image processors
    # preprocess the image
    # vis_processors stores image transforms for "train" and "eval" (validation / testing / inference)
    image = vis_processors["eval"](raw_image).unsqueeze(0).to(device)
    # generate caption
    caption = model.generate({"image": image})[0]
    
    result = {
        'taggingValue': caption,
        'mediaID': media_id
    }
    rabbitMQ_helpers.publish_message(ch, 'tagging.not_added.1.Caption', json.dumps(result))
    print(f"Generated caption: {caption}")

    media_downloader_stub.ReleaseMedia(
        media_downloader_pb2.ReleaseMediaRequest(media_uri=media_URI)
    ) 
        
    print("Finished processing the message")
    return

if __name__ == "__main__":
    main()
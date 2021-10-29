from PIL import Image
import boto3
import io
import os
import requests
import base64


def download_base64(bucket_id: str, object_id: str, s3):
    s3_response_object = s3.get_object(Bucket=bucket_id, Key=object_id)
    binary_file_data = s3_response_object['Body'].read()
    base64_encoded_data = base64.b64encode(binary_file_data)
    return base64_encoded_data.decode('utf-8'), binary_file_data


def get_faces(image: str):
    response = requests.post('https://api-us.faceplusplus.com/facepp/v3/detect', data={
        'api_key': os.getenv('facepp_api_key'),
        'api_secret': os.getenv('facepp_api_secret'),
        'image_base64': image,
    })
    if response.status_code == 200:
        return response.json()['faces']


def crop_and_save(face_rectangle, file_bytes, s3, bucket_id, new_name):
    image = Image.open(io.BytesIO(file_bytes))
    left = face_rectangle['left']
    top = face_rectangle['top']
    width = face_rectangle['width']
    height = face_rectangle['height']
    face_img = image.crop((left, top, left + width, top + height))
    byte_io = io.BytesIO()
    face_img.save(byte_io, 'JPEG')
    s3.put_object(Body=byte_io.getvalue(), Bucket=bucket_id, Key=new_name)


def get_face_bytes(image):
    cropped_bytes = io.BytesIO()
    image.save(cropped_bytes, 'PNG')
    return cropped_bytes.getvalue()


def handler(event, context):
    try:
        session = boto3.session.Session()
        s3 = session.client(
            service_name='s3',
            aws_access_key_id=os.getenv('aws_id'),
            aws_secret_access_key=os.getenv('aws_secret'),
            endpoint_url='https://storage.yandexcloud.net'
        )
        sqs = boto3.client(
            service_name='sqs',
            aws_access_key_id=os.getenv('aws_id'),
            aws_secret_access_key=os.getenv('aws_secret'),
            endpoint_url='https://message-queue.api.cloud.yandex.net',
            region_name='ru-central1'
        )
        for message in event['messages']:
            details = message['details']
            if (str(details['object_id']).endswith('.jpg') | str(details['object_id']).endswith('.png')) \
                    & ('_face_' not in str(details['object_id'])):
                bucket_id = details['bucket_id']
                object_id = details['object_id']
                image_base64, file = download_base64(bucket_id, object_id, s3)
                faces = get_faces(image_base64)
                new_faces = []
                for i, face in enumerate(faces):
                    name_ext = object_id.split('.')
                    name = name_ext[-2]
                    ext = name_ext[-1]
                    new_object = name + '_face_' + str(i) + '.' + ext
                    crop_and_save(face['face_rectangle'], file, s3, bucket_id, name + '_face_' + str(i) + '.' + ext)
                    new_faces.append(new_object)
                if new_faces:
                    sqs.send_message(
                        QueueUrl=os.getenv('queue_url'),
                        MessageBody=str(new_faces)
                    )

    except BaseException as e:
        print(e)
    return {
        'statusCode': 200,
        'body': 'Hello World!',
    }

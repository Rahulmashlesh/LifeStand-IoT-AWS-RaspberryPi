import json
import datetime as dt
from datetime import timedelta
import boto3
import time

list_of_labels = ['Person', 'Human']

dynamodbClient = boto3.client('dynamodb')
rekognition_client=boto3.client('rekognition')

def lambda_handler(event, context):

    # person state: fall   
    photo='B_fall1.jpg'
    bucket='fall-detector-bucket'
    flag=detect_labels(photo, bucket)
    fall_detector(flag)
   
    return {
        'statusCode': 200,
        #'body': json.dumps('Hello from Lambda!')
    }


def detect_labels(photo, bucket):

    response = rekognition_client.detect_labels(Image={'S3Object':{'Bucket':bucket,'Name':photo}},MaxLabels=10)
    print('Input: ' + photo) 

    # 1 for standing
    # 0 for falling

    flag = 1
    for label in response['Labels']:
        if label['Name'] in list_of_labels:
            print ("Label: " + label['Name'])
            print ("Confidence: " + str(label['Confidence']))
            print ("Instances:")
            for instance in label['Instances']:
                coefficient = instance['BoundingBox']['Height'] / instance['BoundingBox']['Width']
                print("Fall Coefficient = ", coefficient)
                
                if (coefficient <= 1.4):
                    flag = 0 
                    print("Flag: true")
    return flag

def fall_detector(flag):
    patient_db =  dynamodbClient.scan(TableName="P_state")
    fall_time = 0
    zero = 0
    red = "red"
    while (flag == 0 ): # patient has fallen

        if (len(patient_db['Items']) != 0):
            visitor_response = dynamodbClient.get_item(
                TableName="P_state",
                Key= {
                    'fall_time': {
                        'N': fall_time
                    }
                }
            )
            print("Previous fall found")
            print("fall time"+ fall_time)
            if (fall_time < time.time() ):
                print("call emergency")
            else: 
                print("wait for 10 sec")
        else: 
            # new fall detected
            dynamodbClient.put_item(
            TableName="P_state",
            Item = {
                'cam_id': {
                    'S': "cam1"
                },
                'state': {
                    'S': "green"
                }
            }
            )
 

          
import json
import boto3
import base64
import uuid
import botocore.vendored.requests as requests
import time
import random
import cv2

video_client = boto3.client('kinesis-video-media',endpoint_url='https://s-1e415f8b.kinesisvideo.us-east-1.amazonaws.com',region_name='us-east-1')
kinesis_client = boto3.client('kinesisvideo',region_name='us-east-1')
rekognition_client=boto3.client('rekognition')
dynamodbClient = boto3.client('dynamodb')
sns_client = boto3.client('sns')
s3 = boto3.client('s3')
cam_id = 'cam-1'
room = 'JAB474'
address = '6 MetroTech Center, Brooklyn, NY 11201'
lat = '40.69'
lng = '-73.98'

list_of_labels = ['Person', 'Human']

owner_phone_number = "+19293104679"

def insert_p_state(state):
    # searching for standing state
    patient_scan_response = dynamodbClient.scan(TableName="P_state")
    #print(patient_scan_response['Items'][0]['fall_timestamp']['S'])
    if (patient_scan_response['Items'][0]['fall_timestamp']['S'] != '0'):
        # no standing state, he was previously fallen down
        # compare time with current time
        interval = int(time.time()) - int(patient_scan_response['Items'][0]['fall_timestamp']['S'])
        if (interval >= 30):
            print('notify')
            sns_client.publish(
                PhoneNumber = owner_phone_number,
                Message = "Person falling down, please help immediately.",
                MessageStructure='string',
            )
    else:
        # standing state, he was previously standing
        ts = '0' # Standing
        if (state == 0):
            # Falls
            ts = str(int(time.time()))
        dynamodbClient.put_item(
            TableName="P_state",
            Item = {
                'cam_id': {
                    'S': cam_id
                },
                'state':{
                    'S': state
                },
                'fall_timestamp': {
                    'S': ts
                },
                'room': {
                    'S': room
                },
                'address': {
                    'S': address
                },
                'lat': {
                    'S': lat
                },
                'lng': {
                    'S': lng
                }
            }
        )

def get_person_status(response):
    flag = 1
    for label in response['Labels']:
        #print(label)
        if label['Name'] in list_of_labels:
            #print ("Label: " + label['Name'])
            #print ("Confidence: " + str(label['Confidence']))
            #print ("Instances:")
            for instance in label['Instances']:
                coefficient = instance['BoundingBox']['Height'] / instance['BoundingBox']['Width']
                print("Fall Coefficient = ", coefficient)
                if (coefficient <= 1.4):
                    print("Falling Down")
                    flag =  0
                else:
                    print("Standing")
        #else:
            #print("Not this label")
    return flag

def lambda_handler(event, context):
    print("START fall detector")
    #getURL()
    extract_frame(event)
    #jsontest={'Labels': [{'Name': 'Indoors', 'Confidence': 99.05536651611328, 'Instances': [], 'Parents': []}, {'Name': 'Office', 'Confidence': 97.6895751953125, 'Instances': [], 'Parents': [{'Name': 'Indoors'}]}, {'Name': 'Person', 'Confidence': 86.98133850097656, 'Instances': [{'BoundingBox': {'Width': 0.22992587089538574, 'Height': 0.6776954531669617, 'Left': 0.0038875460159033537, 'Top': 0.03988803178071976}, 'Confidence': 86.98133850097656}, {'BoundingBox': {'Width': 0.06513333320617676, 'Height': 0.06691455841064453, 'Left': 0.23244357109069824, 'Top': 0.3577145040035248}, 'Confidence': 83.15800476074219}], 'Parents': []}, {'Name': 'Room', 'Confidence': 86.27561950683594, 'Instances': [], 'Parents': [{'Name': 'Indoors'}]}, {'Name': 'Furniture', 'Confidence': 84.04985046386719, 'Instances': [], 'Parents': []}, {'Name': 'Chair', 'Confidence': 83.70223999023438, 'Instances': [{'BoundingBox': {'Width': 0.1267271488904953, 'Height': 0.17899192869663239, 'Left': 0.7133181095123291, 'Top': 0.4906826913356781}, 'Confidence': 83.70223999023438}], 'Parents': [{'Name': 'Furniture'}]}, {'Name': 'Table', 'Confidence': 76.88232421875, 'Instances': [], 'Parents': [{'Name': 'Furniture'}]}, {'Name': 'Desk', 'Confidence': 74.16146850585938, 'Instances': [], 'Parents': [{'Name': 'Table'}, {'Name': 'Furniture'}]}, {'Name': 'Court', 'Confidence': 62.208011627197266, 'Instances': [], 'Parents': [{'Name': 'Room'}, {'Name': 'Indoors'}]}, {'Name': 'Interior Design', 'Confidence': 57.7260856628418, 'Instances': [], 'Parents': [{'Name': 'Indoors'}]}], 'LabelModelVersion': '2.0', 'ResponseMetadata': {'RequestId': '8debc232-9a3e-4d6a-b245-8d4ebc2fc5f9', 'HTTPStatusCode': 200, 'HTTPHeaders': {'content-type': 'application/x-amz-json-1.1', 'date': 'Fri, 20 Dec 2019 05:05:50 GMT', 'x-amzn-requestid': '8debc232-9a3e-4d6a-b245-8d4ebc2fc5f9', 'content-length': '1447', 'connection': 'keep-alive'}, 'RetryAttempts': 0}}
    #get_person_status(jsontest)
    print("End of main funciton.")
    return {
        'statusCode' : 200,
        'body': json.dumps('Hello from Lambda!')
    }

def extract_frame(event):
    #print("Extracting Frames")
    record = event['Records'][0]
    payload=base64.b64decode(record["kinesis"]["data"])
    #print('Decoded payload:', payload)
    
    payload = json.loads(payload.decode('utf-8'))

    check_frame = False
    face_raw_image='FALL_Video_'
    if 'InputInformation' in payload:
        x = payload["InputInformation"]
        y = x["KinesisVideo"]
        fn = y["FragmentNumber"]
        #print("Print triplet")
        print(x,y,fn)
        stream = video_client.get_media(
            StreamARN='arn:aws:kinesisvideo:us-east-1:044197594723:stream/PiStream/1573482770601',
            StartSelector={
                'StartSelectorType': 'FRAGMENT_NUMBER',
                'AfterFragmentNumber': fn
            }
        )
        
        with open('/tmp/stream.mkv', 'wb') as f:
            streamBody = stream['Payload'].read(480*640)
            f.write(streamBody)
            vcap = cv2.VideoCapture('/tmp/stream.mkv')
            ret, frame = vcap.read()
            if frame is not None:
                # Display the resulting frame
                vcap.set(1, int(vcap.get(cv2.CAP_PROP_FRAME_COUNT)/2)-1)
                face_raw_image=face_raw_image+time.strftime("%Y%m%d-%H%M%S")+'.jpg'
                cv2.imwrite('/tmp/'+face_raw_image,frame)
                s3.upload_file('/tmp/'+face_raw_image, 'cc-project-fall-detector', face_raw_image)
                vcap.release()
                print('Image uploaded to S3 :', face_raw_image)
                check_frame = True
            else:
                print("Frame is None")

    if check_frame: # or faceId:
        #print("Rekgonition startings:")
        rekognition_response=rekognition_client.detect_labels(
                            Image={'S3Object':{'Bucket':'cc-project-fall-detector','Name':face_raw_image}}
                            ,MaxLabels=10)

        print("Reckognition response: ")
        print(rekognition_response)
        flag = get_person_status(rekognition_response)
        #print("flag is: ", flag)
        insert_p_state(str(flag))
        
            




def get_test_faceID(event):
    test_data_faceID = "5cc082a5-e928-4c2c-86a6-f8d43f894765qq"
    test_data_finalKey = "kvs1_20191115-220618.jpg"     
    return test_data_faceID, test_data_finalKey

def face_analyzer(faceID, objectKey):
    print("Analyzing facial data")
    
    if (check_has_a_valid_OPT(faceID)):
        # CASE 1: Visitor has a vaild OTP
        # Do nothing
        return
    
    elif (check_if_Known_Visitor(faceID)):
        # CASE 2: Visitor is a return known Visitor
        # He/She needs a needs a new OTP
        print("Generating new password for known Visitor")
        new_otp = get_random_otp()

        # Update Passcode DB with new OTP and get visitor Phone number
        visitor_phoneNumber =  insert_DB_OTP_in_passcode(faceID, new_otp)

        # Send SMS to Known Visitor
        response_sns_text = 'Welcome. Your new OTP code is : ' + new_otp + ". To access, click : https://mysmartdoor.s3-us-west-2.amazonaws.com/webpage-1/index.html"
        send_sms(visitor_phoneNumber, response_sns_text)
        print("sent a SMS to visitor : " + visitor_phoneNumber + " with the OTP : " + new_otp)
        return
    
    else:
        # CASE 3: Visitor is new, need to notify the owner
        print("New Visitor detected, New faceID found")

        # Prevent D-DOS on Owner        
        if (owner_flood_prevent(faceID)):
            # If sms with OTP was sent during the last two minutes. 
            # Do nothing
            return
        else:
            # Need to send or re-send a SMS to owner
            owner_page_prefix = "https://mysmartdoor.s3-us-west-2.amazonaws.com/webpage-2/index.html" + "?faceID=" + faceID + "&objectKey=" + objectKey
            response_sns_text = 'A new visitor request! To approve it, click: ' + owner_page_prefix

            # Send SMS to owner
            send_sms(owner_phone_number, response_sns_text)
            print("SMS sent to owner Mobile: " + owner_phone_number + " SMS data: " + response_sns_text)

            # Update DB 
            insert_DB_Face_On_Door_Step(faceID)
    return 

def check_has_a_valid_OPT(faceID):
    #return True if, faceId has a valid OTP in the Passcodes DB
    visitor_scan_response = dynamodbClient.scan(TableName="Passcodes")
    print(visitor_scan_response)
    if (len(visitor_scan_response['Items']) != 0):
        for item in visitor_scan_response['Items']:
            if(faceID == item['faceID']['S']):
                passcode = item['passcode']['S']
                ttl = item['ttl']['N']
                if (time.time() < int(ttl)):
                    #if the item is still valid
                    print("Visitor has a active OTP : " + passcode)
                    return True
                print("Visitor OTP has Expired")
                return False
    else:
        #Passcodes db is empty
        print("Passcode DB is empty, no visitors OTP found")
        return False
    return False

def check_if_Known_Visitor(faceID):
    # Return True if, Visitor in known, i.e faceID is found in Visitor DB
    visitor_response = dynamodbClient.get_item(
        TableName="visitors",
        Key= {
            'faceId': {
                'S': faceID
            }
        }
    )
    if (visitor_response.get('Item') is not None):
        print("Known visitor found, faceID: " + faceID)

        return True
    else:
        return False

def insert_DB_Face_On_Door_Step(faceID):   
    dynamodbClient.put_item(
        TableName="faceOnDoorStep",
        Item = {
            'faceID': {
                'S': faceID
            },
            'ttl': {
                'N': str(int(time.time() + 120))
            }
        }
    )

def insert_DB_OTP_in_passcode(faceID, new_otp):
    visitor_response = dynamodbClient.get_item(
        TableName="visitors",
        Key= {
            'faceId': {
                'S': faceID
            }
        }
    )
    visitor_data = json.loads(visitor_response['Item']['data']['S'])
    visitor_phoneNumber = visitor_data['phoneNumber']
    dynamodbClient.put_item(
        TableName="Passcodes",
        Item = {
            'faceID': {
                'S': faceID
            },
            'passcode':{
                'S': new_otp
            },
            'ttl': {
                'N': str(int(time.time() + 300))
            }
        }
    )
    return visitor_phoneNumber

def send_sms(phone_number,response_sns_text):
    sns_client.publish(
        PhoneNumber = phone_number,
        Message = response_sns_text,
        MessageStructure='string',
    )
    print("SMS sent")

def owner_flood_prevent(faceID):
    # Return true if a sms was sent in the last two minutes.
    pre_check_response = dynamodbClient.get_item(
        TableName="faceOnDoorStep",
        Key= {
            'faceID': {
                'S': faceID
            }
        })
    if (pre_check_response.get("Item") is not None):
        timestamp_for_last_sms = pre_check_response['Item']['ttl']['N']
        if (time.time() < int(timestamp_for_last_sms)):
            print("Found a sms to owner: wait for 2 minutes")
            return True
        else:
            return False

def get_random_otp():
    passcode = ''
    while True:
        otp = 0
        for _ in range(1,5):
            otp += random.randint(0,9)
            otp*=10
        passcode = str(otp)
        passcode_response = dynamodbClient.get_item(
            TableName="Passcodes",
            Key = {
                "passcode": {
                    'S': passcode
                }
            }
        )
        if (passcode_response.get("Item") is None):
            print("New OPT: " + passcode)
            break
        else:
            print("Oops!! cretaed new OTP but found duplicate: " + passcode + " Lets Generate a new one")
            get_random_otp()
    return passcode
    
def get_as_base64(url):
    return base64.b64encode(requests.get(url).content)

def getURL():
    response = kinesis_client.get_data_endpoint(StreamARN='arn:aws:kinesisvideo:us-east-1:044197594723:stream/PiStream/1573482770601',APIName='GET_MEDIA')
    print(response)

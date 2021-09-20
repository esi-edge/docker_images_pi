import numpy as np
import threading
from confluent_kafka import Producer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
import concurrent.futures
import cv2
import os
import json
from time import time
import logging
from cameras_config import camera_links

#Creates a kafka topic dinamically
def Create_Cam_Topic(self, camera_name):
    broker_topics = self.adminClient.list_topics().topics

    topic = self.hostname+"-Detection_"+camera_name #"Pi"+self.hostname+"-Detection_"+camera_name

    #create the topic if it doesnt exist among the brokers topics
    if not(topic in broker_topics.keys):
        #NewTopic(topic_name, num_partitions, replication_factor)
        topic_list = [NewTopic(topic, 1, 1)]
        admin_client.create_topics(topic_list)

    return topic

def find_camera(list_id):
    return cameras[int(list_id)]


#Serializes img to bytes to publish them into kafka(messages in kafka are bytes)
def serializeImg(img):
    _, img_buffer_arr = cv2.imencode(".jpg", img)
    img_bytes = img_buffer_arr.tobytes()
    return img_bytes


#The produce method is asynchronous. It doesnt wait for confirmation that the message has been 
# delivered or not. We pass in a callback function that logs the information about the message 
# produced or error if any. Callbacks are executed and sent as a side-effect of calls to the poll 
# or flush methods.
def delivery_report(err, msg):
    if err:
        logging.error("Failed to deliver message: {0}: {1}"
              .format(msg.value(), err.str()))
    else:
        logging.info("msg produced. "+
                    "Topic: {0}".format(msg.topic()) +
                    "Partition: {0}".format(msg.partition()) +
                    "Offset: {0}".format(msg.offset()) +
                    "Timestamp: {0}".format(msg.timestamp()))

    
#finds the objects in an image after yolov3 detection and returns coordinates,confidence & class of the objects as dict
def findObjects(outputs, img, frame_no):
        ht, wt, ct = img.shape
        bbox = []
        classIds = []
        conf = []
        frameObjects = dict()
        frameObjects.update({"Image_Nb": frame_no})
        for output in outputs:
            for det in output:
                scores = det[5:]
                classId = np.argmax(scores)
                confidence = scores[classId]
                #only take objects above 50% confidence
                if confidence > 0.5:
                    w,h = int(det[2]*wt), int(det[3]*ht)
                    x,y = int((det[0]*wt)-w/2), int((det[1]*ht)-h/2)
                    bbox.append([x,y,w,h])
                    classIds.append(classId)
                    conf.append(float(confidence))

        #eliminate other potential boxes using a threshhold of 0.3 
        indices = cv2.dnn.NMSBoxes(bbox,conf,0.5,0.3)
        j = 0
        #updates the dict with the filtered boxes(coordinates,conf,class) as a nested dict
        for i in indices:
            i = i[0]
            box = bbox[i]
            x,y,w,h = box[0],box[1],box[2],box[3]
            j+=1
            conf[i] = round(conf[i], 3)
            classe = classNames[classIds[i]]
            #Only consider certain objects
            if classe == "person" or classe == "backpack" or classe == "handbag" or classe == "suitcase" :
                #each box represents an object having a class,confidence and coordinates
                frameObjects.update({'box'+str(j):{'class':classe, 'conf':conf[i], 
                                                        'x':x, 'y':y, 'w':w, 'h':h}})

        return frameObjects   
    
#count the speed of change
def countMov(fram1,fram2):
        diff = cv2.absdiff(fram1, fram2)
        gray = cv2.cvtColor(diff, cv2.COLOR_BGR2GRAY)
        blur = cv2.GaussianBlur(gray, (25,25), 0)
        _, thresh = cv2.threshold(blur, 20, 255, cv2.THRESH_BINARY)
        dilated = cv2.dilate(thresh, None, iterations=3)
        contours, _ = cv2.findContours(dilated, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
        coun = 0
        for contour in contours:
            (x, y, w, h) = cv2.boundingRect(contour)
            coun+=cv2.contourArea(contour)
        if len(contours)>0:
            if coun/len(contours) <= 5000:
                return coun/len(contours)
            else:
                return 5000.0
        else: return coun

class ProducerThread:
    def __init__(self, hostname, adminClient, kafka_broker_url, netModel):
        self.adminClient = adminClient
        self.producer = Producer({'bootstrap_servers': kafka_broker_url}) #KAFKA_BROKER_URL
        self.hostname = hostname
        self.netModel = netModel

    #get the number of person
    def numPer(self, Objects):
        j = 0
        b = True
        n = 0
        while b:
            j+=1
            if "box"+str(j) in Objects:
                if Objects["box"+str(j)]["class"] == 'person' : n+=1
            else:
                b = False
        return n

    def publishFrame(self, cam):        
        camera_name = "Camera"+str(cam)

        camera = find_camera(cam)
        cap = cv2.VideoCapture(camera)
        frame_no = 0
        while cap.isOpened():
            _, frame = cap.read()
            if _:
                frame_time = time()
                self.topic = Create_Cam_Topic(self, camera_name)
                # applying detection every 5th frame (reduces traffic-load)
                if frame_no % 5 == 0:
                    
                    img = cv2.resize(frame, (224, 224))
                    netModel = self.netModel

                    #creates a blob from the img to pass to the network
                    blob = cv2.dnn.blobFromImage(img,1/255,(wht,wht),[0,0,0],1,crop=False)
                    netModel.setInput(blob)

                    layerNames = netModel.getLayerNames()
                    outputNames = [layerNames[i[0]-1] for i in netModel.getUnconnectedOutLayers()]

                    outputs = netModel.forward(outputNames)
                    frameObjectss = findObjects(outputs,img, frame_no)

                    #get number of persons in the frame
                    nb_people = self.numPer(frameObjectss)

                    #get speed of change
                    speed_change = 0
                    oldframe = None
                    if oldframe == None:
                        oldframe = frame
                    else:
                        speed_change = countMov(oldframe,frame)
                        oldframe = frame

                    #serializes objectss to bytes
                    frameObjectss = (json.dumps(frameObjectss)).encode()

                    frame_bytes = serializeImg(frame)
                    self.producer.produce(
                        topic= self.topic, 
                        value=frame_bytes, 
                        on_delivery=delivery_report,
                        timestamp=frame_time,
                        headers={"Host": str.encode(self.hostname),
                            "camera_name": str.encode(camera_name),
                            "frameObjects":frameObjectss,
                            "Nb_people": nb_people,
                            "Speed_change":speed_change,
                            "frame_no": frame_no 
                            }
                        )
                    self.producer.poll(0)
                    #time.sleep(0.01)
                    frame_no += 1
                    print(frameObjectss)

            if cv2.waitKey(1) & 0xFF == ord('q'):
                break
        cap.release()
    

    #maps each camera from cam_IPs list to a producer thread 
    def start(self, cams):
        # runs until the processes in all the threads are finished
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(self.publishFrame, cams)

        self.producer.flush() # push all the remaining messages in the queue
        print("Finished...")


if __name__ == "__main__":

    # list of camera accesses
    cameras = camera_links

    
    wht = 320

    classFile = 'coco.names'
    classNames = []
    with open(classFile,'rt') as f:
        classNames = f.read().rstrip('\n').split('\n')
    
    modelConfiguration = 'yolov3.cfg'
    modelWeights = 'yolov3.weights' #yolov3.weights file must be present !!

    #init the neural network with configuration & weights
    netModel = cv2.dnn.readNetFromDarknet(modelConfiguration,modelWeights)
    netModel.setPreferableBackend(cv2.dnn.DNN_BACKEND_OPENCV)
    netModel.setPreferableTarget(cv2.dnn.DNN_TARGET_CPU)



    #kafka_broker_url = os.environ.get("KAFKA_BROKER_URL")
    kafka_broker_url = 'broker:9092'

    admin_client = AdminClient({
    "bootstrap.servers":  kafka_broker_url
    })

    #get all brokers topics (a workaround to test if the broker is up)
    broker_topics = admin_client.list_topics().topics 
    while not broker_topics:
        broker_topics = admin_client.list_topics().topics

    host = os.environ.get('HOST_NAME')

    cams = len(cameras)
    
    producer_thread = ProducerThread(host, admin_client, kafka_broker_url, netModel)
    producer_thread.start(cams)

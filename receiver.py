import json
import socket
import tweepy
import argparse

from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener


#Setting up credentials , reading from txt file

with open('./twitter_api_keys.txt') as f:
    content = f.readlines()

content = [x.strip() for x in content] 

consumer_key = content[0][9:]
consumer_secret = content[1][17:]
access_token = content[3][14:]
access_secret = content[4][21:]

class TweetsListener(StreamListener):
    def __init__(self,csocket):
        self.client_socket = csocket
    def on_data(self,data):
        try:
            msg = json.loads(data) #Creates message
            self.client_socket.send(msg['text'].encode('utf-8'))
            
            return True
        except BaseException as e:
                print('Error on_data:  %s' % str(e))
        return True
    def on_error(self,status):
        print(status)
        return True
    
def sendData(c_socket,input_string):
    auth = OAuthHandler(consumer_key,consumer_secret)
    auth.set_access_token(access_token,access_secret)

    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=[input_string])

if __name__ == '__main__':
	
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic', type=str, metavar='', help = '''input hastag topic so stream twitter data''')
    parser.add_argument('--address', type=str, metavar='', help = '''ip address ''')
    parser.add_argument('--port', type=int, metavar='', help = '''any open ports''')

    args = parser.parse_args()

    twitter_topic = args.topic

    s = socket.socket()
    host = args.address #localhost ip
    port = args.port

    s.bind((host,port))
    print('Listening on port: %s' % str(port))

    s.listen(5,twitter_topic)
    c, addr = s.accept()

    print('Received request from: ' + str(addr))
    sendData(c)

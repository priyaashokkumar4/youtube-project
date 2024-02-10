import streamlit as st
import googleapiclient.discovery
import pymongo
import pandas as pd
import mysql.connector
#pip install pymongo mysql-connector-python streamlit


from pymongo.mongo_client import MongoClient
from pprint import pprint

client = MongoClient("mongodb+srv://priya:1234@cluster0.whpqvxd.mongodb.net/?retryWrites=true&w=majority")
database = client.youtubeproject
coll1 = database["channeldeatils"]


mydb = mysql.connector.connect(
  host="localhost",
 user="root",
  password="",
  database = "youtubeproject"  
)
print(mydb)


mycursor = mydb.cursor(buffered=True)



st.title("YOUTUBE DATA HARVESTIND AND WAREHOUSING")


api_key = 'AIzaSyDK0BYP8yLNhRL07o3gQ9tOYzYrZIT6Ruk'


api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
  
#channel detail;

#c_id = input()
def get_channel_info(abc):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=abc
    )
    response = request.execute()

    z = dict(chid=response['items'][0]['id'],
             t=response['items'][0]['snippet']['title'],
             p=response['items'][0]['snippet']['publishedAt'],
             d=response['items'][0]['snippet']['description'],
             sb=response['items'][0]['statistics']['subscriberCount'],
             vc=response['items'][0]['statistics']['videoCount'],
             views=response['items'][0]['statistics']['viewCount'],
             p_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads'])
    return z
c_id  =  st.text_input("enter the channel id")
if c_id and st.button('for channel'):
    A = get_channel_info(c_id)
    st.write(A)



#  playlist id
    
def get_video_id(channel_id):
   video_id = []
   response = youtube.channels().list(id=channel_id, part='contentDetails').execute()
   P_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

   next_page_token = None

   while True:
       response1 = youtube.playlistItems().list(
           part='snippet',
           playlistId=P_id,
           maxResults=50,
           pageToken=next_page_token).execute()

       for i in range(len(response1['items'])):
           video_id.append(response1['items'][i]['snippet']['resourceId']['videoId'])

       next_page_token = response1.get('nextPageToken')

       if next_page_token is None:
        break
   return dict.fromkeys(video_id)
if c_id:
    video_ids = get_video_id(c_id)   




#video deatils:

def get_video_info(video_ids):
    video_data = []

    for vid in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=vid
        )
        response = request.execute()

        for item in response["items"]:

            data = dict(
                channel_Name=item['snippet']['channelTitle'],
                channel_Id=item['snippet']['channelId'],  # Corrected channel_Id to 'channelId'
                video_Id=item['id'],
                Title=item['snippet']['title'],
                Thumbnail=item['snippet']['thumbnails']['default']['url'],  # Adjusted Thumbnail access
                Description=item['snippet']['description'],
                Published_Date=item['snippet']['publishedAt'],
                Duration=item['contentDetails']['duration'],
                Likes = item['statistics'].get('likeCount'),
                Views=item.get('statistics', {}).get('viewCount', 0),  # Updated Views access
                Comments=item.get('statistics', {}).get('commentCount', 0),  # Updated Comments access
                Definition=item['contentDetails']['definition'],
                Caption_Status=item['contentDetails']['caption']
            )
            
            video_data.append(data)
    return video_data
if c_id and st.button('for video'):
    B = get_video_info(video_ids)
    st.write(B)



#commmentdetalis:
    
def get_comment_info(video_ids):
    comment_data = []

    for video_id in video_ids:
        try:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id
            )
            response = request.execute()

            for item in response.get('items', []):
                snippet = item.get('snippet', {}).get('topLevelComment', {}).get('snippet', {})
                data = {
                    'comment_Id': snippet.get('id', ''),
                    'Video_Id': snippet.get('videoId', ''),
                    'Comment_Text': snippet.get('textDisplay', ''),
                    'Comment_Author': snippet.get('authorDisplayName', ''),
                    'Comment_Published': snippet.get('publishedAt', '')
                }
                comment_data.append(data)

        except:
            pass

    
    return comment_data
if c_id and st.button('for comment'):
    C = get_comment_info(video_ids)
    st.write(C)

#insert mongodb:

if st.button("MONGODB"):
    CH_IDS = []
    for channeldetails in coll1.find({},{"channelinfo.chid": 1, "_id": 0}):
       CH_IDS.append(channeldetails["channelinfo"]["chid"])
    if c_id not in  CH_IDS:
            def channel_details(c_id):
                ch_details = get_channel_info(c_id)
                vi_details = get_video_info(video_ids)
                com_details = get_comment_info(video_ids)
                coll1 = database["channeldeatils"]
                coll1.insert_one({"channelinfo": ch_details, "videodetails": vi_details, "commentdetails": com_details})
                #return {"channelinfo": ch_details, "videodetails": vi_details, "commentdetails": com_details}
            channel_details(c_id)
            
    else:
        print("already exists")




st.title("Select Channel from MongoDB")

# Retrieve all channel IDs from MongoDB
all_channel_ids = [channeldetails["channelinfo"]["chid"] for channeldetails in coll1.find({}, {"channelinfo.chid": 1, "_id": 0})]

# Create a Streamlit dropdown to select a channel ID
selected_channel_id = st.selectbox("Select a channel ID", all_channel_ids)

# Display details of the selected channel from MongoDB
if st.button("Show Channel Details"):
    selected_channel_details = coll1.find_one({"channelinfo.chid": selected_channel_id}, {"_id": 0})
    if selected_channel_details:
        st.write(selected_channel_details)
    else:
        st.write("Channel details not found.")

if st.button("SQL"):

    def get_info_(CD):

        d = coll1.find_one({"channelinfo.chid": CD}, {"_id": 0})
        CHD= '''INSERT INTO channeld(chid, t, p, d, sb, vc, views, p_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)'''
        val = tuple(d['channelinfo'].values())
        mycursor.execute(CHD, val)
        mydb.commit()
        
        VID = '''INSERT INTO video(channel_Name,channel_Id ,video_Id,Title,Thumbnail ,Description,
                                  Published_Date,Duration ,Likes , Views ,Comments,Definition,Caption_Status )
                                  VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        for i in d['videodetails']:
            val = tuple(i.values())
            mycursor.execute(VID, val)
            mydb.commit()
        
        COM = '''INSERT INTO  commentdetails(comment_Id , Video_Id ,Comment_Text ,Comment_Author ,Comment_Published )
                                  VALUES(%s,%s,%s,%s,%s)'''
        for i in d['commentdetails']:
            val = tuple(i.values())
            mycursor.execute(COM, val)
            mydb.commit()

    get_info_(c_id)



question = st.selectbox("select your question",("1.the names of all the videos and their corresponding channels" , 
                                       "2.channels have the most number of videos, and how many videos do they have",
                                       "3.the top 10 most viewed videos and their respective channels",
                                       "4.How many comments were made on each video, and what are thei corresponding video names",
                                       "5.Which videos have the highest number of likes, and what are their corresponding channel names",
                                       "6.the total number of likes and dislikes for each video, and what are their corresponding video names",
                                       "7.the total number of views for each channel, and what are their corresponding channel names",
                                       "8.the names of all the channels that have published videos in the year2022",
                                       "9.the average duration of all videos in each channel, and what are their corresponding channel names",
                                       "10.Which videos have the highest number of comments, and what are their corresponding channel names"))

if question == "1.the names of all the videos and their corresponding channels":
    mycursor.execute("SELECT title, channel_name FROM youtubeproject.video")
    out = mycursor.fetchall()
    df = pd.DataFrame(out, columns=['title', 'channel_name'])
    st. write(df)

elif question =="2.channels have the most number of videos, and how many videos do they have":
    mycursor.execute("Select t  as title , vc  as videocount FROM youtubeproject .channeld  order by vc DESC ")
    out=mycursor.fetchall()
    df = pd.DataFrame(out, columns=['title', 'videocount'])
    st.write(df)

elif question =="3.the top 10 most viewed videos and their respective channels":
    mycursor.execute("Select views  , t as title FROM youtubeproject .channeld order by views DESC limit 10")
    out=mycursor.fetchall()
    df = pd.DataFrame(out, columns=['views', 'title'])
    st.write(df)

elif question == "4.How many comments were made on each video, and what are thei corresponding video names":
    mycursor.execute("SELECT COUNT(cd.comment_id), video.title FROM commentdetails cd JOIN video ON cd.video_id = video.video_id GROUP BY video.title")
    out = mycursor.fetchall()
    df = pd.DataFrame(out, columns=['CommentCount', 'Title'])
    st.write(df)

elif question =="5.Which videos have the highest number of likes, and what are their corresponding channel names":
    mycursor.execute("Select title as videotitle ,channel_name as channelname ,likes as likescount  FROM youtubeproject .video order by likes desc")
    out=mycursor.fetchall()
    df = pd.DataFrame(out, columns=['videotitle' , 'channelname', 'likescount'])
    st.write(df)


elif question =="6.the total number of likes and dislikes for each video, and what are their corresponding video names":
    mycursor.execute("Select Likes , title FROM youtubeproject .video")
    out=mycursor.fetchall()
    df = pd.DataFrame(out, columns=['Likes', 'title'])
    st.write(df)

elif question =="7.the total number of views for each channel, and what are their corresponding channel names":
    mycursor.execute("Select views  , t as title FROM youtubeproject .channeld ")
    out=mycursor.fetchall()
    df = pd.DataFrame(out, columns=['views', 'title'])
    st.write(df)

elif question =="8.the names of all the channels that have published videos in the year2022":
    mycursor.execute("Select t as title , p as published  FROM youtubeproject .channeld where  p = 2022")
    out=mycursor.fetchall()
    df = pd.DataFrame(out, columns=['title', 'published '])
    st.write(df)

elif question == "9.the average duration of all videos in each channel, and what are their corresponding channel names":
    mycursor.execute("SELECT AVG(vd.duration), title FROM video vd JOIN channeld ch ON vd.channel_Id = ch.chid GROUP BY title")
    out = mycursor.fetchall()
    df = pd.DataFrame(out, columns=['AvgDuration', 'Title'])
    st.write(df)

elif question =="10.Which videos have the highest number of comments, and what are their corresponding channel names":
    mycursor.execute("SELECT total_count , channel_name FROM (select count(comment_Text) as \
                 total_count ,channel_name from commentdetails cd \
                 left join video vd on cd.video_id = vd.video_id left join channeld chd  on vd.channel_id = chd.chid \
                 group by channel_name)as temp  order by total_count desc")
    out = mycursor.fetchall()
    df = pd.DataFrame(out, columns=['total_count', 'channel_name'])
    st.write(df)

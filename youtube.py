import googleapiclient.discovery
from pymongo import MongoClient
import psycopg2
import pandas as pd
from psycopg2 import IntegrityError
import streamlit as st



#API key connection

def api_connect():
    api_service_name = "youtube"
    api_version = "v3"
    api_key="AIzaSyDPdlTR6oxePSjzsH231GjewhVGAETeQeM"
    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
    
    return youtube

youtube=api_connect()



#Channel Details
def get_channel_info(channel_id):

    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id)
    response = request.execute()

    for i in response['items']:
        data=dict(Channel_Name=i['snippet']['title'],
                Channel_Description=i['snippet']['description'],
                Channel_Id=i['id'],
                Subscribers=i['statistics']['subscriberCount'],
                Views=i['statistics']['viewCount'],
                Total_Videos=i['statistics']['videoCount'],
                Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads'])

    return data



#retrieving video ids

def get_video_ids(channel_id):

  video_ids=[]
  request = youtube.channels().list(part="contentDetails",id=channel_id)
  response = request.execute()
  Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

  next_page_token=None
  while True:
    response1=youtube.playlistItems().list(
                                          part='snippet',
                                          playlistId=Playlist_Id,
                                          maxResults=50,
                                          pageToken=next_page_token).execute()

    for i in range(len(response1['items'])):
      video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
    next_page_token=response1.get('nextPageToken')

    if next_page_token is None:
      break
  return video_ids


#retrieving video data

def get_video_info(video_ids):

    video_data = []

    for video_id in video_ids:
        request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id= video_id)
        response = request.execute()

        for item in response["items"]:
            data = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_Id = item['id'],
                        Title = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet']['description'],
                        Published_Date = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        Views = item['statistics']['viewCount'],
                        Likes = item['statistics'].get('likeCount'),
                        Dislikes= item['statistics'].get('dislikeCount'),
                        Comments = item['statistics'].get('commentCount'),
                        Favorite_Count = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition'],
                        Caption_Status = item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data


#retrieving comments
def get_comment_details(video_ids):
  Comment_Data=[]
  try:
    for video_id in video_ids:
      request=youtube.commentThreads().list(
                                      part='snippet',
                                      videoId=video_id,
                                      maxResults=50
      )
      response=request.execute()

      for item in response['items']:
        data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                  Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                  Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                  Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                  Comment_Published_Date=item['snippet']['topLevelComment']['snippet']['publishedAt'])
        
        Comment_Data.append(data)
        


  except:
    pass
  return Comment_Data



#retrieving playlist details

def get_playlist_details(channel_id):

  next_page_token=None
  Playlist_Data=[]

  while True:
    request=youtube.playlists().list(
                                part='snippet, contentDetails',
                                channelId=channel_id,
                                maxResults=50,
                                pageToken=next_page_token
    )
    response=request.execute()

    for item in response['items']:
      data=dict(Playlist_Id=item['id'],
                Title=item['snippet']['title'],
                Channel_Id=item['snippet']['channelId'],
                Channel_Name=item['snippet']['channelTitle'],
                Published_At=item['snippet']['publishedAt'],
                Video_Count=item['contentDetails']['itemCount']
                )

      Playlist_Data.append(data)  
    
    next_page_token=response.get('nextPageToken')
    if next_page_token is None:
      break
  return Playlist_Data


#MongoDB Connection

connection = MongoClient("mongodb+srv://kamayanimgnf2021:12345@cluster0.rgwducf.mongodb.net/")
db=connection["Youtube"]

def channel_details(channel_id):
    channel_data= get_channel_info(channel_id)
    playlist_data = get_playlist_details(channel_id)
    video_ids = get_video_ids(channel_id)
    video_data = get_video_info(video_ids)
    comment_data = get_comment_details(video_ids)

    col = db["Channel_Database"]
    col.insert_one({"channel_information":channel_data,"playlist_information":playlist_data,"video_information":video_data,
                     "comment_information":comment_data})
    
    return "Uploaded Successfully"



#creating tables for youtube channels

def channels_table():
    mydb=psycopg2.connect(host='localhost',
                        user='postgres',
                        password=12345,
                        database='youtube_database',
                        port=5432)
    cursor=mydb.cursor()

    try:
        # Check if the table exists
        cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'youtube_channel')")
        table_exists = cursor.fetchone()[0]

        if table_exists:
            print("Channel's Table already present")
        else:
            # Table doesn't exist, so create it
            create_query = '''create table youtube_channel(
                                Channel_Name varchar(100),
                                Channel_Description text,
                                Channel_Id varchar(80) primary key, 
                                Subscribers bigint, 
                                Views bigint,
                                Total_Videos int,
                                Playlist_Id varchar(50))'''
            cursor.execute(create_query)
            mydb.commit()  # Commit the transaction
            print("Table 'youtube_channel' created successfully")
    except Exception as e:
        mydb.rollback()  # Rollback the transaction in case of an error
        print("Error creating table: Already Exists", e)

    channel_list=[]
    db=connection["Youtube"]
    col= db["Channel_Database"]
    for data in col.find({},{"_id":0,"channel_information":1}):
        channel_list.append(data["channel_information"])
    df=pd.DataFrame(channel_list)

    #inserting data from df into tables
    for index, row in df.iterrows():
        insert_query = '''INSERT INTO youtube_channel(Channel_Name,
                                            Channel_Description,
                                            Channel_Id,
                                            Subscribers,
                                            Views,
                                            Total_Videos,
                                            Playlist_Id)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['Channel_Name'],
                row['Channel_Description'],
                row['Channel_Id'],
                row['Subscribers'],
                row['Views'],
                row['Total_Videos'],
                row['Playlist_Id'])

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
            print("Data inserted successfully for Channel ID:", row['Channel_Id'])
        except IntegrityError as e:
            mydb.rollback()  # Rollback the transaction
            print(f"Data for Channel ID {row['Channel_Id']} already present")
        except Exception as e:
            mydb.rollback()  # Rollback the transaction
            print(f"Data for Channel ID {row['Channel_Id']} already present")

    
        



def playlist_table():

    mydb = psycopg2.connect(host='localhost',
                            user='postgres',
                            password=12345,
                            database='youtube_database',
                            port=5432)
    cursor = mydb.cursor()

    create_query = None  # Initialize create_query with None

    # Check if the table exists
    cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'youtube_playlist')")
    table_exists = cursor.fetchone()[0]

    if table_exists:
        print("Playlist's Table already present")
    else:
        # Table doesn't exist, so create it
        create_query = '''create table youtube_playlist(
                        Playlist_Id varchar(100) primary key,
                        Title varchar(100),
                        Channel_Id varchar(100), 
                        Channel_Name varchar(100), 
                        Published_At timestamp,
                        Video_Count int)
                        
        '''
        cursor.execute(create_query)
        mydb.commit()

    playlist_list = []  
    db = connection["Youtube"]
    col = db["Channel_Database"]
    for play_data in col.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(play_data["playlist_information"])):
            playlist_list.append(play_data["playlist_information"][i])
    df1 = pd.DataFrame(playlist_list)

    # Inserting data from df1 into tables
    for index, row in df1.iterrows():
        insert_query = '''INSERT INTO youtube_playlist(Playlist_Id,
                                            Title,
                                            Channel_Id,
                                            Channel_Name,
                                            Published_At,
                                            Video_Count
                                            )
                        VALUES (%s,%s,%s,%s,%s,%s)'''
        values = (row['Playlist_Id'],
                  row['Title'],
                  row['Channel_Id'],
                  row['Channel_Name'],
                  row['Published_At'],
                  row['Video_Count'])
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
            print("Playlist Data inserted successfully for Channel ID:", row['Channel_Id'])
        except IntegrityError as e:
            mydb.rollback()  # Rollback the transaction
            print(f"Playlist Data for Channel ID {row['Channel_Id']} already present")
        except Exception as e:
            mydb.rollback()  # Rollback the transaction
            print(f"An unexpected error occurred while inserting playlist data for Channel ID {row['Channel_Id']}: {e}")


    


def video_table():
    mydb = psycopg2.connect(host='localhost',
                            user='postgres',
                            password=12345,
                            database='youtube_database',
                            port=5432)
    cursor = mydb.cursor()

    # Check if the table exists
    cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'youtube_video')")
    table_exists = cursor.fetchone()[0]

    if not table_exists:
        # Table doesn't exist, so create it
        create_query = '''create table youtube_video(
                        Channel_Name varchar(100),
                        Channel_Id varchar(100),
                        Video_Id varchar(100) primary key,
                        Title varchar(100),
                        Tags text,
                        Thumbnail varchar(200),
                        Description text,
                        Published_Date timestamp,
                        Duration interval,
                        Views bigint,
                        Likes bigint,
                        Dislikes bigint,
                        Comments int,
                        Favorite_Count int,
                        Definition varchar(10),
                        Caption_Status varchar(100))'''

        cursor.execute(create_query)
        mydb.commit()

    video_list = []
    db = connection["Youtube"]
    col = db["Channel_Database"]
    for video_data in col.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(video_data["video_information"])):
            video_list.append(video_data["video_information"][i])
    df2 = pd.DataFrame(video_list)

    # Inserting data from df1 into tables
    for index, row in df2.iterrows():
        insert_query = '''INSERT INTO youtube_video(
                                            Channel_Name,
                                            Channel_Id,
                                            Video_Id,
                                            Title,
                                            Tags,
                                            Thumbnail,
                                            Description,
                                            Published_Date,
                                            Duration,
                                            Views,
                                            Likes,
                                            Dislikes,
                                            Comments,
                                            Favorite_Count,
                                            Definition,
                                            Caption_Status)
                                            
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (Video_Id) DO UPDATE SET
                            Channel_Name = excluded.Channel_Name,
                            Channel_Id = excluded.Channel_Id,
                            Title = excluded.Title,
                            Tags = excluded.Tags,
                            Thumbnail = excluded.Thumbnail,
                            Description = excluded.Description,
                            Published_Date = excluded.Published_Date,
                            Duration = excluded.Duration,
                            Views = excluded.Views,
                            Likes = excluded.Likes,
                            Dislikes = excluded.Dislikes,
                            Comments = excluded.Comments,
                            Favorite_Count = excluded.Favorite_Count,
                            Definition = excluded.Definition,
                            Caption_Status = excluded.Caption_Status;'''

        values = (row['Channel_Name'],
                  row['Channel_Id'],
                  row['Video_Id'],
                  row['Title'],
                  row['Tags'],
                  row['Thumbnail'],
                  row['Description'],
                  row['Published_Date'],
                  row['Duration'],
                  row['Views'],
                  row['Likes'],
                  row['Dislikes'],
                  row['Comments'],
                  row['Favorite_Count'],
                  row['Definition'],
                  row['Caption_Status'])

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except psycopg2.IntegrityError:
            print("Video Data already present and updated")
        

    
def comment_table():
    mydb = psycopg2.connect(host='localhost',
                             user='postgres',
                             password=12345,
                             database='youtube_database',
                             port=5432)
    cursor = mydb.cursor()

    # Check if the table exists
    cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'youtube_comment')")
    table_exists = cursor.fetchone()[0]

    if not table_exists:
        # Table doesn't exist, so create it
        create_query = '''create table youtube_comment(
                                        Comment_Id varchar(100) primary key,
                                        Video_Id varchar(50),
                                        Comment_Text text,
                                        Comment_Author varchar(200),
                                        Comment_Published_Date timestamp )'''

        cursor.execute(create_query)
        mydb.commit()

    comment_list = []
    db = connection["Youtube"]
    col = db["Channel_Database"]
    for comment_data in col.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(comment_data["comment_information"])):
            comment_list.append(comment_data["comment_information"][i])
    df3 = pd.DataFrame(comment_list)

    # Inserting data from df3 into tables
    for index, row in df3.iterrows():
        insert_query = '''INSERT INTO youtube_comment(
                                                Comment_Id,
                                                Video_Id,
                                                Comment_Text,
                                                Comment_Author,
                                                Comment_Published_Date)
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (Comment_Id) DO NOTHING'''  # Ignore insertion if Comment_Id already exists

        values = (row['Comment_Id'],
                  row['Video_Id'],
                  row['Comment_Text'],
                  row['Comment_Author'],
                  row['Comment_Published_Date'])

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except psycopg2.IntegrityError:
            print("Comment Data already present")
        

        

def tables():
    channels_table()
    playlist_table()
    video_table()
    comment_table()

    return "Migrated Successfully !!"

Tables=tables()



def show_channel_tables():
    channel_list=[]
    db=connection["Youtube"]
    col= db["Channel_Database"]
    for data in col.find({},{"_id":0,"channel_information":1}):
        channel_list.append(data["channel_information"])
    df=st.dataframe(channel_list)

    return df


def show_playlist_table():
    playlist_list=[]
    db=connection["Youtube"]
    col= db["Channel_Database"]
    for play_data in col.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(play_data["playlist_information"])):
            playlist_list.append(play_data["playlist_information"][i])

    df1=st.dataframe(playlist_list)

    return df1


def show_video_table():
    video_list = []
    db = connection["Youtube"]
    col = db["Channel_Database"]
    for video_data in col.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(video_data["video_information"])):
            video_list.append(video_data["video_information"][i])

    df2 = st.dataframe(video_list)  

    return df2

def show_comment_table():
    comment_list = []
    db = connection["Youtube"]
    col = db["Channel_Database"]
    for comment_data in col.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(comment_data["comment_information"])):
            comment_list.append(comment_data["comment_information"][i])

    df3 = st.dataframe(comment_list)

    return df3




#streamlit UI loading

st.title(":red[YouTube:DATA HARVESTING AND WAREHOUSING]")



with st.sidebar:
    st.image("logo.png",width=250)
    st.header("About:")
    st.caption("A user-friendly tool to harvest, store, and analyze data from YouTube channels using Python, MongoDB, and Streamlit.")
    
    


channel_id = st.text_input("Enter a YouTube channel ID")
st.write("")

# Center-align the button
col1, col2, col3 = st.columns([1, 2, 1])

with col1:
    if st.button("Store Data in MongoDB"):
        channel_list = []
        db = connection["Youtube"]
        col = db["Channel_Database"]
        
        
        for data in col.find({}, {"_id": 0, "channel_information.Channel_Id": 1}):
            channel_list.append(data["channel_information"]["Channel_Id"])

        if channel_id in channel_list:
            st.success("Channel Details of the YouTube Channel already exist")
        else:
            
            insert = channel_details(channel_id)
            st.success(insert)

with col3:
    if st.button("Migrate Data from MongoDB to SQL"):
        Table = tables()
        st.success(Table)

with st.sidebar:
    show_table=st.selectbox("SELECT THE TABLE FOR VIEW",("CHANNELS", "PLAYLIST", "VIDEOS", "COMMENTS"))
if show_table=="CHANNELS":
    show_channel_tables()

elif show_table=="PLAYLIST":
    show_playlist_table()

elif show_table=="VIDEOS":
    show_video_table()

elif show_table=="COMMENTS":
    show_comment_table()


 
    
#   SQL CONNECTION
mydb = psycopg2.connect(host='localhost',
                            user='postgres',
                            password=12345,
                            database='youtube_database',
                            port=5432)
cursor = mydb.cursor()

question=st.selectbox("TOP 10 QnAs", (
    "1. What are the titles of all the videos and who posted them?",
    "2. Which channels have posted the most videos and how many?",
    "3. What are the top 10 videos with the most views, and who uploaded them?",
    "4. How many comments does each video have, and what are their titles?",
    "5. Which videos have the most likes, and who posted them?",
    "6. How many likes does each video have, and what are their titles?",
    "7. How many views does each channel have, and what are their names?",
    "8. Which channels posted videos in 2022?",
    "9. What's the average length of videos for each channel?",
    "10.Which videos have the highest number of comments, and what are their corresponding channel names"))


mydb = psycopg2.connect(host='localhost',
                            user='postgres',
                            password=12345,
                            database='youtube_database',
                            port=5432)
cursor = mydb.cursor()


if question=="1. What are the titles of all the videos and who posted them?":

    q1=''' select title as videos, channel_name as channel from youtube_video'''
    cursor.execute(q1)
    mydb.commit()

    a1=cursor.fetchall()
    df=pd.DataFrame(a1, columns=["Video Title", "Channel Name"])
    st.write(df)

elif question=="2. Which channels have posted the most videos and how many?":

    q2=''' select channel_name as channel, total_videos as all_videos from youtube_channel order by total_videos desc'''
    cursor.execute(q2)
    mydb.commit()

    a2=cursor.fetchall()
    df2=pd.DataFrame(a2, columns=["Channel Name","Total Videos"])
    st.write(df2)

elif question=="3. What are the top 10 videos with the most views, and who uploaded them?":

    q3=''' select views as views, channel_name as channel, title as videos from youtube_video where views is not null
            order by views desc limit 10'''
    cursor.execute(q3)
    mydb.commit()

    a3=cursor.fetchall()
    df3=pd.DataFrame(a3, columns=["Views", "Channel Name","Video Title"])
    st.write(df3)

elif question=="4. How many comments does each video have, and what are their titles?":

    q4=''' select comments as totalcomments, title as videos from youtube_video where comments is not null
                '''
    cursor.execute(q4)
    mydb.commit()

    a4=cursor.fetchall()
    df4=pd.DataFrame(a4, columns=["Comments","Video Title"])
    st.write(df4)

elif question=="5. Which videos have the most likes, and who posted them?":

    q5=''' select title as videos, channel_name as channel, likes as likescount from youtube_video 
            where likes is not null order by likes desc'''
    cursor.execute(q5)
    mydb.commit()

    a5=cursor.fetchall()
    df5=pd.DataFrame(a5, columns=["Video Title", "Channel", "Likes"])
    st.write(df5)

elif question=="6. How many likes does each video have, and what are their titles?":

    q6=''' select title as videos, channel_name as channel, likes as likecount
            from youtube_video where likes is not null'''
    cursor.execute(q6)
    mydb.commit()

    a6=cursor.fetchall()
    df6=pd.DataFrame(a6, columns=["Video Title", "Channel", "Likes"])
    st.write(df6)
    
elif question=="7. How many views does each channel have, and what are their names?":

    q7=''' select channel_name as channel, views as totalviews from youtube_channel'''
    cursor.execute(q7)
    mydb.commit()

    a7=cursor.fetchall()
    df7=pd.DataFrame(a7, columns=[ "Channel", "Total Views"])
    st.write(df7)
    

elif question=="8. Which channels posted videos in 2022?":

    q8=''' select title as video_title, published_date as videodate, channel_name as channel from youtube_video
            where extract(year from published_date)=2022'''
    cursor.execute(q8)
    mydb.commit()

    a8=cursor.fetchall()
    df8=pd.DataFrame(a8, columns=[ "Video","Published in 2022", "Channel"])
    st.write(df8)

elif question=="9. What's the average length of videos for each channel?":

    q9=''' select channel_name as channel, AVG(duration) as averageduration from youtube_video
            group by channel_name'''
    cursor.execute(q9)
    mydb.commit()

    a9=cursor.fetchall()
    df9=pd.DataFrame(a9, columns=[ "Channel", "Average Duration of Video"])

    T9 = []
    for index, row in df9.iterrows():
        channel_title = row["Channel"]
        average_duration = row["Average Duration of Video"]
        average_duration_str = str(average_duration)
        T9.append(dict(channeltitle=channel_title, avgduration=average_duration_str))

    df1 = pd.DataFrame(T9)
    st.write(df9)

elif question=="10.Which videos have the highest number of comments, and what are their corresponding channel names":

    q10 = '''select title as videotitle, channel_name as channel, comments AS comments
            from youtube_video order by comments desc'''
            
    cursor.execute(q10)
    mydb.commit()

    a10 = cursor.fetchall()
    df10 = pd.DataFrame(a10, columns=["Video", "Channel", "Comment"])
    st.write(df10)





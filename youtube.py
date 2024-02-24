import googleapiclient.discovery
import pymongo
from pymongo import MongoClient
import psycopg2
import pandas as pd
import pyarrow
from psycopg2 import IntegrityError

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

channel_details=get_channel_info("UC6vd-JCD7Z_FuF8N5LTydPA")

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

Video_ids=get_video_ids('UC6vd-JCD7Z_FuF8N5LTydPA')

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

comments=get_comment_details(Video_ids)

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

playlistdetails=get_playlist_details('UC6vd-JCD7Z_FuF8N5LTydPA')

#MongoDB Connection

connection = MongoClient("mongodb+srv://kamayanimgnf2021:12345@cluster0.rgwducf.mongodb.net/")
db=connection["Youtube"]

def channel_details(channel_id):
    channel_data= get_channel_info(channel_id)
    playlist_data = get_playlist_details(channel_id)
    video_ids_data = get_video_ids(channel_id)
    video_data = get_video_info(Video_ids)
    comment_data = get_comment_details(Video_ids)

    col = db["Channel_Database"]
    col.insert_one({"channel_information":channel_data,"playlist_information":playlist_data,"video_information":video_data,
                     "comment_information":comment_data})
    
    return "Uploaded Successfully"

insert=channel_details('UC6vd-JCD7Z_FuF8N5LTydPA')

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

# Call the channels_table function
channels_table()

#creating playlist table

def playlist_table():

    mydb=psycopg2.connect(host='localhost',
                    user='postgres',
                    password=12345,
                    database='youtube_database',
                    port=5432)
    cursor=mydb.cursor()

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

    if create_query is not None:
        playlist_list=[]
        db=connection["Youtube"]
        col= db["Channel_Database"]
        for play_data in col.find({},{"_id":0,"playlist_information":1}):
            for i in range(len(play_data["playlist_information"])):
                playlist_list.append(play_data["playlist_information"][i])

        df1=pd.DataFrame(playlist_list)

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

playlist_table()

#creating video table

def video_table():

    mydb = psycopg2.connect(host='localhost',
                            user='postgres',
                            password=12345,
                            database='youtube_database',
                            port=5432)
    cursor = mydb.cursor()

    create_query = None  # Initialize create_query with None

    # Check if the table exists
    cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'youtube_video')")
    table_exists = cursor.fetchone()[0]

    if table_exists:
        print("Videos's Table already present")
    else:
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
                        Comments int,
                        Favorite_Count int,
                        Definition varchar(10),
                        Caption_Status varchar(100))'''
        
        cursor.execute(create_query)
        mydb.commit()

     
    if create_query is not None:
        
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
                                                Comments,
                                                Favorite_Count,
                                                Definition,
                                                Caption_Status)
                                                
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
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
                        row['Comments'],
                        row['Favorite_Count'],
                        row['Definition'],
                        row['Caption_Status'])
                                    
            try:
                cursor.execute(insert_query, values)
                mydb.commit()
                print("Video Data inserted successfully")
            except:
                print(f"Video Data already present")
            
video_table()

#creating comment table

def comment_table():

    mydb = psycopg2.connect(host='localhost',
                                user='postgres',
                                password=12345,
                                database='youtube_database',
                                port=5432)
    cursor = mydb.cursor()

    create_query = None  # Initialize create_query with None

    # Check if the table exists
    cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'youtube_comment')")
    table_exists = cursor.fetchone()[0]

    if table_exists:
        print("Comment's Table already present")
    else:
        # Table doesn't exist, so create it
        create_query = '''create table youtube_comment(
                                        Comment_Id varchar(100) primary key,
                                        Video_Id varchar(50),
                                        Comment_Text text,
                                        Comment_Author varchar(200),
                                        Comment_Published_Date timestamp )'''
        
        cursor.execute(create_query)
        mydb.commit()

        if create_query is not None:
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
                                            
        VALUES (%s,%s,%s,%s,%s)'''
        
        values =    (row['Comment_Id'],
                    row['Video_Id'],
                    row['Comment_Text'],
                    row['Comment_Author'],
                    row['Comment_Published_Date'])
                
                                
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
            print("Comment Data inserted successfully")
        except IntegrityError as e:
            mydb.rollback()  # Rollback the transaction
            print(f"Comment Data already present")
        except Exception as e:
            mydb.rollback()  # Rollback the transaction
            print(f"An unexpected error occurred while inserting data: {e}")

   
comment_table()   
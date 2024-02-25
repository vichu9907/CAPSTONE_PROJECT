from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st
#Api key import
def Api_import():
    Api_Id="AIzaSyCPROhfynemZMs4Na1Hn7qHrsg2yYxkpYY"
    Api_service_provider = "youtube"
    Api_version = "v3"
    data = build(Api_service_provider,Api_version,developerKey=Api_Id)
    return data

data1=Api_import()

def get_channel_details(channel_id):
    request=data1.channels().list(
                    part="snippet,statistics,contentDetails",
                    id=channel_id
        )
    response=request.execute()

    for i in response['items']:
            data=dict(Channel_Name=i["snippet"]["title"],
                    Channel_ID=i["id"],
                    Subscribers=i['statistics']['subscriberCount'],
                    Views=i['statistics']['viewCount'],
                    Total_Videos=i['statistics']['videoCount'],
                    Channel_Description=i["snippet"]["description"],
                    Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
            return data
    
#you get multiple channel's video ID so you may wirte function
def get_video_ids(channel_id):
    video_ids=[]

    response=data1.channels().list(id=channel_id,
                                part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None
    # If you get total videoID you may use while loop (it's help repeated the below code)
    while True:
        response1=data1.playlistItems().list(
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

#Collect Video Information with using nested for loop
def video_details(video_ids):
    Video_details=[]
    for video_id in video_ids:
        request=data1.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response=request.execute()
        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    channel_id=item['snippet']['channelId'],
                    Video_id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet']['description'],
                    Published_date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Viewcount=item['statistics']['viewCount'],
                    Comments = item['statistics'].get('commentCount'),
                    Likes=item['statistics']['likeCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_status=item['contentDetails']['caption']
                    )
            Video_details.append(data)
    return Video_details

#Comment
def CommentDetails(video_ids):
    Comment_details=[]
    try:

        for Video_id in video_ids:
            request=data1.commentThreads().list(
                part="snippet",
                videoId=Video_id,
                maxResults=50
                )
            response=request.execute()

            for item in response['items']:
                data=dict(comment_id=item['snippet']['topLevelComment']['id'],
                        Video_id=item['snippet']['videoId'],
                        Comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_published_date=item['snippet']['topLevelComment']['snippet']['publishedAt']
                        )

                Comment_details.append(data)
                
    except:
        pass   

    return Comment_details

#PlayList
def PlayList(channel_id):

    All_list=[]
    next_page_token = None
    next_page=True

    while next_page:

        request=data1.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
            )
        response=request.execute()

        for item in response['items']:
            data=dict(Playlist_Id=item['id'],
                    Playlist_name=item['snippet']['title'],
                    Channel_Id=item['snippet']['channelId'],
                    VideoCount=item['contentDetails']['itemCount'])
            All_list.append(data)
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            next_page=False
            break
    return All_list

#Data Insert on MongoDB
client = pymongo.MongoClient("mongodb+srv://CapstoneProjectData:guvipj@capstone.vqdheu9.mongodb.net/?retryWrites=true&w=majority&appName=CapStone")
db=client["Capstone_Data"]

def Channel_info(channel_id):
    ch_info=get_channel_details(channel_id)
    pl_info=PlayList(channel_id)
    vi_ids=get_video_ids(channel_id)
    vi_info=video_details(vi_ids)
    com_info=CommentDetails(vi_ids)


    DataColl=db["channel_info"]
    DataColl.insert_one({"channel_details":ch_info,"playlist_details":pl_info,
                         "video_details":vi_info,"comment_details":com_info})
    
    return "insert successfully"

#Create Table in SQL

def Channels_table():
        
        Youtubedb= psycopg2.connect(host="localhost",
                                user="postgres",
                                password="youtubeproject",
                                database="channel_DB",
                                port="5432"
                                )
        cursor=Youtubedb.cursor()

        query_drop='''drop table if exists channels'''
        cursor.execute(query_drop)
        Youtubedb.commit()

        try:
                create_table='''create table if not exists channels(Channel_Name varchar(100),
                                Channel_ID varchar(80) primary key,
                                Subscribers bigint,
                                Views bigint,
                                Total_Videos int,
                                Channel_Description text,
                                Playlist_Id varchar(50))'''
                cursor.execute(create_table)
                Youtubedb.commit()
        except:
                print("Channel Table already created")

        #Data get from MongoDB 
        #{} means call all data from MongoDB

        channel_list=[]
        db=client["Capstone_Data"]
        DataColl=db["channel_info"]
        for data in DataColl.find({},{"_id":0,"channel_details":1}):
                channel_list.append(data["channel_details"])
        dataframe=pd.DataFrame(channel_list)

        for index,row in dataframe.iterrows():
                query_insert= '''insert into channels(Channel_Name,
                                                Channel_ID,
                                                Subscribers,
                                                Views,
                                                Total_Videos,
                                                Channel_Description,
                                                Playlist_Id)
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s)'''
                values=(row["Channel_Name"],
                        row["Channel_ID"],
                        row["Subscribers"],
                        row["Views"],
                        row["Total_Videos"],
                        row["Channel_Description"],
                        row["Playlist_Id"])
        
        cursor.execute(query_insert,values)
        Youtubedb.commit()

#Create Table for Playlist in SQL

#Create Table in SQL

def Playlist_table():


    Youtubedb= psycopg2.connect(host="localhost",
                                user="postgres",
                                password="youtubeproject",
                                database="channel_DB",
                                port="5432"
                                )
    cursor=Youtubedb.cursor()

    query_drop='''drop table if exists playlists'''
    cursor.execute(query_drop)
    Youtubedb.commit()


    create_table='''create table if not exists playlists(Playlist_Id varchar(80) primary key,
                            Playlist_name varchar(100),
                            Channel_Id varchar(80),
                            VideoCount int
                            )'''
    cursor.execute(create_table)
    Youtubedb.commit()

    playlist=[]
    db=client["Capstone_Data"]
    DataColl=db["channel_info"]
    for pl_data in DataColl.find({},{"_id":0,"playlist_details":1}):
        for i in range(len(pl_data["playlist_details"])):
            playlist.append(pl_data["playlist_details"][i])
    dataframe_pl=pd.DataFrame(playlist)


    for index,row in dataframe_pl.iterrows():
        query_insert= '''insert into playlists(Playlist_Id,
                                            Playlist_name,
                                                Channel_Id,
                                                VideoCount)
                                                
                                                values(%s,%s,%s,%s)'''
        values=(row['Playlist_Id'],
                row["Playlist_name"],
                row["Channel_Id"],
                row["VideoCount"])
        
        cursor.execute(query_insert,values)
        Youtubedb.commit()

# Create Table for Video Details in SQL 


def Video_table():

    Youtubedb= psycopg2.connect(host="localhost",
                                user="postgres",
                                password="youtubeproject",
                                database="channel_DB",
                                port="5432"
                                )
    cursor=Youtubedb.cursor()

    query_drop='''drop table if exists videolists'''
    cursor.execute(query_drop)
    Youtubedb.commit()



    create_table='''create table if not exists videolists(Channel_Name varchar(200),
                            channel_id varchar(125) ,
                            Video_id varchar(100) primary key ,
                            Title varchar(150),
                            Tags text,
                            Viewcount int,
                            Thumbnail varchar(200),
                            Description text,
                            Published_date timestamp,
                            Duration interval,
                            Comments int,
                            Likes bigint,
                            Definition varchar(50),
                            Caption_status varchar(80)

                            )'''
    cursor.execute(create_table)
    Youtubedb.commit()


    videolist=[]
    db=client["Capstone_Data"]
    DataColl=db["channel_info"]
    for Video_data in DataColl.find({},{"_id":0,"video_details":1}):
        for i in range(len(Video_data["video_details"])):
            videolist.append(Video_data["video_details"][i])
    dataframe_video=pd.DataFrame(videolist)



    for index,row in dataframe_video.iterrows():
        query_insert= '''insert into videolists(Channel_Name,
                                                channel_id,
                                                Video_id,
                                                Title,
                                                Tags,
                                                Viewcount,
                                                Thumbnail,
                                                Description,
                                                Published_date,
                                                Duration,
                                                Comments,
                                                Likes,
                                                Definition,
                                                Caption_status)
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values=(
                row["Channel_Name"],
                row["channel_id"],
                row["Video_id"],
                row["Title"],
                row["Tags"],
                row["Viewcount"],
                row["Thumbnail"],
                row["Description"],
                row["Published_date"],
                row["Duration"],
                row["Comments"],
                row["Likes"],
                row["Definition"],
                row["Caption_status"])
        
        cursor.execute(query_insert,values)
        Youtubedb.commit()

# Create Table for Comments details in SQL

def Comment_table():


    Youtubedb= psycopg2.connect(host="localhost",
                                user="postgres",
                                password="youtubeproject",
                                database="channel_DB",
                                port="5432"
                                )
    cursor=Youtubedb.cursor()

    query_drop='''drop table if exists commentlists'''
    cursor.execute(query_drop)
    Youtubedb.commit()

    create_table='''create table if not exists commentlists(comment_id varchar(80) primary key,
                            Video_id varchar(100),
                            Comment_text text,
                            Comment_author varchar(200),
                            Comment_published_date timestamp
                            )'''
    cursor.execute(create_table)
    Youtubedb.commit()


    commentlist=[]
    db=client["Capstone_Data"]
    DataColl=db["channel_info"]
    for Comment_data in DataColl.find({},{"_id":0,"comment_details":1}):
        for i in range(len(Comment_data["comment_details"])):
            commentlist.append(Comment_data["comment_details"][i])
    dataframe_comment=pd.DataFrame(commentlist)



    for index,row in dataframe_comment.iterrows():
        query_insert= '''insert into commentlists(comment_id,
                                                Video_id,
                                                Comment_text,
                                                Comment_author,
                                                Comment_published_date)
                                                
                                                values(%s,%s,%s,%s,%s)'''
        values=(
                row["comment_id"],
                row["Video_id"],
                row["Comment_text"],
                row["Comment_author"],
                row["Comment_published_date"])
        
        cursor.execute(query_insert,values)
        Youtubedb.commit()

def sqltables():
    Channels_table()
    Playlist_table()
    Video_table()
    Comment_table()
    return "Tables are created"

def st_channels_table():

        channel_list=[]
        db=client["Capstone_Data"]
        DataColl=db["channel_info"]
        for data in DataColl.find({},{"_id":0,"channel_details":1}):
                channel_list.append(data["channel_details"])
        channels_table=st.dataframe(channel_list)
        return channels_table

def st_playlist_table(): 
    playlist=[]
    db=client["Capstone_Data"]
    DataColl=db["channel_info"]
    for pl_data in DataColl.find({},{"_id":0,"playlist_details":1}):
        for i in range(len(pl_data["playlist_details"])):
                playlist.append(pl_data["playlist_details"][i])
    playlist_table=st.dataframe(playlist)
    return playlist_table

def st_videolist_table(): 
    videolist=[]
    db=client["Capstone_Data"]
    DataColl=db["channel_info"]
    for Video_data in DataColl.find({},{"_id":0,"video_details":1}):
            for i in range(len(Video_data["video_details"])):
                videolist.append(Video_data["video_details"][i])
    Video_table=st.dataframe(videolist)
    return Video_table

def st_comment_table():
    commentlist=[]
    db=client["Capstone_Data"]
    DataColl=db["channel_info"]
    for Comment_data in DataColl.find({},{"_id":0,"comment_details":1}):
        for i in range(len(Comment_data["comment_details"])):
            commentlist.append(Comment_data["comment_details"][i])
    comment_table=st.dataframe(commentlist)
    return comment_table

with st.sidebar:
    st.title(":red[CAPSTONE PROJECT: YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management Using MongoDB and SQL")

channels_id=st.text_input("Enter the Channle ID")


if st.button("stored data"):
    channels_ids=[]
    db=client["Capstone_Data"]
    DataColl=db["channel_info"]
    for channels_data in DataColl.find({},{"_id":0,"channel_details":1}):
        channels_ids.append(channels_data["channel_details"]["Channel_ID"])
    if channels_id in channels_ids:
        st.success("Channel Details are already stored")
    else:
        insert=Channel_info(channels_id)
        st.success(insert)


if st.button("Transfer to SQL"):
    Tables=sqltables()
    st.success(Tables)

St_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOLISTS","COMMENTLISTS"))

if St_table=="CHANNELS":
    st_channels_table()

elif St_table=="PLAYLISTS":
    st_playlist_table()

elif St_table=="VIDEOLISTS":
    st_videolist_table()

elif St_table=="COMMENTLISTS":
    st_comment_table()


Youtubedb= psycopg2.connect(host="localhost",
                            user="postgres",
                            password="youtubeproject",
                            database="channel_DB",
                            port="5432"
                            )
cursor=Youtubedb.cursor()

Query=st.selectbox("Select your query",("1. What are the names of all the videos and their corresponding channels?",
                                        "2. Which channels have the most number of videos, and how many videos do they have?",
                                        "3. What are the top 10 most viewed videos and their respective channels?",
                                        "4. How many comments were made on each video, and what are their corresponding video names?",
                                        "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                        "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                        "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                        "8. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                        "9. What are the names of all the channels that have published videos in the year 2022?",
                                        "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))

Youtubedb= psycopg2.connect(host="localhost",
                            user="postgres",
                            password="youtubeproject",
                            database="channel_DB",
                            port="5432"
                            )
cursor=Youtubedb.cursor()
if Query == '1. What are the names of all the videos and their corresponding channels?':
    question1 = "select Title,Channel_Name from videolists;"
    cursor.execute(question1)
    Youtubedb.commit()
    d1=cursor.fetchall()
    st.write(pd.DataFrame(d1, columns=["Title","Channel_Name"]))

elif Query == '2. Which channels have the most number of videos, and how many videos do they have?':
    question2= "select  Channel_Name, Total_Videos from channels order by Total_Videos desc;"
    cursor.execute(question2)
    Youtubedb.commit()
    d2=cursor.fetchall()
    st.write(pd.DataFrame(d2,columns=["Channel_Name","Total_Videos"]))

elif Query == '3. What are the top 10 most viewed videos and their respective channels?':
    question3 = '''select Viewcount,Channel_Name,Title from videolists 
                        where Viewcount is not null order by Viewcount desc limit 10;'''
    cursor.execute(question3)
    Youtubedb.commit()
    d3 = cursor.fetchall()
    st.write(pd.DataFrame(d3, columns = ["Viewcount","Channel_Name","Title"]))

elif Query =='4.How many comments were made on each video, and what are their corresponding video names?':
    question4= '''select Title, Comments from videolists where Comments is not null order by Comments desc;'''
    cursor.execute(question4)
    Youtubedb.commit()
    d4=cursor.fetchall()
    df=pd.DataFrame(d4,columns=["Comments","Title"])
    st.write(df)

elif Query =='5. Which videos have the highest number of likes, and what are their corresponding channel names?':
    Query5= '''select Channel_Name,Title,Likes from videolists where Likes is not null order by Likes desc;'''
    cursor.execute(Query5)
    Youtubedb.commit()
    d5=cursor.fetchall()
    st.write(pd.DataFrame(d5,columns=["Channel_Name","Title","Likes"]))

elif Query =='6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    Query6= '''select Title,Likes,Comments from videolists where Likes is not null order by Likes desc;'''
    cursor.execute(Query6)
    Youtubedb.commit()
    d6=cursor.fetchall()
    st.write(pd.DataFrame(d6,columns=["Title","Likes","Comments"]))

elif Query =='7. What is the total number of views for each channel, and what are their corresponding channel names?':
    question7= '''select Channel_Name,Views as Views_Count from channels;'''
    cursor.execute(question7)
    Youtubedb.commit()
    d7=cursor.fetchall()
    st.write(pd.DataFrame(d7,columns=["Channel_Name","Views_Count"]))

elif Query =='8. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    question8= '''SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM Videolists GROUP BY Channel_Name;'''
    cursor.execute(question8)
    Youtubedb.commit()
    d8=cursor.fetchall()
    df1=pd.DataFrame(d8,columns=["Channel_Name","AVG_Duration"])
    df2=[]
    for index, row in df1.iterrows():
        channel_title = row['Channel_Name']
        average_duration = row['AVG_Duration']
        average_duration_str = str(average_duration)
        df2.append({"Channel_Name": channel_title ,  "AVG_Duration": average_duration_str})
    st.write(pd.DataFrame(df2))

elif Query =='9. What are the names of all the channels that have published videos in the year 2022?':
    question9= '''select Title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from Videolists 
                where extract(year from Published_Date) = 2022;'''
    cursor.execute(question9)
    Youtubedb.commit()
    d9=cursor.fetchall()
    maxResult=50
    st.write(pd.DataFrame(d9,columns=["Video_Title","VideoRelease","ChannelName"]))

elif Query =='10. Which videos have the highest number of comments, and what are their corresponding channel names?':
    question10= '''select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from Videolists 
                       where Comments is not null order by Comments desc;'''
    cursor.execute(question10)
    Youtubedb.commit()
    d10=cursor.fetchall()
    st.write(pd.DataFrame(d10,columns=["Video_Title","ChannelName","Comments"]))
  
  
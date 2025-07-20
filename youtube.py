from googleapiclient.discovery import build
import pymysql
import pandas as pd
import streamlit as st
import time
import isodate
from streamlit_option_menu import option_menu

#API KEY CONNECTION
def Api_connect():
    Api_id="AIzaSyB3dpv2bz0_wFTlElU3EjjwcmLu4MzXkkw"

    api_service_name="youtube"
    api_version="v3"

    youtube=build( api_service_name,api_version,developerKey=Api_id)

    return youtube

youtube=Api_connect()

#get channels information
def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,contentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()

    for i in response['items']:
        data=dict(channel_Name=i["snippet"]["title"],
                channel_Id=i["id"],
                subscribers=i["statistics"]["subscriberCount"],
                views=i["statistics"]["viewCount"],
                Total_videos=i["statistics"]["videoCount"],
                channel_Description=i["snippet"]["description"],
                playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data

 #get video ids
def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    
    return video_ids

#get video information
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response=request.execute()
        

        for item in response["items"]:
            data=dict(channel_Name=item['snippet']['channelTitle'],
                    channel_Id=item['snippet']['channelId'],
                    video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['description'],
                    Description=item.get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favorite_Count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    caption_Status=item['contentDetails']['caption']
                    )
            video_data.append(data)
    return video_data

#get comment information
def get_comment_info(video_ids):
    comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=50,
                )
            response=request.execute()

            for item in response['items']:
                data=dict(comment_Id=item['snippet']['topLevelComment']['id'],
                        video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                comment_data.append(data)
    except:
        pass
    return comment_data

#get_playlist_details
def get_playlist_details(channel_id):
        next_page_token=None
        All_data=[]
        while True:
                request=youtube.playlists().list(
                        part='snippet,contentDetails',
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token
                )
                response=request.execute()

                for item in response['items']:
                        data=dict(playlist_Id=item['id'],
                                Title=item['snippet']['title'],
                                channel_Id=item['snippet']['channelId'],
                                channel_Name=item['snippet']['channelTitle'],
                                publishedAt=item['snippet']['publishedAt'],
                                video_Count=item['contentDetails']['itemCount'])
                        All_data.append(data)

                naxt_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break
        return All_data


# streamlit-Application Details
with st.sidebar:
    web=option_menu(
        menu_title="youtube data processing",
        options=["HOME","DATA COLLECTION","MIGRATE TO SQL","DATA ANALYSIS"],
        icons=["house","upload","database"]
 )
    
# Home_page settings
if web=="HOME":
    st.title("YouTube Data Harvesting and Warehousing using SQL and Streamlit")
    st.subheader(":red[Domain:]  Social Media")
    st.subheader(":red[Overview:]") 
    st.markdown('''Build a simple dashboard or UI using Streamlit and 
                retrieve YouTube channel data with the help of the YouTube API.
                Stored the data in an SQL database(warehousing),
                enable query in SQL and finally display in Streamlit''')
    st.subheader(":red[Skill-take:]")
    st.markdown("Python scripting,Data Collection,API integration,Data Management using SQL,Streamlit")

# Data-page setting
if web=="DATA COLLECTION":
    C=st.text_input("Enter the Channel ID")
    if C:          
        channel_s=get_channel_info(channel_id=C)
        video_s=get_video_info(video_ids=get_videos_ids(channel_id=C))
        playlist_s=get_playlist_details(channel_id=C)
        comments_s=get_comment_info(video_ids=get_videos_ids(channel_id=C))
    if st.button("submit"):
        st.success("Link submitted successfully")
        with st.spinner('Wait for it...'):

            time.sleep(5)
            st.write("channels")
            st.dataframe(channel_s)
            st.write("videos1")
            st.dataframe(video_s)
            st.write("comments")
            st.dataframe(comments_s)
            st.write("playlists")
            st.dataframe(playlist_s)


# SQL-page setting
if web=="MIGRATE TO SQL":
    C=st.text_input("Enter the Channel ID")
    if st.button("migrate to sql"):
        mydb=pymysql.connect(host="localhost",user="root",password="",database="youtube",port=3306)
        if mydb:
            print("Connected to MySQL database")

        # Create database if not exists
        mycursor=mydb.cursor()
        mycursor.execute("CREATE DATABASE IF NOT EXISTS youtube")      
          
        # Create table - CHANNEL        
        mycursor.execute('''CREATE TABLE IF NOT EXISTS channels(channel_Name varchar(100),
                                                                channel_Id varchar(80) primary key,
                                                                subscribers bigint,
                                                                view bigint,
                                                                Total_videos int,
                                                                channel_Description text,
                                                                playlist_Id varchar(80))''')

        # Create table - PLAYLIST
        mycursor.execute('''CREATE TABLE IF NOT EXISTS playlists(playlist_Id varchar(100),
                                                            Title varchar(100),
                                                            channel_Id varchar(100),
                                                            channel_Name varchar(100),
                                                            publishedAt timestamp,
                                                            video_Count int)''')
        # Create table - VIDEO
        mycursor.execute('''CREATE TABLE IF NOT EXISTS videos1(channel_Name varchar(100),
                                                    channel_Id varchar(100),
                                                    video_Id varchar(50) primary key,
                                                    Title varchar(150),                                                    
                                                    Thumbnail varchar(200),
                                                    Description text,
                                                    Published_Date timestamp,
                                                    Duration int,        
                                                    Views bigint,
                                                    Likes bigint,
                                                    Comments int,
                                                    Favorite_Count int,
                                                    Definition varchar(10),
                                                    caption_Status varchar(50))''')

        # create table-COMMENT
        mycursor.execute('''CREATE TABLE IF NOT EXISTS comments(comment_Id varchar(100) primary key,
                                                            video_Id varchar(50),
                                                            comment_Text text,
                                                            comment_Author varchar(150),
                                                            comment_Published timestamp)''')        
        
        #Transform corresponding data's into pandas dataframe
        df_channel=pd.DataFrame([get_channel_info(channel_id=C)])
        df_video=pd.DataFrame(get_video_info(video_ids=get_videos_ids(channel_id=C)))
        df_playlist=pd.DataFrame(get_playlist_details(channel_id=C))
        df_comment=pd.DataFrame(get_comment_info(video_ids=get_videos_ids(channel_id=C)))

        #Insert DataFrame into channel table           
        for index,row in df_channel.iterrows():
            insert_query='''INSERT INTO channels(channel_Name,channel_Id,subscribers,view,Total_videos,channel_Description,playlist_Id)                                                    
                                                values(%s,%s,%s,%s,%s,%s,%s)'''         
        
            values=(row['channel_Name'],row['channel_Id'],row['subscribers'],row['views'],row['Total_videos'],row['channel_Description'],row['playlist_Id'])
            mycursor.execute(insert_query,values)
        mydb.commit()        

        #Insert DataFrame into playlist table
        mycursor=mydb.cursor()
        for index,row in df_playlist.iterrows():
            insert_query='''INSERT INTO playlists(playlist_Id,Title,channel_Id,channel_Name,publishedAt,video_Count)                                                    
                                                values(%s,%s,%s,%s,%s,%s)'''
           
            values=(row['playlist_Id'],row['Title'],row['channel_Id'],row['channel_Name'],row['publishedAt'],row['video_Count'])         
    
            mycursor.execute(insert_query,values)
        mydb.commit()       

        #Insert DataFrame into video table        
        for index,row in df_video.iterrows():
            insert_query='''INSERT INTO videos1 values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            

            values=(row['channel_Name'],row['channel_Id'],row['video_Id'],row['Title'],row['Thumbnail'],row['Description'],row['Published_Date'],
                    row['Duration'],row['Views'],row['Likes'],row['Comments'],row['Favorite_Count'],row['Definition'],row['caption_Status'])
            
            mycursor.execute(insert_query,values)
        mydb.commit()        

        #Insert DataFrame into comment table    
        for index,row in df_comment.iterrows():
            insert_query='''INSERT INTO comments(comment_Id,video_Id,comment_Text,comment_Author,comment_Published)                                        
                                                values(%s,%s,%s,%s,%s)'''            

            values=(row['comment_Id'],row['video_Id'],row['comment_Text'],row['comment_Author'],row['comment_Published'])            
            
            mycursor.execute(insert_query,values)
        mydb.commit()                
        st.success("MIGRATED SUCCESSFULL")

#query and visualization-page setting
if web=="DATA ANALYSIS":
    mydb=pymysql.connect(host="localhost",user="root",password="",database="youtube",port=3306)
    if mydb:
        print("Connected to MySQL database")    
    mycursor=mydb.cursor()
    st.header("SELECT THE QUESTIONS TO GET INSIGHTS")
    options=st.selectbox("Select options",("1.What are the names of all the videos and their corresponding channels?",
                          "2.Which channels have the most number of videos, and how many videos do they have?",
                          "3.What are the top 10 most viewed videos and their respective channels?",
                          "4.How many comments were made on each video, and what are their corresponding video names?",
                          "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                          "6.What is the total number of likes and dislikes for each video, and what are  their corresponding video names?",
                          "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                          "8.What are the names of all the channels that have published videos in the year 2022?",
                          "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                          "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))
    
    # Query to excute 1st Question
    if options=="1.What are the names of all the videos and their corresponding channels?":
            if st.button("SUBMIT"):
                mycursor.execute(''' select channel_Name,video_Id from videos1 order by channel_Name  ''')
                out=mycursor.fetchall()
                que_1=pd.DataFrame(out,columns=["channel_Name","video_Id"])
                st.success("ANSWER")
                st.write(que_1)
        
    # Query to excute 2nd Question
    if options=="2.Which channels have the most number of videos, and how many videos do they have?":
            if st.button("SUBMIT"):
                mycursor.execute(''' select channel_Name,count(video_Id) from videos1 group by channel_Name 
                                        order by count(video_Id) desc ''')
                out=mycursor.fetchall()
                que_2=pd.DataFrame(out,columns=["channel_Name","video_count"])
                st.success("ANSWER")
                st.write(que_2)
        
    # Query to excute 3nd Question
    if options=="3.What are the top 10 most viewed videos and their respective channels?":
            if st.button("SUBMIT"):
                mycursor.execute(''' select channel_Id,video_Id,views from videos1 
                                        order by views desc ''')
                out=mycursor.fetchall()
                que_3=pd.DataFrame(out,columns=["channel_Id","video_Id","views_count"])
                st.success("ANSWER")
                st.write(que_3)
        
    # Query to excute 4th Question
    if options=="4.How many comments were made on each video, and what are their corresponding video names?":
            if st.button("SUBMIT"):
                mycursor.execute(''' select video_Id,Comments from videos1 
                                        order by Comments desc limit 100 ''')
                out=mycursor.fetchall()
                que_4=pd.DataFrame(out,columns=["video_Id","comments_count"])
                st.success("ANSWER")
                st.write(que_4)

    # Query to excute 5th Question
    if options=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
            if st.button("SUBMIT"):
                mycursor.execute(''' select videos1.video_Id,videos1.Likes,videos1.channel_Name from videos1 
                                        order by videos1.Likes desc limit 100 ''')
                out=mycursor.fetchall()
                que_5=pd.DataFrame(out,columns=["video_Id","Like_count","channel_Name"])
                st.success("ANSWER")
                st.write(que_5)
        
    # Query to excute 6th Question
    if options=="6.What is the total number of likes and dislikes for each video, and what are  their corresponding video names?":
            if st.button("SUBMIT"):
                mycursor.execute(''' select video_Id,Likes from videos1 
                                        order by Likes desc limit 100 ''')
                out=mycursor.fetchall()
                que_6=pd.DataFrame(out,columns=["video_Id","Like_count",])
                st.success("ANSWER")
                st.write(que_6)
        
    # Query to excute 8th Question
    if options=="8.What are the names of all the channels that have published videos in the year 2022?":
            if st.button("SUBMIT"):
                mycursor.execute(''' select video_Id,channel_Name,Published_Date from videos1 
                                        where year(videos1.Published_Date)=2022 ''')
                out=mycursor.fetchall()
                que_8=pd.DataFrame(out,columns=["video_Id","channel_Name","Published_Date"])
                st.success("ANSWER")
                st.write(que_8)
        
    # Query to excute 9th Question
    if options=="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
            if st.button("SUBMIT"):
                mycursor.execute(''' select channel_Name, avg(Duration)/60 as Durations from videos1 
                                        group by channel_Name
                                        order by Durations ''')
                out=mycursor.fetchall()
                que_9=pd.DataFrame(out,columns=["channel_Name","Durations"])
                st.success("ANSWER")
                st.write(que_9)

    # Query to excute 10th Question
    if options=="10.Which videos have the highest number of comments, and what are their corresponding channel names?":
            if st.button("SUBMIT"):
                mycursor.execute(''' select video_Id,Comments,channel_Name from videos1                                     
                                        order by Comments desc limit 100 ''')
                out=mycursor.fetchall()
                que_10=pd.DataFrame(out,columns=["channel_Name","Comment_Count","video_Id"])
                st.success("ANSWER")
                st.write(que_10)
            
            
            
            
            
            

            


        
        
        


    
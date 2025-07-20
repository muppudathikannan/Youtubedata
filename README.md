# Youtube_Data_Harvesting_and_Warehousing
## Introduction
  YouTube Data Harvesting and Warehousing is a project aimed at developing a user-friendly Streamlit application that leverages the power of   the Google API to extract valuable information from YouTube channels. The extracted data is then stored in a SQL data warehouse, and made    accessible for analysis and exploration within the Streamlit app.
## Table of Contents
  1.Pre-requisites
  2.Key Technologies and Skills
  3.Usage
  4.Data Scrapping
  5.Storing in MySQL
## Pre-requisites :
  Install the following packages to run the project :
  
  1.pip install streamlit
  2.pip install google-api-python-client
  3.pip install mysql-connector-python
  4.pip install pandas  
  5.pip install streamlit_option_menu
## Key Technologies and Skills
  1.Python scripting
  2.Web scraping
  3.MYSQL querying
  4.Application Programming Interface (API) integration
  5.Streamlit development
## Usage
  To use this project, follow these steps:
  
  1.Clone the repository
  2.Install the required packages
  3.Run the Streamlit app
  4.Access the app in your browser
## Data Scrapping
  The project uses Google API key to connect with youtube API and fetch the all details about the channel, given the channel ID. Further     the data scrapped is presented in JSON format to users of the streamit application.
## Storing in MYSQL
  Connecting python with MYSQL using mysql.connector allows us to store data in a structured format. Initally, we prepare the data by     extracting the required features in channel, video and comments section of the JSON document. Later, we perform a query to check if the channel scrapped already exists in our database or not. If it is avaiable, we are going to update the document with the specified channelID. Else, we insert the channel information as a new document into MYSQL.We have already created 4 tables in the backend called channel, video and comment. 


# This project is part of Udacity's Nanodegree program "Data Engineering"

# Purpose

The purpose of this project is to support Sparkify by creating an airflow pipeline which automates a hourly task to extract data from various json files residing in S3. The data is then transformed and loaded into Amazon Redshift.

# Data sources

Source data resides in S3 and comes in json files of the following formats:

## log_data (user attributes and activities like song plays)
```json

{ "artist": "N.E.R.D. FEATURING MALICE",
  "auth": "Logged In",
  "firstName": "Jayden",
  "gender": "M",
  "itemInSession": 0,
  "lastName": "Fox",
  "length": 288.9922,
  "level": "free",
  "location": "New Orleans-Metairie, LA",
  "method": "PUT",
  "page":"NextSong",
  "registration": 1541033612796.0,
  "sessionId": 184,
  "song": "Am I High (Feat. Malice)",
  "status": 200,
  "ts": 1541121934796,
  "userAgent": "\"Mozilla\/5.0 (Windows NT 6.3; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"",
  "userId": 101
}
```

## song_data (known songs and their artists)
```json
{ "num_songs": 1, 
  "artist_id": "ARJIE2Y1187B994AB7", 
  "artist_latitude": null, 
  "artist_longitude": null, 
  "artist_location": "", 
  "artist_name": "Line Renaud", 
  "song_id": "SOUPIRU12A6D4FA1E1", 
  "title": "Der Kleine Dompfaff", 
  "duration": 152.92036, 
  "year": 0
}
```

# Strategy

We first convert the JSONs residing in S3 into staging tables and then use these staging tables to fill our final fact and dimension tables.

# Schema (final tables)

We follow a star-schema with songplay table being the fact table. This enables us to run performant queries and we are also able to set up meaningful queries with ease.

## Fact table

### songplays (extracted from log files and linked to artists and songs)

|   Column    |  Type     |  Property   |
| ----------- | --------- | ----------- |
| songplay_id | integer   | Primary key |
| start_time  | timestamp |             |
| user_id     | integer   | Not Null    |
| level       | varchar   |             |
| song_id     | varchar   | Not Null    |
| artist_id   | varchar   | Not Null    |
| session_id  | integer   | Not Null    |
| location    | varchar   |             |
| user_agent  | varchar   |             |

## Dimension tables

### users (users in database)

|   Column    |  Type     |  Property   |
| ----------- | --------- | ----------- |
| user_id     | integer   | Primary key |
| first_name  | varchar   |             |
| last_name   | varchar   |             |
| gender      | varchar   |             |
| level       | varchar   |             |

### songs (known songs)

|   Column    |  Type     |  Property   |
| ----------- | --------- | ----------- |
| song_id     | varchar   | Primary key |
| title       | varchar   |             |
| artist_id   | varchar   | Not Null    |
| year        | integer   |             |
| duration    | numeric   |             |

### artists (known artists)

|   Column    |  Type     |  Property   |
| ----------- | --------- | ----------- |
| artist_id   | varchar   | Primary key |
| name        | varchar   |             |
| location    | varchar   |             |
| latitude    | numeric   |             |
| longitude   | numeric   |             |

### time (timestamps of song plays split into different units)

|   Column    |  Type     |  Property   |
| ----------- | --------- | ----------- |
| start_time  | timestamp | Primary key |
| hour        | integer   |             |
| day         | integer   |             |
| week        | integer   |             |
| month       | integer   |             |
| year        | integer   |             |
| weekday     | integer   |             |
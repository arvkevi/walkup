# MLB Walk-Up Songs Database

This repository contains data and scripts for managing a database of MLB walk-up songs.

## Setup Instructions

### Local Database Setup

1. Make sure PostgreSQL is installed on your system.
2. Install the required Python packages:

   ```
   pip install -r requirements.txt
   ```

3. Create a local PostgreSQL database:

   ```
   createdb mlb_walk_up_songs
   ```

4. Load the data from the CSV file:

   ```
   python load_data.py
   ```

### AWS RDS Setup

1. Edit the `.env` file with your AWS credentials:

   ```
   AWS_ACCESS_KEY_ID=your_access_key
   AWS_SECRET_ACCESS_KEY=your_secret_key
   AWS_REGION=us-east-1
   RDS_USERNAME=postgres
   RDS_PASSWORD=your_password
   RDS_SECURITY_GROUP=your_security_group_id
   ```

2. Run the AWS RDS setup script:

   ```
   python aws_db_setup.py
   ```

## Database Schema

The `mlb_walk_up_songs` table has the following columns:

- `player`: Player name
- `song_name`: Name of the walk-up song
- `song_artist`: Artist of the song
- `team`: Player's team
- `walkup_date`: Date when the walk-up song was used
- `spotify_uri`: Spotify URI for the song
- `explicit`: Boolean indicating if the song has explicit content

## Usage

You can connect to the database using the following connection string:

- Local: `postgresql://localhost:5432/mlb_walk_up_songs`
- AWS RDS: The endpoint will be displayed after running the AWS RDS setup script.

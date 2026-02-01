# MLB Walk-Up Songs

A Streamlit app to browse MLB player walk-up songs and create Spotify playlists.

## Features

- Browse walk-up songs from all MLB teams
- Filter by team, date range, explicit content, and Spotify availability
- Preview songs with embedded Spotify player
- Create Spotify playlists from selected songs
- Daily automated scraping via GitHub Actions

## Quick Start

1. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Set up environment variables in `.env`:

   ```
   SUPABASE_URL=your_supabase_url
   SUPABASE_KEY=your_supabase_anon_key
   SPOTIFY_CLIENT_ID=your_spotify_client_id
   SPOTIFY_CLIENT_SECRET=your_spotify_client_secret
   REDIRECT_URI=http://localhost:8501/callback
   ```

3. Run the app:

   ```bash
   streamlit run app.py
   ```

## Database Setup (Supabase)

1. Create a new Supabase project
2. Run `supabase_schema.sql` in the SQL Editor to create the tables
3. Copy your project URL and anon key to `.env`

## Scraper

The scraper runs daily via GitHub Actions to update the database with current walk-up songs from MLB.com.

To run manually:

```bash
python scraper.py "$SPOTIFY_CLIENT_ID" "$SPOTIFY_CLIENT_SECRET"
```

Required environment variables for scraper:

- `DATABASE_URL`: PostgreSQL connection string (Supabase)
- `SPOTIFY_CLIENT_ID`: Spotify API client ID
- `SPOTIFY_CLIENT_SECRET`: Spotify API client secret

## Database Schema

### mlb_walk_up_songs

- `id` (BIGINT): Primary key
- `team` (VARCHAR): MLB team name
- `player` (VARCHAR): Player name
- `song_name` (VARCHAR): Walk-up song title
- `song_artist` (VARCHAR): Song artist
- `spotify_uri` (VARCHAR): Spotify track URI
- `explicit` (BOOLEAN): Explicit content flag
- `first_seen_date` (DATE): When song was first seen
- `last_updated_date` (DATE): Last time song was active
- `is_current` (BOOLEAN): Whether song is currently active

### batting_stats

Daily batting statistics per player (for causal analysis of song changes).

### song_changes

Tracks when players change their walk-up songs.

## Tech Stack

- **Frontend**: Streamlit
- **Database**: Supabase (PostgreSQL)
- **Authentication**: Spotify OAuth
- **Scraping**: BeautifulSoup, requests
- **CI/CD**: GitHub Actions

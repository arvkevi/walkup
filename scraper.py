#!/usr/bin/env python3
"""
Fixed MLB Walk-up Songs Scraper

This fixed version addresses the critical issue where all songs were being assigned 
the current scrape date as their "walkup_date", creating artificial clustering in 
causal analysis.

Key Changes:
1. Uses first_seen_date and last_updated_date instead of walkup_date
2. Only tracks when songs are first discovered or changed
3. Maintains historical tracking of actual song changes
4. Prevents artificial temporal clustering in causal analysis

Usage: python scraper.py <spotify_client_id> <spotify_client_secret> [--verbose]
"""

import datetime
from bs4 import BeautifulSoup
import requests
import pandas as pd
from sqlalchemy import create_engine, text
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import time
import sys
import re
from pytz import timezone
import backoff  # For retrying failed operations
import socket
import os
import logging
import psycopg2
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import OperationalError
from urllib.parse import urlparse

EST = timezone("US/Eastern")


def log(message, verbose=False):
    """Log message if verbose mode is on or if verbose parameter is False."""
    if not verbose or (verbose and VERBOSE_MODE):
        sys.stdout.write(f"{message}\n")


# Global verbose mode flag
VERBOSE_MODE = False

# List of MLB team names and their corresponding URLs
MLB_TEAMS = {
    "orioles": "orioles",
    "redsox": "redsox",
    "yankees": "yankees",
    "rays": "rays",
    "bluejays": "bluejays",
    "whitesox": "whitesox",
    "guardians": "guardians",
    "tigers": "tigers",
    "royals": "royals",
    "twins": "twins",
    "athletics": "athletics",
    "astros": "astros",
    "angels": "angels",
    "mariners": "mariners",
    "rangers": "rangers",
    "braves": "braves",
    "marlins": "marlins",
    "mets": "mets",
    "phillies": "phillies",
    "nationals": "nationals",
    "reds": "reds",
    "brewers": "brewers",
    "pirates": "pirates",
    "cardinals": "cardinals",
    "dbacks": "dbacks",
    "rockies": "rockies",
    "dodgers": "dodgers",
    "padres": "padres",
    "giants": "giants",
}


def verify_database_schema(engine):
    """Verify the database schema exists with required columns.

    The schema should be created via supabase_schema.sql before running the scraper.
    """
    # Check what tables exist (for debugging)
    with engine.connect() as conn:
        # List all tables in public schema
        tables_result = conn.execute(text("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        """))
        tables = [row[0] for row in tables_result]
        log(f"📋 Tables in database: {tables}")

        # Check for mlb_walk_up_songs with is_current column
        check_sql = """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'mlb_walk_up_songs'
          AND column_name = 'is_current'
        """
        result = conn.execute(text(check_sql))
        if result.fetchone():
            log("✅ Database schema verified")
            return

        raise ValueError(
            "Table 'mlb_walk_up_songs' not found or missing 'is_current' column. "
            "Run supabase_schema.sql in Supabase SQL Editor first."
        )


def get_existing_songs(engine):
    """Get existing songs from database to compare for changes."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT team, player, song_name, song_artist, spotify_uri, explicit, 
                       first_seen_date, last_updated_date
                FROM mlb_walk_up_songs 
                WHERE is_current = TRUE
            """))
            
            existing_songs = {}
            for row in result:
                key = (row.team, row.player)
                if key not in existing_songs:
                    existing_songs[key] = []
                existing_songs[key].append({
                    'song_name': row.song_name,
                    'song_artist': row.song_artist,
                    'spotify_uri': row.spotify_uri,
                    'explicit': row.explicit,
                    'first_seen_date': row.first_seen_date,
                    'last_updated_date': row.last_updated_date
                })
            
            return existing_songs
    except Exception as e:
        log(f"Error getting existing songs: {e}")
        return {}


def detect_song_changes(current_songs, existing_songs, scrape_date):
    """
    Detect actual song changes by comparing current scrape with existing data.
    
    Returns:
    - new_songs: Songs never seen before
    - changed_songs: Players who changed their songs
    - unchanged_songs: Songs that remain the same
    """
    new_songs = []
    changed_songs = []
    unchanged_songs = []
    
    for song_data in current_songs:
        team = song_data['team']
        player = song_data['player']
        song_name = song_data['song_name']
        
        player_key = (team, player)
        
        if player_key not in existing_songs:
            # Completely new player
            new_songs.append({
                **song_data,
                'first_seen_date': scrape_date,
                'last_updated_date': scrape_date,
                'is_current': True
            })
        else:
            # Check if this is a new song for this player
            existing_player_songs = existing_songs[player_key]
            song_exists = any(
                existing['song_name'] == song_name 
                for existing in existing_player_songs
            )
            
            if song_exists:
                # Song unchanged, just update last_updated_date
                unchanged_songs.append({
                    **song_data,
                    'last_updated_date': scrape_date
                })
            else:
                # This is a song change!
                changed_songs.append({
                    **song_data,
                    'first_seen_date': scrape_date,  # This is when we first saw this new song
                    'last_updated_date': scrape_date,
                    'is_current': True
                })
    
    return new_songs, changed_songs, unchanged_songs


def store_songs_with_change_tracking(engine, new_songs, changed_songs, unchanged_songs, scrape_date):
    """Store songs with proper change tracking."""
    try:
        with engine.connect() as conn:
            # Start transaction
            trans = conn.begin()
            
            # Mark old songs as no longer current for players with changes
            if changed_songs:
                players_with_changes = [(song['team'], song['player']) for song in changed_songs]
                for team, player in players_with_changes:
                    conn.execute(text("""
                        UPDATE mlb_walk_up_songs 
                        SET is_current = FALSE, updated_at = CURRENT_TIMESTAMP
                        WHERE team = :team AND player = :player AND is_current = TRUE
                    """), {'team': team, 'player': player})
            
            # Insert new songs
            all_new_entries = new_songs + changed_songs
            if all_new_entries:
                conn.execute(text("""
                    INSERT INTO mlb_walk_up_songs 
                    (team, player, song_name, song_artist, spotify_uri, explicit, 
                     first_seen_date, last_updated_date, is_current)
                    VALUES (:team, :player, :song_name, :song_artist, :spotify_uri, 
                            :explicit, :first_seen_date, :last_updated_date, :is_current)
                """), all_new_entries)
            
            # Update last_updated_date for unchanged songs
            if unchanged_songs:
                for song in unchanged_songs:
                    conn.execute(text("""
                        UPDATE mlb_walk_up_songs 
                        SET last_updated_date = :last_updated_date, updated_at = CURRENT_TIMESTAMP
                        WHERE team = :team AND player = :player AND song_name = :song_name
                    """), song)
            
            trans.commit()
            
            log(f"✅ Stored: {len(new_songs)} new songs, {len(changed_songs)} changes, {len(unchanged_songs)} unchanged")
            
            if changed_songs:
                log("🎵 SONG CHANGES DETECTED:")
                for song in changed_songs[:10]:  # Show first 10 changes
                    log(f"   {song['team']} - {song['player']}: {song['song_name']}")
                if len(changed_songs) > 10:
                    log(f"   ... and {len(changed_songs) - 10} more changes")
                    
    except Exception as e:
        log(f"❌ Error storing songs: {e}")
        raise


def get_team_links():
    """Get all MLB team links."""
    try:
        mlb_site = "https://www.mlb.com"
        team_links = {}

        # Get team links from the fans page
        bs = BeautifulSoup(requests.get(f"{mlb_site}/fans").text, "html.parser")
        links = bs.find_all("a", {"data-parent": "Teams"}, href=True)
        for link in links:
            music_url = f"{mlb_site}{link['href']}/ballpark/music"
            team_name = music_url.split("/")[-3]
            team_links[team_name] = music_url
            log(f"Found team link: {music_url}")

        if not team_links:
            log("No team links found. Exiting.")
            return {}

        log(f"Found {len(team_links)} team links")
        return team_links
    except Exception as e:
        log(f"Error getting team links: {str(e)}")
        return {}


def validate_connection_uri(uri):
    """Validate the connection URI format."""
    if not uri:
        raise ValueError("Connection URI cannot be empty")

    if not uri.startswith(("postgresql://", "postgresql+psycopg2://")):
        raise ValueError(
            "Connection URI must start with postgresql:// or postgresql+psycopg2://"
        )

    try:
        # Extract host from URI
        parts = uri.split("@")
        if len(parts) != 2:
            raise ValueError(
                "Invalid connection URI format. Expected format: postgresql://user:pass@host:port/dbname"
            )

        host_parts = parts[1].split("/")
        if len(host_parts) < 2:
            raise ValueError("Invalid connection URI format. Missing database name")

        host = host_parts[0]
        if not host:
            raise ValueError("Host cannot be empty in connection URI")

        return True
    except Exception as e:
        raise ValueError(f"Invalid connection URI format: {str(e)}")


def get_database_engine():
    """Create a database connection with connection pooling and retries."""
    try:
        # Option 1: Use DATABASE_URL directly (recommended for Supabase)
        connection_uri = os.getenv("DATABASE_URL")

        if not connection_uri:
            # Option 2: Build from individual env vars (legacy support)
            db_user = os.getenv("DB_USER")
            db_password = os.getenv("DB_PASSWORD")
            db_host = os.getenv("DB_HOST")
            db_port = os.getenv("DB_PORT", "5432")
            db_name = os.getenv("DB_NAME")

            if not all([db_user, db_password, db_host, db_name]):
                raise ValueError(
                    "Set DATABASE_URL or DB_USER, DB_PASSWORD, DB_HOST, DB_NAME env vars"
                )

            connection_uri = (
                f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            )

        # Create connection pool with optimized settings
        engine = create_engine(
            connection_uri,
            poolclass=QueuePool,
            pool_size=3,
            max_overflow=5,
            pool_timeout=60,
            pool_pre_ping=True,
            pool_recycle=1800,
            connect_args={
                "connect_timeout": 30,
                "keepalives": 1,
                "keepalives_idle": 60,
                "keepalives_interval": 30,
                "keepalives_count": 3,
                "application_name": "mlb_walkup_scraper",
                "options": "-c statement_timeout=60000",
            },
        )

        # Test the connection with a simple query
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            log("Database connection test successful")
            return engine

    except Exception as e:
        log(f"Database connection error: {str(e)}")
        raise


def scrape_team_songs(url, team, sp):
    """Scrape songs for a specific team."""
    try:
        # Configure requests session with headers to mimic a browser
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Connection": "keep-alive",
            }
        )

        # Add retry logic for requests
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = session.get(url, timeout=30)
                response.raise_for_status()
                break
            except (requests.RequestException, requests.Timeout) as e:
                if attempt == max_retries - 1:
                    raise
                log(f"Attempt {attempt + 1} failed: {str(e)}, retrying...")
                time.sleep(2**attempt)

        bsteam = BeautifulSoup(response.text, "html.parser")
        log(f"Page content length: {len(response.text)}")

        songs = []
        # Method 1: Try finding content in the forge list
        try:
            players = bsteam.find("div", {"class": "p-forge-list"}).findAll(
                "div", {"class": "p-featured-content__body"}
            )
            for player in players:
                player_name = player.find("div", {"class": "u-text-h4"}).text.strip()
                p_tag = player.find("div", {"class": "p-featured-content__text"}).find(
                    ["p", "span"]
                )
                spans = p_tag.find_all("span")

                player_songs = set()
                # Extract song names and artists
                for span in spans:
                    text = span.get_text().strip()
                    if " by " in text:
                        song, artist = text.split(" by ", 1)
                        player_songs.add((song.strip(), artist.strip()))

                if not player_songs:
                    for a_tag in p_tag.find_all("a"):
                        try:
                            song_name = a_tag.em.get_text().strip()
                            artist_name = a_tag.next_sibling.strip(" by ")
                            player_songs.add((song_name, artist_name))
                        except AttributeError:
                            pass

                if player_songs:
                    for song_name, song_artist in player_songs:
                        # Search for song on Spotify
                        spotify_data = None
                        if sp and song_name and song_artist:
                            try:
                                search_query = f"track:{song_name} artist:{song_artist}"
                                results = sp.search(
                                    q=search_query, type="track", limit=1
                                )
                                if results["tracks"]["items"]:
                                    spotify_data = results["tracks"]["items"][0]
                                time.sleep(0.2)
                            except Exception as e:
                                log(f"Spotify search error: {e}")
                                spotify_data = None

                        songs.append(
                            {
                                "team": team,
                                "player": player_name,
                                "song_name": song_name,
                                "song_artist": song_artist,
                                "spotify_uri": (
                                    spotify_data["uri"] if spotify_data else None
                                ),
                                "explicit": (
                                    spotify_data["explicit"] if spotify_data else None
                                ),
                            }
                        )

        except Exception:
            log(f"{team}: forge list method failed, trying walkup music method...")

        # Method 2: Try finding content in the walkup music table
        if not songs:
            try:
                song_table = bsteam.find("div", {"data-testid": "player-walkup-music"})
                table = song_table.find("table")

                # Find all player entries
                player_entries = table.find_all(
                    "tr", {"data-selected": "false", "data-underlined": "false"}
                )

                for entry in player_entries:
                    # Extract the player name
                    player_first_name = entry.find(
                        "div", {"data-testid": re.compile(r"spot-tag__super-name")}
                    )
                    player_last_name = entry.find(
                        "div", {"data-testid": re.compile(r"spot-tag__name")}
                    )

                    if player_first_name and player_last_name:
                        player_first_name = " ".join(
                            tag.get_text() for tag in player_first_name
                        )
                        player_last_name = " ".join(
                            tag.get_text() for tag in player_last_name
                        )
                        player_name = f"{player_first_name} {player_last_name}".strip()

                        # Find all songs for this player
                        player_songs = entry.find_all(
                            "div",
                            {
                                "data-testid": re.compile(
                                    r"player-walkup-music-song-content-\d+"
                                )
                            },
                        )

                        for song in player_songs:
                            song_name = (
                                song.find(
                                    "div",
                                    {
                                        "class": "player-walkup-music__song--content--songname"
                                    },
                                )
                                .get_text()
                                .strip()
                            )
                            artist_name = (
                                song.find(
                                    "div",
                                    {
                                        "class": "player-walkup-music__song--content--artistname"
                                    },
                                )
                                .get_text()
                                .strip()
                            )

                            if song_name and artist_name:
                                # Search for song on Spotify
                                spotify_data = None
                                if sp and song_name and artist_name:
                                    try:
                                        search_query = (
                                            f"track:{song_name} artist:{artist_name}"
                                        )
                                        results = sp.search(
                                            q=search_query, type="track", limit=1
                                        )
                                        if results["tracks"]["items"]:
                                            spotify_data = results["tracks"]["items"][0]
                                        time.sleep(0.2)
                                    except Exception:
                                        log(f"Spotify search error for {song_name}")
                                        spotify_data = None

                                songs.append(
                                    {
                                        "team": team,
                                        "player": player_name,
                                        "song_name": song_name,
                                        "song_artist": artist_name,
                                        "spotify_uri": (
                                            spotify_data["uri"]
                                            if spotify_data
                                            else None
                                        ),
                                        "explicit": (
                                            spotify_data["explicit"]
                                            if spotify_data
                                            else None
                                        ),
                                    }
                                )

            except Exception:
                log(f"{team}: walkup music method failed, trying simple table method...")

        # Method 3: Simple HTML table (used by Angels and some other teams)
        if not songs:
            try:
                # Find tables on the page
                tables = bsteam.find_all("table")
                for table in tables:
                    # Look for tables with PLAYER, SONG, ARTIST headers
                    headers = table.find_all("th")
                    header_texts = [h.get_text().strip().upper() for h in headers]

                    if "PLAYER" in header_texts and "SONG" in header_texts:
                        player_idx = header_texts.index("PLAYER")
                        song_idx = header_texts.index("SONG")
                        artist_idx = header_texts.index("ARTIST") if "ARTIST" in header_texts else None

                        rows = table.find_all("tr")
                        current_player = None

                        for row in rows:
                            cells = row.find_all("td")
                            if not cells or len(cells) < 2:
                                continue

                            # Get player name (may be empty for multi-song players)
                            player_cell = cells[player_idx].get_text().strip()
                            if player_cell:
                                current_player = player_cell

                            if not current_player:
                                continue

                            # Get song name
                            song_name = cells[song_idx].get_text().strip() if len(cells) > song_idx else ""

                            # Get artist name
                            artist_name = ""
                            if artist_idx is not None and len(cells) > artist_idx:
                                artist_name = cells[artist_idx].get_text().strip()

                            if song_name:
                                # Search for song on Spotify
                                spotify_data = None
                                if sp and song_name and artist_name:
                                    try:
                                        search_query = f"track:{song_name} artist:{artist_name}"
                                        results = sp.search(q=search_query, type="track", limit=1)
                                        if results["tracks"]["items"]:
                                            spotify_data = results["tracks"]["items"][0]
                                        time.sleep(0.2)
                                    except Exception:
                                        log(f"Spotify search error for {song_name}")
                                        spotify_data = None

                                songs.append({
                                    "team": team,
                                    "player": current_player,
                                    "song_name": song_name,
                                    "song_artist": artist_name,
                                    "spotify_uri": spotify_data["uri"] if spotify_data else None,
                                    "explicit": spotify_data["explicit"] if spotify_data else None,
                                })

                        if songs:
                            break  # Found songs, no need to check other tables

            except Exception as e:
                log(f"{team}: simple table method failed: {str(e)}")

        if songs:
            log(f"Found {len(songs)} songs for {team}")
            if VERBOSE_MODE:
                for song in songs:
                    log(f"  Player: {song['player']}")
                    log(f"    Song: {song['song_name']} by {song['song_artist']}")
                    if song["spotify_uri"]:
                        log(f"    Spotify URI: {song['spotify_uri']}")
        else:
            log(f"No songs found for {team}")

        return songs

    except Exception as e:
        log(f"Error scraping {team}: {str(e)}")
        return []


def scrape_all_teams(spotify_client_id, spotify_client_secret):
    """Scrape all MLB teams and return combined song data."""
    try:
        # Initialize Spotify client
        sp = None
        if spotify_client_id and spotify_client_secret:
            try:
                client_credentials_manager = SpotifyClientCredentials(
                    client_id=spotify_client_id, client_secret=spotify_client_secret
                )
                sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
                log("Spotify client initialized successfully")
            except Exception as e:
                log(f"Warning: Failed to initialize Spotify client: {e}")

        # Get team links
        team_links = get_team_links()
        if not team_links:
            log("No team links found")
            return []

        all_songs = []
        # Process each team
        for team, url in team_links.items():
            try:
                log(f"\nProcessing team: {team}")
                songs = scrape_team_songs(url, team, sp)
                all_songs.extend(songs)

            except Exception:
                log(f"Error processing team {team}")
                continue

        log(f"\nScraping completed. Total songs found: {len(all_songs)}")
        return all_songs

    except Exception as e:
        log(f"Error in scrape_all_teams: {str(e)}")
        raise


def main():
    """Main scraper function with fixed change tracking."""
    if len(sys.argv) < 3:
        print("Usage: python scraper.py <spotify_client_id> <spotify_client_secret> [--verbose]")
        sys.exit(1)
    
    global VERBOSE_MODE
    VERBOSE_MODE = "--verbose" in sys.argv
    
    scrape_date = datetime.datetime.now(EST).date()
    log(f"🚀 Starting fixed MLB walkup songs scraper on {scrape_date}")
    
    # Get database connection
    engine = get_database_engine()
    if not engine:
        log("❌ Failed to connect to database")
        sys.exit(1)
    
    # Create fixed schema
    verify_database_schema(engine)
    
    # Get existing songs for comparison
    existing_songs = get_existing_songs(engine)
    log(f"📊 Found {len(existing_songs)} players with existing songs")
    
    # Scrape current songs (using existing scraper logic)
    current_songs = scrape_all_teams(sys.argv[1], sys.argv[2])
    log(f"🎵 Scraped {len(current_songs)} current songs")
    
    # Detect changes
    new_songs, changed_songs, unchanged_songs = detect_song_changes(
        current_songs, existing_songs, scrape_date
    )
    
    # Store with proper change tracking
    store_songs_with_change_tracking(
        engine, new_songs, changed_songs, unchanged_songs, scrape_date
    )
    
    log("✅ Scraper completed successfully!")
    log(f"📈 Summary: {len(new_songs)} new, {len(changed_songs)} changed, {len(unchanged_songs)} unchanged")


if __name__ == "__main__":
    main()

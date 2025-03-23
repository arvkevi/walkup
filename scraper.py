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


def get_team_links():
    """Get all MLB team links."""
    try:
        mlb_site = "https://www.mlb.com"
        team_links = []

        # Get team links from the fans page
        bs = BeautifulSoup(requests.get(f"{mlb_site}/fans").text, "html.parser")
        links = bs.find_all("a", {"data-parent": "Teams"}, href=True)
        for link in links:
            music_url = f"{mlb_site}{link['href']}/ballpark/music"
            team_links.append(music_url)
            log(f"Found team link: {music_url}")

        if not team_links:
            log("No team links found. Exiting.")
            return []

        log(f"Found {len(team_links)} team links")
        return team_links
    except Exception as e:
        log(f"Error getting team links: {str(e)}")
        return []


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


def get_db_connection(connection_uri):
    """Create a database connection with connection pooling and retries."""
    try:
        # Get connection parameters from environment variables
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT", "5432")
        db_name = os.getenv("DB_NAME")

        if not all([db_user, db_password, db_host, db_name]):
            raise ValueError(
                "Missing required database connection parameters in environment variables"
            )

        # Construct connection URI from environment variables
        connection_uri = (
            f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        )

        # Create connection pool with optimized settings
        engine = create_engine(
            connection_uri,
            poolclass=QueuePool,
            pool_size=3,  # Reduced pool size
            max_overflow=5,  # Reduced max overflow
            pool_timeout=60,  # Increased timeout
            pool_pre_ping=True,
            pool_recycle=1800,  # Recycle connections after 30 minutes
            connect_args={
                "connect_timeout": 30,  # Increased connection timeout
                "keepalives": 1,
                "keepalives_idle": 60,  # Increased idle time
                "keepalives_interval": 30,  # Increased interval
                "keepalives_count": 3,  # Reduced count
                "application_name": "mlb_walkup_scraper",  # Added application name
                "options": "-c statement_timeout=60000",  # 60 second statement timeout
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


def store_records(df, engine):
    """Store records in the database with retries and chunking."""
    try:
        # Split DataFrame into smaller chunks
        chunk_size = 100
        total_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size else 0)

        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i : i + chunk_size]
            chunk_num = (i // chunk_size) + 1

            log(f"Processing chunk {chunk_num}/{total_chunks} ({len(chunk)} records)")

            # Retry logic for each chunk
            max_retries = 3
            retry_delay = 5

            for attempt in range(max_retries):
                try:
                    # Use SQLAlchemy's bulk_insert_mappings for better performance
                    with engine.connect() as conn:
                        conn.execute(
                            text(
                                """
                                INSERT INTO walkup_songs 
                                (team, player, song_name, song_artist, walkup_date, spotify_uri, explicit)
                                VALUES (:team, :player, :song_name, :song_artist, :walkup_date, :spotify_uri, :explicit)
                                ON CONFLICT (team, player, song_name) 
                                DO UPDATE SET
                                    song_artist = EXCLUDED.song_artist,
                                    walkup_date = EXCLUDED.walkup_date,
                                    spotify_uri = EXCLUDED.spotify_uri,
                                    explicit = EXCLUDED.explicit
                            """
                            ),
                            chunk.to_dict("records"),
                        )
                        conn.commit()
                        log(f"Successfully stored chunk {chunk_num}")
                        break

                except OperationalError as e:
                    if attempt < max_retries - 1:
                        log(
                            f"Database error on chunk {chunk_num}, attempt {attempt + 1}/{max_retries}: {str(e)}"
                        )
                        time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
                        continue
                    else:
                        log(
                            f"Failed to store chunk {chunk_num} after {max_retries} attempts"
                        )
                        raise

                except Exception as e:
                    log(f"Unexpected error storing chunk {chunk_num}: {str(e)}")
                    raise

    except Exception as e:
        log(f"Error storing records: {str(e)}")
        raise


@backoff.on_exception(backoff.expo, Exception, max_tries=5)
def scrape_and_store(
    connection_uri, spotify_client_id, spotify_client_secret, dry_run=False
):
    """Scrape MLB walk-up songs and store them in the database."""
    log(
        f"Scraping MLB walk-up songs...\n"
        f"Spotify Client ID is None: {spotify_client_id is None}\n"
        f"Spotify Client Secret is None: {spotify_client_secret is None}\n"
        f"Dry run mode: {dry_run}\n"
        f"Verbose mode: {VERBOSE_MODE}\n"
    )

    # Validate connection URI first
    try:
        validate_connection_uri(connection_uri)
    except ValueError as e:
        log(f"Invalid connection URI: {str(e)}")
        sys.exit(1)

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

    team_links = get_team_links()
    if not team_links:
        log("No team links found. Exiting.")
        sys.exit(1)

    team_songs = {}
    total_songs_found = 0
    for team_link in team_links:
        team_name = team_link.split("/")[-3]
        log(f"\nProcessing team: {team_name}")
        log(f"URL: {team_link}")
        try:
            # Add retry logic for requests
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = session.get(team_link, timeout=30)
                    response.raise_for_status()
                    break
                except (requests.RequestException, requests.Timeout) as e:
                    if attempt == max_retries - 1:
                        raise
                    log(f"Attempt {attempt + 1} failed: {str(e)}, retrying...")
                    time.sleep(2**attempt)  # Exponential backoff

            bsteam = BeautifulSoup(response.text, "html.parser")
            log(f"Page content length: {len(response.text)}")

            # Method 1: Try finding content in the forge list
            try:
                players = bsteam.find("div", {"class": "p-forge-list"}).findAll(
                    "div", {"class": "p-featured-content__body"}
                )
                player_songs = {}
                for player in players:
                    player_name = player.find(
                        "div", {"class": "u-text-h4"}
                    ).text.strip()
                    player_songs[player_name] = []
                    p_tag = player.find(
                        "div", {"class": "p-featured-content__text"}
                    ).find(["p", "span"])
                    spans = p_tag.find_all("span")

                    songs = set()
                    # Extract song names and artists
                    for span in spans:
                        text = span.get_text().strip()
                        if " by " in text:
                            song, artist = text.split(" by ", 1)
                            songs.add((song.strip(), artist.strip()))

                    if not songs:
                        for a_tag in p_tag.find_all("a"):
                            try:
                                song_name = a_tag.em.get_text().strip()
                                artist_name = a_tag.next_sibling.strip(" by ")
                                songs.add((song_name, artist_name))
                            except:
                                pass

                    if songs:
                        for song, artist in songs:
                            player_songs[player_name].append(
                                {"song_name": song, "song_artist": artist}
                            )

                    if not songs:
                        # Additional code to get song name and artist name
                        p_text_only = ""
                        for content in p_tag.contents:
                            if content.name is None:  # Text, not a tag
                                p_text_only += content

                        p_text_only = p_text_only.strip()
                        em_tag = p_tag.find("em") if p_tag else None
                        i_tag = p_tag.find("i") if p_tag else None

                        if em_tag:
                            song_name = em_tag.text
                        elif i_tag:
                            song_name = i_tag.text
                        else:
                            song_name = ""

                        song_artist = (
                            p_tag.text.replace(song_name, "").replace("by", "").strip()
                        )
                        player_songs[player_name].append(
                            {"song_name": song_name, "song_artist": song_artist}
                        )

                team_songs[team_name] = player_songs
                log(f"Found {len(player_songs)} players using forge list method")

            except Exception as e:
                log(
                    f"{team_name}: forge list method failed, trying walkup music method..."
                )

            # Method 2: Try finding content in the walkup music table
            if team_name not in team_songs:
                try:
                    song_table = bsteam.find(
                        "div", {"data-testid": "player-walkup-music"}
                    )
                    table = song_table.find("table")

                    # Find all player entries
                    player_entries = table.find_all(
                        "tr", {"data-selected": "false", "data-underlined": "false"}
                    )

                    # Initialize player songs dictionary
                    player_songs = {}

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
                            player_name = (
                                f"{player_first_name} {player_last_name}".strip()
                            )

                            # Find all songs for this player
                            player_songs[player_name] = []
                            songs = entry.find_all(
                                "div",
                                {
                                    "data-testid": re.compile(
                                        r"player-walkup-music-song-content-\d+"
                                    )
                                },
                            )

                            for song in songs:
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
                                    player_songs[player_name].append(
                                        {
                                            "song_name": song_name,
                                            "song_artist": artist_name,
                                        }
                                    )

                    if player_songs:
                        team_songs[team_name] = player_songs
                        log(
                            f"Found {len(player_songs)} players using walkup music method"
                        )

                except Exception as e:
                    log(f"{team_name}: walkup music method failed, skipping...")

            if team_name in team_songs:
                total_songs_found += sum(
                    len(songs) for songs in team_songs[team_name].values()
                )
                if VERBOSE_MODE:
                    for player_name, songs in team_songs[team_name].items():
                        log(f"\n  Player Name: {player_name}", verbose=True)
                        for idx, song in enumerate(songs, 1):
                            log(
                                f"    Song #{idx}:\n"
                                f"      Title:  {song['song_name']}\n"
                                f"      Artist: {song['song_artist']}",
                                verbose=True,
                            )
            else:
                log(f"No songs found for {team_name}")

        except Exception as e:
            log(f"Error scraping {team_name}: {str(e)}")
            continue

    if not team_songs:
        log("No songs found for any team. Exiting.")
        sys.exit(1)

    log(f"\nTotal songs found across all teams: {total_songs_found}")

    if dry_run:
        log("Dry run mode - skipping database operations")
        return

    # Initialize Spotify client
    spotify_search = None
    if spotify_client_id and spotify_client_secret:
        spotify_search = spotipy.Spotify(
            client_credentials_manager=SpotifyClientCredentials(
                client_id=spotify_client_id, client_secret=spotify_client_secret
            )
        )

    # Search for songs on Spotify and prepare records
    records = []
    spotify_hits = 0
    spotify_misses = 0

    for team, players in team_songs.items():
        log(f"\nProcessing Spotify data for team: {team}")
        for player, songs in players.items():
            log(f"  Processing player: {player}", verbose=True)
            for song in songs:
                try:
                    spotify_data = None
                    if spotify_search and song["song_name"] and song["song_artist"]:
                        try:
                            search_query = f"track:{song['song_name']} artist:{song['song_artist']}"
                            log(f"    Searching Spotify: {search_query}", verbose=True)
                            results = spotify_search.search(
                                q=search_query,
                                type="track",
                                limit=1,
                            )
                            if results["tracks"]["items"]:
                                spotify_data = results["tracks"]["items"][0]
                                spotify_hits += 1
                                log("    ✓ Found on Spotify", verbose=True)
                            else:
                                spotify_misses += 1
                                log("    ✗ Not found on Spotify", verbose=True)
                        except Exception as e:
                            log(f"    ✗ Spotify search error: {e}", verbose=True)
                            spotify_data = None
                            spotify_misses += 1
                            time.sleep(0.2)  # Rate limiting

                    # Always create record, even if Spotify search fails
                    record = {
                        "team": team,
                        "player": player,
                        "song_name": song["song_name"],
                        "song_artist": song["song_artist"],
                        "walkup_date": datetime.datetime.now(EST).date(),
                        "spotify_uri": spotify_data["uri"] if spotify_data else None,
                        "explicit": spotify_data["explicit"] if spotify_data else None,
                    }
                    records.append(record)
                    log(
                        f"    Added record: {song['song_name']} by {song['song_artist']}",
                        verbose=True,
                    )
                except Exception as e:
                    log(f"Error creating record for {player}: {e}")
                    continue

    if not records:
        log("No records to store. Exiting.")
        sys.exit(1)

    log(f"\nPrepared {len(records)} records for storage")
    log(f"Spotify matches: {spotify_hits} hits, {spotify_misses} misses")

    # Create DataFrame
    df = pd.DataFrame(records)
    log(f"Created DataFrame with {len(df)} rows")

    if VERBOSE_MODE:
        log("\nFirst few records:", verbose=True)
        log(str(df.head()), verbose=True)

    try:
        # Initialize database connection with retries
        max_retries = 3
        retry_delay = 5

        for attempt in range(max_retries):
            try:
                engine = get_db_connection(connection_uri)
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    log(
                        f"Database connection attempt {attempt + 1}/{max_retries} failed: {str(e)}"
                    )
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                else:
                    log("Failed to establish database connection after all retries")
                    raise

        # Store records with chunking for better performance
        log("Storing records in database...")
        store_records(df, engine)

        log(
            f"\nSuccessfully stored {len(df)} records\n"
            f"({df['spotify_uri'].notna().sum()} with Spotify URIs)"
        )

    except Exception as e:
        log(f"Database error: {str(e)}")
        log("Please check database connection settings and security groups")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 2:  # Only need Spotify credentials now
        sys.stderr.write(
            "Usage: python scraper.py "
            "<spotify_client_id> <spotify_client_secret> [--dry-run] [--verbose]\n"
        )
        sys.exit(1)

    # Check for flags
    dry_run = "--dry-run" in sys.argv
    if dry_run:
        sys.argv.remove("--dry-run")

    VERBOSE_MODE = "--verbose" in sys.argv
    if VERBOSE_MODE:
        sys.argv.remove("--verbose")

    scrape_and_store(
        connection_uri=None,  # No longer needed as we use environment variables
        spotify_client_id=sys.argv[1],
        spotify_client_secret=sys.argv[2],
        dry_run=dry_run,
    )

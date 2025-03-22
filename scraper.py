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

EST = timezone("US/Eastern")

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

        # Generate links for all teams
        for team_name in MLB_TEAMS.values():
            music_url = f"{mlb_site}/{team_name}/ballpark/music"
            team_links.append(music_url)
            sys.stdout.write(f"Found team link: {music_url}\n")

        if not team_links:
            sys.stdout.write("No team links found. Exiting.\n")
            return []

        sys.stdout.write(f"Found {len(team_links)} team links\n")
        return team_links
    except Exception as e:
        sys.stdout.write(f"Error getting team links: {str(e)}\n")
        return []


def scrape_and_store(
    connection_uri, spotify_client_id, spotify_client_secret, dry_run=False
):
    """Scrape MLB walk-up songs and store them in the database."""
    sys.stdout.write(
        f"Scraping MLB walk-up songs...\n"
        f"Spotify Client ID is None: {spotify_client_id is None}\n"
        f"Spotify Client Secret is None: {spotify_client_secret is None}\n"
        f"Dry run mode: {dry_run}\n"
    )

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
        sys.stdout.write("No team links found. Exiting.\n")
        sys.exit(1)

    team_songs = {}
    for team_link in team_links:
        team_name = team_link.split("/")[-3]
        sys.stdout.write(f"\nProcessing team: {team_name}\n")
        sys.stdout.write(f"URL: {team_link}\n")
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
                    sys.stdout.write(
                        f"Attempt {attempt + 1} failed: {str(e)}, retrying...\n"
                    )
                    time.sleep(2**attempt)  # Exponential backoff

            bsteam = BeautifulSoup(response.text, "html.parser")
            sys.stdout.write(f"Page content length: {len(response.text)}\n")

            # Initialize player songs dictionary
            player_songs = {}
            songs_found = False

            # Method 1: Try finding a dedicated music/walkup section
            music_section = bsteam.find(
                ["section", "div"], {"class": re.compile(r".*(music|walkup).*", re.I)}
            )
            if music_section:
                sys.stdout.write("- Found music/walkup section\n")
                # Look for player entries in various formats
                player_entries = music_section.find_all(
                    ["div", "article"],
                    {"class": re.compile(r".*(player|entry).*", re.I)},
                )

                if not player_entries:
                    # Try finding a table structure within the music section
                    player_entries = music_section.find_all("tr")

                for entry in player_entries:
                    try:
                        # Try to find player name in various formats
                        name_elem = entry.find(
                            ["h2", "h3", "h4", "td", "div"],
                            {"class": re.compile(r".*(name|player).*", re.I)},
                        )
                        if not name_elem:
                            name_elem = entry.find(["h2", "h3", "h4", "td", "div"])

                        if name_elem:
                            player_name = name_elem.text.strip()
                            if not player_name:
                                continue

                            if player_name not in player_songs:
                                player_songs[player_name] = []

                            # Try multiple methods to find song information
                            song_pattern = re.compile(r".*(song|music|content).*", re.I)
                            song_containers = entry.find_all(
                                ["div", "td", "p", "span"], {"class": song_pattern}
                            )

                            for container in song_containers:
                                content = container.text.strip()
                                if " by " in content.lower():
                                    song_name, artist = content.lower().split(" by ", 1)
                                    player_songs[player_name].append(
                                        {
                                            "song_name": song_name.strip().title(),
                                            "song_artist": artist.strip().title(),
                                        }
                                    )
                                    songs_found = True
                                else:
                                    # Try finding song in em/i tags
                                    song_tag = container.find(["em", "i"])
                                    if song_tag:
                                        song_name = song_tag.text.strip()
                                        # Artist might be after the song
                                        parts = container.text.split("by")
                                        if len(parts) > 1:
                                            artist = parts[1].strip()
                                            player_songs[player_name].append(
                                                {
                                                    "song_name": song_name.title(),
                                                    "song_artist": artist.title(),
                                                }
                                            )
                                            songs_found = True

                    except Exception as e:
                        sys.stdout.write(f"Error processing player entry: {str(e)}\n")
                        continue

            # Method 2: Try finding a forge list structure
            if not songs_found:
                forge_list = bsteam.find("div", {"class": "p-forge-list"})
                if forge_list:
                    sys.stdout.write("- Found forge list structure\n")
                    player_entries = forge_list.find_all(
                        "div", {"class": "p-featured-content__body"}
                    )

                    for entry in player_entries:
                        try:
                            player_name = entry.find(
                                "div", {"class": "u-text-h4"}
                            ).text.strip()

                            if player_name not in player_songs:
                                player_songs[player_name] = []

                            text_elem = entry.find(
                                "div", {"class": "p-featured-content__text"}
                            )
                            if text_elem:
                                p_tag = text_elem.find(["p", "span"])
                                if p_tag:
                                    # Try to find song information in spans
                                    spans = p_tag.find_all("span")
                                    for span in spans:
                                        content = span.text.strip()
                                        if " by " in content:
                                            song_name, artist = content.split(" by ", 1)
                                            player_songs[player_name].append(
                                                {
                                                    "song_name": song_name.strip(),
                                                    "song_artist": artist.strip(),
                                                }
                                            )
                                            songs_found = True

                                    if not songs_found:
                                        # Try finding song in em/i tags
                                        song_tag = p_tag.find(["em", "i"])
                                        if song_tag:
                                            song_name = song_tag.text.strip()
                                            parts = p_tag.text.split("by")
                                            if len(parts) > 1:
                                                artist = parts[1].strip()
                                                player_songs[player_name].append(
                                                    {
                                                        "song_name": song_name,
                                                        "song_artist": artist,
                                                    }
                                                )
                                                songs_found = True

                        except Exception as e:
                            sys.stdout.write(
                                f"Error processing forge list entry: {str(e)}\n"
                            )
                            continue

            if player_songs:
                team_songs[team_name] = player_songs
                sys.stdout.write(f"Found {len(player_songs)} players for {team_name}\n")
            else:
                sys.stdout.write(f"No songs found for {team_name}\n")

        except Exception as e:
            sys.stdout.write(f"Error scraping {team_name}: {str(e)}\n")
            continue

    if not team_songs:
        sys.stdout.write("No songs found for any team. Exiting.\n")
        sys.exit(1)

    if dry_run:
        sys.stdout.write("Dry run mode - skipping database operations\n")
        return

    # Initialize Spotify client
    spotify_search = None
    if spotify_client_id and spotify_client_secret:
        spotify_search = spotipy.Spotify(
            client_credentials_manager=SpotifyClientCredentials(
                client_id=spotify_client_id, client_secret=spotify_client_secret
            )
        )

    # Search for songs on Spotify
    if spotify_search:
        for team, players in team_songs.items():
            for player, songs in players.items():
                for i, song in enumerate(songs):
                    song_name = song["song_name"]
                    song_artist = song["song_artist"]

                    if song_name and song_artist:
                        try:
                            results = spotify_search.search(
                                q=f"track:{song_name} artist:{song_artist}",
                                type="track",
                                limit=1,
                            )
                            if results["tracks"]["items"]:
                                team_songs[team][player][i]["spotify_id"] = results[
                                    "tracks"
                                ]["items"][0]
                            else:
                                team_songs[team][player][i]["spotify_id"] = None
                        except Exception as e:
                            sys.stdout.write(f"Spotify search error: {str(e)}\n")
                            team_songs[team][player][i]["spotify_id"] = None
                    else:
                        team_songs[team][player][i]["spotify_id"] = None
                    time.sleep(0.2)  # Rate limiting

    # Prepare records for database
    records = []
    for team, players in team_songs.items():
        for player, songs in players.items():
            for song in songs:
                record = {
                    "team": team,
                    "player": player,
                    "song_name": song["song_name"],
                    "song_artist": song["song_artist"],
                    "walkup_date": datetime.datetime.now(EST).date(),
                    "spotify_uri": (
                        song["spotify_id"]["uri"] if song["spotify_id"] else None
                    ),
                    "explicit": (
                        song["spotify_id"]["explicit"] if song["spotify_id"] else None
                    ),
                }
                records.append(record)

    if not records:
        sys.stdout.write("No records to store. Exiting.\n")
        sys.exit(1)

    # Create DataFrame
    df = pd.DataFrame(records)
    sys.stdout.write(f"Attempting to store {len(records)} records...\n")

    try:
        # Configure database engine with longer timeouts and retry logic
        engine = create_engine(
            connection_uri.replace("postgresql://", "postgresql+psycopg2://"),
            connect_args={
                "connect_timeout": 60,
                "keepalives": 1,
                "keepalives_idle": 30,
                "keepalives_interval": 10,
                "keepalives_count": 5,
            },
            pool_pre_ping=True,
            pool_recycle=3600,
            pool_timeout=30,
        )

        # Store records with chunking for better performance
        df.to_sql(
            "mlb_walk_up_songs",
            engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=100,
        )

        sys.stdout.write(
            f"Successfully stored {len(df)} records\n"
            f"({df['spotify_uri'].notna().sum()} with Spotify URIs)\n"
        )

    except Exception as e:
        sys.stdout.write(f"Database error: {str(e)}\n")
        sys.stdout.write(
            "Please check database connection settings and security groups\n"
        )
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 4:
        sys.stderr.write(
            "Usage: python scraper.py <connection_uri> "
            "<spotify_client_id> <spotify_client_secret> [--dry-run]\n"
        )
        sys.exit(1)

    # Check for dry-run flag
    dry_run = "--dry-run" in sys.argv
    if dry_run:
        sys.argv.remove("--dry-run")

    scrape_and_store(
        connection_uri=sys.argv[1],
        spotify_client_id=sys.argv[2],
        spotify_client_secret=sys.argv[3],
        dry_run=dry_run,
    )

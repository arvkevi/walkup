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


def get_team_links():
    """Get all MLB team links."""
    try:
        mlb_site = "https://www.mlb.com"
        response = requests.get(f"{mlb_site}/team", timeout=30)
        response.raise_for_status()
        bs = BeautifulSoup(response.text, "html.parser")

        team_links = []
        for link in bs.find_all("a", href=re.compile(r"/[a-z-]+/roster")):
            team_path = link["href"].split("/roster")[0]
            if team_path and not team_path.endswith(("milb", "teams")):
                team_links.append(f"{mlb_site}{team_path}/ballpark/music")

        return team_links
    except Exception as e:
        sys.stdout.write(f"Error getting team links: {str(e)}\n")
        return []


def scrape_and_store(connection_uri, spotify_client_id, spotify_client_secret):
    """Scrape MLB walk-up songs and store them in the database."""
    sys.stdout.write(
        f"Scraping MLB walk-up songs...\n"
        f"Spotify Client ID is None: {spotify_client_id is None}\n"
        f"Spotify Client Secret is None: {spotify_client_secret is None}\n"
    )

    team_links = get_team_links()
    if not team_links:
        sys.stdout.write("No team links found. Exiting.\n")
        sys.exit(1)

    team_songs = {}
    for team_link in team_links:
        team_name = team_link.split("/")[-3]
        try:
            response = requests.get(team_link, timeout=30)
            response.raise_for_status()
            bsteam = BeautifulSoup(response.text, "html.parser")

            # Try the first method (new website structure)
            player_songs = {}
            music_section = bsteam.find(
                "section", {"class": re.compile(r".*music.*", re.I)}
            )
            if music_section:
                for player_div in music_section.find_all(
                    "div", {"class": re.compile(r".*player.*", re.I)}
                ):
                    try:
                        name_elem = player_div.find(
                            ["h2", "h3", "h4"], {"class": re.compile(r".*name.*", re.I)}
                        )
                        if not name_elem:
                            continue

                        player_name = name_elem.text.strip()
                        player_songs[player_name] = []

                        song_elems = player_div.find_all(
                            ["p", "div"], {"class": re.compile(r".*song.*", re.I)}
                        )
                        for song_elem in song_elems:
                            song_text = song_elem.text.strip()
                            if " by " in song_text:
                                song_name, artist = song_text.split(" by ", 1)
                                player_songs[player_name].append(
                                    {
                                        "song_name": song_name.strip(),
                                        "song_artist": artist.strip(),
                                    }
                                )
                    except Exception as e:
                        sys.stdout.write(
                            f"Error processing player in {team_name}: {str(e)}\n"
                        )
                        continue

            # If no songs found, try the second method (table structure)
            if not any(songs for songs in player_songs.values()):
                table = bsteam.find("table", {"class": re.compile(r".*walkup.*", re.I)})
                if table:
                    for row in table.find_all("tr")[1:]:  # Skip header
                        try:
                            cols = row.find_all("td")
                            if len(cols) >= 3:
                                player_name = cols[0].text.strip()
                                song_name = cols[1].text.strip()
                                artist = cols[2].text.strip()

                                if player_name and song_name:
                                    if player_name not in player_songs:
                                        player_songs[player_name] = []
                                    player_songs[player_name].append(
                                        {"song_name": song_name, "song_artist": artist}
                                    )
                        except Exception as e:
                            sys.stdout.write(
                                f"Error processing table row in {team_name}: {str(e)}\n"
                            )
                            continue

            if player_songs:
                team_songs[team_name] = player_songs
                sys.stdout.write(
                    f"Successfully scraped {len(player_songs)} players from {team_name}\n"
                )
            else:
                sys.stdout.write(f"No songs found for {team_name}\n")

        except Exception as e:
            sys.stdout.write(f"Error scraping {team_name}: {str(e)}\n")

    if not team_songs:
        sys.stdout.write("No songs found for any team. Exiting.\n")
        sys.exit(1)

    # Initialize Spotify client
    spotify_search = spotipy.Spotify(
        client_credentials_manager=SpotifyClientCredentials(
            client_id=spotify_client_id, client_secret=spotify_client_secret
        )
    )

    # Search for songs on Spotify
    for team in team_songs:
        for player in team_songs[team]:
            for i, song in enumerate(team_songs[team][player].copy()):
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

    # Create DataFrame and store in database
    df = pd.DataFrame(records)
    sys.stdout.write(f"Attempting to store {len(records)} records...\n")

    try:
        engine = create_engine(
            connection_uri.replace("postgresql://", "postgresql+psycopg2://"),
            connect_args={
                "connect_timeout": 60,  # Increased timeout
                "keepalives": 1,
                "keepalives_idle": 30,
                "keepalives_interval": 10,
                "keepalives_count": 5,
            },
        )

        # Test connection first
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            sys.stdout.write("Database connection test successful\n")

        df.to_sql(
            "mlb_walk_up_songs",
            engine,
            if_exists="append",
            index=False,
            method="multi",  # Use multi-row inserts
            chunksize=100,  # Insert in chunks
        )
        sys.stdout.write(
            f"Successfully stored {len(records)} records\n"
            f"({df.loc[df['spotify_uri'].notnull()].shape[0]} with Spotify URIs)\n"
        )
    except Exception as e:
        sys.stdout.write(f"Database error: {str(e)}\n")
        sys.stdout.write(
            "Connection URI format should be: postgresql://user:pass@host:5432/dbname\n"
        )
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        sys.stderr.write(
            "Usage: python scraper.py <connection_uri> "
            "<spotify_client_id> <spotify_client_secret>\n"
        )
        sys.exit(1)

    scrape_and_store(
        connection_uri=sys.argv[1],
        spotify_client_id=sys.argv[2],
        spotify_client_secret=sys.argv[3],
    )

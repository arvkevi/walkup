import datetime

from bs4 import BeautifulSoup
import requests
import pandas as pd
import psycopg2

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

from sqlalchemy import create_engine

import time
import sys
import re

if __name__ == "__main__":
    
    CONNECTION_URI = sys.argv[1]
    SPOTIFY_CLIENT_ID = sys.argv[2]
    SPOTIFY_CLIENT_SECRET = sys.argv[3]

    sys.stdout.write(f"Scraping MLB walk-up songs...\n"
                     f"Connection URI: {CONNECTION_URI}\n"
                     f"Spotify Client ID is None: {SPOTIFY_CLIENT_ID is None}\n"
                     f"Spotify Client Secret is None: {SPOTIFY_CLIENT_SECRET is None}\n")

    mlb_site = "https://mlb.com"
    music_endpoint = "ballpark/music"

    bs = BeautifulSoup(requests.get(f"{mlb_site}/fans").text, "html.parser")

    team_links = []
    links = bs.find_all("a", {"data-parent": "Teams"}, href=True)
    for link in links:
        team_links.append(f"{mlb_site}{link['href']}/{music_endpoint}")

    team_songs = {}
    for team_link in team_links:
        team_name = team_link.split("/")[-3]
        bsteam = BeautifulSoup(requests.get(team_link, timeout=60).text, "html.parser")

        try:
            players = bsteam.find("div", {"class": "p-forge-list"}).findAll(
                "div", {"class": "p-featured-content__body"}
            )
            player_songs = {}
            for player in players:
                player_name = player.find("div", {"class": "u-text-h4"}).text.strip()
                player_songs[player_name] = []
                p_tag = player.find("div", {"class": "p-featured-content__text"}).find(
                    ["p", "span"]
                )
                spans = p_tag.find_all('span')

                songs = set()
                # Extract song names and artists
                for span in spans:
                    text = span.get_text().strip()
                    if ' by ' in text:
                        song, artist = text.split(' by ', 1)
                        songs.add((song.strip(), artist.strip()))

                if not songs:
                    for a_tag in p_tag.find_all('a'):
                        try:
                            song_name = a_tag.em.get_text().strip()
                            artist_name = a_tag.next_sibling.strip(' by ')
                            songs.add((song_name, artist_name))
                        except:
                            # use the final method
                            pass
                
                if songs:
                    # Displaying the results
                    for song, artist in songs:
                        player_songs[player_name].append(
                            {
                                "song_name": song,
                                "song_artist": artist
                            }
                        )
                
                if not songs:
                    # Additional code to get song name and artist name
                    p_text_only = ""

                    # Loop through the elements inside the <p> tag
                    for content in p_tag.contents:
                        if content.name is None:  # Text, not a tag
                            p_text_only += content

                    # Remove leading and trailing whitespace
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
                        {
                            "song_name": song_name,
                            "song_artist": song_artist
                        }
                    )

            team_songs[team_name] = player_songs

        except Exception as e:
            print(f"{team_name}: trying another method...")

        if team_name not in team_songs:
            try:
                song_table = bsteam.find("div", {"data-testid": "player-walkup-music"})

                table = song_table.find("table")
                for i, rows in enumerate(table):
                    # table header
                    if i == 0:
                        continue

                # Find all player entries
                player_entries = rows.find_all("tr", {"data-selected": "false", "data-underlined": "false"})

                # Initialize a dictionary to hold player names and their unique songs
                player_songs = {}

                for entry in player_entries:
                    # Extract the player name
                    player_first_name = entry.find("div", {"data-testid": re.compile(r"spot-tag__super-name")})
                    player_last_name = entry.find("div", {"data-testid": re.compile(r"spot-tag__name")})
                    player_first_name = " ".join(tag.get_text() for tag in player_first_name)
                    player_last_name = " ".join(tag.get_text() for tag in player_last_name)
                    player_name = f"{player_first_name} {player_last_name}"

                    # Find all songs for this player
                    player_songs[player_name] = []
                    songs = entry.find_all("div", {"data-testid": re.compile(r"player-walkup-music-song-content-\d+")})
                    for song in songs:
                        song_name = song.find("div", {"class": "player-walkup-music__song--content--songname"}).get_text()
                        artist_name = song.find("div", {"class": "player-walkup-music__song--content--artistname"}).get_text()
                        player_songs[player_name].append({"song_name": song_name, "artist_name": artist_name})

                team_songs[team_name] = player_songs

            except Exception as e:
                print(f"{team_name}: Error, skipping...")


    spotify_search = spotipy.Spotify(
        client_credentials_manager=SpotifyClientCredentials(
            client_id=SPOTIFY_CLIENT_ID, client_secret=SPOTIFY_CLIENT_SECRET
        )
    )

    for team in team_songs:
        for player in team_songs[team]:
            for i, song in enumerate(team_songs[team][player].copy()):
                song_name = song["song_name"]
                song_artist = song["song_artist"]

                if song_name and song_artist:
                    results = spotify_search.search(
                        q=f"track:{song_name} artist:{song_artist}", type="track", limit=1
                    )
                    if results["tracks"]["items"]:
                        team_songs[team][player][i]["spotify_id"] = results["tracks"]["items"][0]
                    else:
                        team_songs[team][player][i]["spotify_id"] = None
                else:
                    team_songs[team][player][i]["spotify_id"] = None
                time.sleep(0.2)
    
    # List to store each record
    records = []

    # Iterate over the dictionary to create records
    for team, players in team_songs.items():
        for player, songs in players.items():
            for song in songs:
                # Create a record for each song
                record = {
                    'team': team,
                    'player': player,
                    'song_name': song['song_name'],
                    'song_artist': song['song_artist'],
                    'walkup_date': datetime.date.today(),
                    'spotify_uri': song['spotify_id']['uri'] if song['spotify_id'] else None,
                    'explicit': song['spotify_id']['explicit'] if song['spotify_id'] else None
                }
                records.append(record)

    df = pd.DataFrame(records)

    engine = create_engine(CONNECTION_URI.replace("postgresql", "postgresql+psycopg2"))
    df.to_sql('mlb_walk_up_songs', engine, if_exists='append', index=False)

    sys.stdout.write(f"Successfully scraped {df.loc[df['spotify_uri'].notnull()].shape[0]} of {df.shape[0]} MLB walk-up songs.")

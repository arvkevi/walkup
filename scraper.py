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

                p_tag = player.find("div", {"class": "p-featured-content__text"}).find(
                    ["p", "span"]
                )

                # Initialize an empty string to hold the text from the <p> tag
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

                player_songs[player_name] = {
                    "song_name": song_name,
                    "song_artist": song_artist,
                }

            team_songs[team_name] = player_songs
        except Exception as e:
            print(f"Error with {team_name}: {e}")

    spotify_search = spotipy.Spotify(
        client_credentials_manager=SpotifyClientCredentials(
            client_id=SPOTIFY_CLIENT_ID, client_secret=SPOTIFY_CLIENT_SECRET
        )
    )

    for team in team_songs:
        for player in team_songs[team]:
            song_name = team_songs[team][player]["song_name"].strip().replace("\n", " ")
            song_artist = (
                team_songs[team][player]["song_artist"].strip().replace("\n", " ")
            )
            if song_name and song_artist:
                results = spotify_search.search(
                    q=f"track:{song_name} artist:{song_artist}", type="track", limit=1
                )
                if results["tracks"]["items"]:
                    team_songs[team][player]["spotify_id"] = results["tracks"]["items"][
                        0
                    ]
                else:
                    team_songs[team][player]["spotify_id"] = None
            else:
                team_songs[team][player]["spotify_id"] = None
            time.sleep(0.2)

    df = pd.DataFrame()
    for team in team_songs:
        dfteam = pd.DataFrame(team_songs[team]).T.reset_index()
        dfteam["team"] = team
        dfteam.rename(columns={"index": "player"}, inplace=True)
        df = pd.concat([df, dfteam], ignore_index=True, axis=0)

    df["walkup_date"] = datetime.date.today()
    df["spotify_uri"] = df["spotify_id"].apply(lambda x: x["uri"] if x else None)
    df["explicit"] = df["spotify_id"].apply(lambda x: x["explicit"] if x else None)
    df.drop(columns=["spotify_id"], inplace=True)
    engine = create_engine(CONNECTION_URI.replace("postgresql", "postgresql+psycopg2"))
    df.to_sql('mlb_walk_up_songs', engine, if_exists='append', index=False)

    sys.stdout.write(f"Successfully scraped {df.loc[df['spotify_uri'].notnull()].shape[0]} of {df.shape[0]} MLB walk-up songs.")

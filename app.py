import datetime
import os

import pandas as pd
import psycopg2
import streamlit as st
from streamlit.components.v1 import html
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from oauth import st_oauth, _STKEY

CONNECTION_URI = os.environ.get("CONNECTION_URI")
SPOTIFY_CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET")
REDIRECT_URI = os.environ.get("REDIRECT_URI")

st.set_page_config(
    page_title="MLB Walkup Songs to Spotify Playlist",
    page_icon="baseball",
    layout="wide",
)


@st.cache_data()
def get_mlb_walkup_data(
    connection_uri: str,
    walkup_date: datetime.date = datetime.date.today(),
):
    """
    Query the MLB walkup songs on a given date
    """
    try:
        conn = psycopg2.connect(connection_uri)
    except psycopg2.OperationalError as conn_error:
        st.error(f"Unable to connect!\n{conn_error}")

    walkup_date = walkup_date.strftime("%Y-%m-%d")

    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM mlb_walk_up_songs
        WHERE walkup_date BETWEEN %s AND %s
        """,
        (walkup_date, walkup_date),
    )
    walkup_song_data = cur.fetchall()
    conn.close()
    column_names = [
        "player",
        "song_name",
        "song_artist",
        "team",
        "walkup_date",
        "spotify_uri",
        "explicit",
    ]
    walkup_data = pd.DataFrame(walkup_song_data, columns=column_names)
    walkup_data["team"] = walkup_data["team"].str.capitalize()
    return walkup_data


# Title and gif
config = {
    "authorization_endpoint": "https://accounts.spotify.com/authorize",
    "token_endpoint": "https://accounts.spotify.com/api/token",
    "redirect_uri": REDIRECT_URI,
    "client_id": SPOTIFY_CLIENT_ID,
    "client_secret": SPOTIFY_CLIENT_SECRET,
    "scope": "playlist-modify-private",
}

st.title("Create a Spotify playlist from MLB walkup songs")
but, gif = st.columns([0.5, 0.5])
gif.markdown(
    """
    <iframe src="https://giphy.com/embed/XTGstcX6TBdlFk7Cd8" width="320" height="180" frameBorder="0" class="giphy-embed" allowFullScreen></iframe><p><a href="https://giphy.com/gifs/mlb-sports-baseball-XTGstcX6TBdlFk7Cd8"></a></p>
    """,
    unsafe_allow_html=True,
)

# Date picker and metrics
col1, col2, col3, col4, col5 = st.columns([0.2] * 5, gap="large")
date = col1.date_input(
    "Choose a date :calendar: : ",
    value=datetime.date.today(),
    min_value=datetime.date(2023, 9, 23),
)

data = get_mlb_walkup_data(CONNECTION_URI, date)
data["spotify_uri"] = data.apply(lambda row: row["spotify_uri"].replace("spotify:track:", "https://open.spotify.com/track/") if row["spotify_uri"] else None, axis=1)
n_spotify = data["spotify_uri"].notnull().sum()
data["Selected"] = [False] * data.shape[0]
data = data[["Selected", "team", "player", "song_name", "song_artist", "explicit", "spotify_uri"]]
data.sort_values(by=["team", "player"], inplace=True)

col2.metric("Songs", data["song_name"].nunique())
col3.metric("Songs in Spotify", n_spotify)
col4.metric("Players", data["player"].nunique())
col5.metric("Teams", data["team"].nunique())
filter_explicit = col4.checkbox("No explicit songs?", value=False)
filter_in_spotify = col5.checkbox("Only songs in Spotify?", value=False)

if data.empty:
    st.warning("No walkup songs found for this date.")
else:
    disabled_columns = ["team", "player", "song_name", "song_artist", "explicit", "spotify_uri"]
    column_config = {
        "Selected": st.column_config.CheckboxColumn(
            "Selected?",
            help="Select songs for the Playlist",
            default=False,
        ),
        "spotify_uri": st.column_config.LinkColumn(
            "Spotify Link (click link to follow)",
            help="Link to Spotify song",
            disabled=True,
        )
    }
    if filter_explicit:
        data = data[data["explicit"] == False]
    if filter_in_spotify:
        data = data[data["spotify_uri"].notnull()]
    edited_df = st.data_editor(data=data, column_config=column_config, hide_index=True, use_container_width=True, num_rows="fixed", disabled=disabled_columns)
    # Extracting selected rows
    selected_rows_df = edited_df[edited_df['Selected']]
try:
    but.image('https://storage.googleapis.com/pr-newsroom-wp/1/2018/11/Spotify_Logo_RGB_Green.png', width=200)
    st_oauth(config=config, label='Start by Logging into Spotify', but=but)
    spotify = spotipy.Spotify(st.session_state[_STKEY]["access_token"])
    selected = "Check songs with the 'Selected?' column."
    search_bar = "Hover over top right of the table for search."
    but.write(
        f"""
        Authenticated successfully as **{spotify.me()['display_name']}**.\n
        **{selected}**\n
        **{search_bar}**
        """
    )
except:
    spotify = None
    st.error("Unable to authenticate with Spotify.")

with st.form("playlist-form", clear_on_submit=False):
    st.subheader("Create Spotify playlist from selected songs")
    col, buff, buff2 = st.columns([0.2, 0.6, 0.2])
    playlist_name = col.text_input("Playlist Name", value=f"MLB Walkup Songs {date}", max_chars=25)

    if not selected_rows_df.empty:
        selected_rows_df = selected_rows_df.drop(columns=["Selected"])
        st.dataframe(
            selected_rows_df.rename(
                columns={
                    "team": "Team",
                    "player": "Player Name",
                    "song_name": "Song Name",
                    "song_artist": "Song Artist",
                    "explicit": "Explicit",
                }
            ),
            hide_index=True,
        )
    if spotify:
        submit = st.form_submit_button("Create Playlist", type="primary")
        if submit:
            if selected_rows_df.empty:
                st.warning("No songs selected.")
            else:
                try:
                    mlb_walkup_playlist = spotify.user_playlist_create(
                        user=spotify.current_user()["id"],
                        name=playlist_name,
                        public=False,
                        collaborative=False,
                        description="MLB Walkup Songs",
                    )
                except Exception as e:
                    st.error(f"Error creating playlist: {e}")

                if mlb_walkup_playlist:
                    st.success("Successfully created playlist!")
                    try:
                        spotify.user_playlist_add_tracks(
                            user=spotify.current_user()["id"],
                            playlist_id=mlb_walkup_playlist["id"],
                            tracks=selected_rows_df["spotify_uri"].dropna().tolist(),
                        )
                    except Exception as e:
                        st.error(f"Error adding tracks to playlist: {e}")

                    st.success("Successfully added tracks to playlist!")
                    st.write(
                        f"Check out your playlist [here]({mlb_walkup_playlist['external_urls']['spotify']})."
                    )
                else:
                    st.error("Unable to authenticate with Spotify.")

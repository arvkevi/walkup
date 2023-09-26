import datetime
import os
import webbrowser

import pandas as pd
import psycopg2
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder

import spotipy
from spotipy.oauth2 import SpotifyOAuth


CONNECTION_URI = os.environ.get("CONNECTION_URI")
SPOTIFY_CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET")

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
        st.error("Unable to connect!\n{0}").format(conn_error)

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
    return pd.DataFrame(walkup_song_data, columns=column_names)


# Title and gif
title_, gif = st.columns([0.60, 0.40])
title_.title("Create a Spotify playlist from MLB walkup songs")
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
n_spotify = data["spotify_uri"].notnull().sum()
data = data[["team", "player", "song_name", "song_artist", "explicit", "spotify_uri"]]
data.sort_values(by=["team", "player"], inplace=True)

col2.metric("Songs", data["song_name"].nunique())
col3.metric("Songs in Spotify", n_spotify)
col4.metric("Players", data["player"].nunique())
col5.metric("Teams", data["team"].nunique())

selected_rows = None

if data.empty:
    st.warning("No walkup songs found for this date.")
else:
    gb = GridOptionsBuilder.from_dataframe(data)
    gb.configure_selection(
        "multiple",
        use_checkbox=True,
        header_checkbox=True,
        header_checkbox_filtered_only=True,
        rowMultiSelectWithClick=True,
    )
    gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=10)
    gb.configure_column(field="spotify_uri", hide=True)
    gb.configure_column(field="team", header_name="Team")
    gb.configure_column(field="player", header_name="Player Name")
    gb.configure_column(field="song_name", header_name="Song Name")
    gb.configure_column(field="song_artist", header_name="Song Artist")
    gb.configure_column(
        field="explicit",
        cell_renderer="agCheckboxCellRenderer",
        cell_renderer_params={
            "suppressTrueText": True,
            "suppressFalseText": True,
            "disabled": True,
        },
        cell_style={"textAlign": "center"},
    )
    gb.configure_grid_options(
        **{
            "statusBar": {
                "statusPanels": [
                    {"statusPanel": "agTotalRowCountComponent", "align": "left"},
                    {"statusPanel": "agSelectedRowCountComponent", "align": "center"},
                ]
            },
        }
    )
    go = gb.build()

    grid = AgGrid(
        data,
        gridOptions=go,
        fit_columns_on_grid_load=True,
        theme="material",
        key="grid",
    )
    selected_rows = grid["selected_rows"]
    selected_rows_df = pd.DataFrame(selected_rows)

# if st.button("Create Spotify Playlist from Selected Songs"):
with st.form("playlist-form", clear_on_submit=False):
    st.subheader("Create Spotify playlist from selected songs")
    col, buff, buff2 = st.columns([0.2, 0.6, 0.2])
    playlist_name = col.text_input("Playlist Name", value=f"MLB Walkup Songs {date}")

    if selected_rows:
        selected_rows_df.drop(columns=["_selectedRowNodeInfo"], inplace=True)
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

    submit = st.form_submit_button("Create Playlist")
    if submit:
        # Check if the user is redirected back to the app after login
        params = st.experimental_get_query_params()
        code = params.get("code")
        spotify = None
        if code:
            code = code[0]
            sp_oauth = SpotifyOAuth(client_id=SPOTIFY_CLIENT_ID, client_secret=SPOTIFY_CLIENT_SECRET, redirect_uri="https://walkup.streamlit.app")
            token_info = sp_oauth.get_access_token(code=code)
            spotify = spotipy.Spotify(auth=token_info["access_token"])
            st.write(f"Authenticated successfully as {spotify.me()['display_name']}")
        else:
            # User is not authenticated yet. Show the authentication link.
            sp_oauth = SpotifyOAuth(client_id=SPOTIFY_CLIENT_ID, client_secret=SPOTIFY_CLIENT_SECRET, redirect_uri="https://walkup.streamlit.app")
            auth_url = sp_oauth.get_authorize_url()
            webbrowser.open_new_tab(auth_url)

        if spotify:
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

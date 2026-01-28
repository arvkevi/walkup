import datetime
import os

import pandas as pd
import streamlit as st
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Supabase connection
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")  # Use anon key for public read access

# Spotify credentials
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI", "http://localhost:8501/callback")

# Page config
st.set_page_config(
    page_title="MLB Walkup Songs",
    page_icon="⚾",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Custom CSS for modern look
st.markdown("""
<style>
    /* Hide default Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}

    /* Overall page styling */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1400px;
    }

    /* Metric cards - minimal style */
    div[data-testid="stMetric"] {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 10px;
        border: 1px solid #e9ecef;
    }

    div[data-testid="stMetric"] label {
        color: #6c757d !important;
        font-weight: 500;
        font-size: 0.85rem;
    }

    div[data-testid="stMetricValue"] {
        font-size: 1.8rem;
        color: #212529 !important;
        font-weight: 600;
    }

    /* Data table styling */
    .stDataFrame {
        border-radius: 12px;
        overflow: hidden;
    }

    div[data-testid="stDataFrame"] > div {
        border-radius: 12px;
        border: 1px solid #e0e0e0;
    }

    /* Section headers */
    .stSubheader {
        color: #1a1a2e;
        font-weight: 600;
    }

    /* Form styling */
    div[data-testid="stForm"] {
        background-color: #f8f9fa;
        padding: 1.5rem;
        border-radius: 12px;
        border: 1px solid #e9ecef;
    }

    /* Success/info messages */
    div[data-testid="stAlert"] {
        border-radius: 8px;
    }

    /* Checkbox and select styling */
    .stCheckbox, .stSelectbox {
        padding: 0.5rem 0;
    }

    /* Dividers */
    hr {
        margin: 1.5rem 0;
        border-color: #e9ecef;
    }

    /* Filter section background */
    .filter-section {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 12px;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_supabase_client() -> Client:
    """Initialize Supabase client."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        st.error("Missing Supabase credentials. Set SUPABASE_URL and SUPABASE_KEY.")
        st.stop()
    return create_client(SUPABASE_URL, SUPABASE_KEY)


@st.cache_data(ttl=300)
def get_walkup_songs():
    """Get all walkup songs from Supabase (including historical)."""
    supabase = get_supabase_client()
    response = supabase.table("mlb_walk_up_songs").select("*").execute()

    if not response.data:
        return pd.DataFrame()

    df = pd.DataFrame(response.data)
    df["team"] = df["team"].str.title()
    # Convert date columns
    df["first_seen_date"] = pd.to_datetime(df["first_seen_date"]).dt.date
    df["last_updated_date"] = pd.to_datetime(df["last_updated_date"]).dt.date
    return df


def get_stats(df):
    """Get summary statistics from dataframe."""
    if df.empty:
        return {"songs": 0, "players": 0, "teams": 0, "spotify_songs": 0}

    return {
        "songs": df["song_name"].nunique(),
        "players": df["player"].nunique(),
        "teams": df["team"].nunique(),
        "spotify_songs": df["spotify_uri"].notna().sum(),
    }


def get_spotify_client():
    """Get authenticated Spotify client using Streamlit's native OAuth."""
    if "spotify_token" not in st.session_state:
        return None

    return spotipy.Spotify(auth=st.session_state["spotify_token"])


def spotify_login():
    """Handle Spotify OAuth flow."""
    sp_oauth = SpotifyOAuth(
        client_id=SPOTIFY_CLIENT_ID,
        client_secret=SPOTIFY_CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        scope="playlist-modify-private",
        open_browser=False,
        cache_handler=None,  # Disable file caching for Streamlit
    )

    # Check for callback code in URL
    if "code" in st.query_params:
        code = st.query_params["code"]
        try:
            token_info = sp_oauth.get_access_token(code, as_dict=True)
            st.session_state["spotify_token"] = token_info["access_token"]
            st.session_state["just_logged_in"] = True  # Flag for showing celebration
            st.query_params.clear()
            st.rerun()
        except Exception as e:
            st.error(f"Authentication failed: {e}")
            return None

    return sp_oauth


# Header with custom styling
st.markdown("""
<h1 style="
    background: linear-gradient(90deg, #1DB954 0%, #191414 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    font-size: 2.8rem;
    font-weight: 800;
    margin-bottom: 0;
">⚾ MLB Walkup Songs</h1>
""", unsafe_allow_html=True)
st.markdown("""
<p style="color: #666; font-size: 1.1rem; margin-top: 0.5rem;">
    Create Spotify playlists from MLB player walkup songs
</p>
""", unsafe_allow_html=True)

# Load data early for stats
all_data_for_stats = get_walkup_songs()
current_data = all_data_for_stats[all_data_for_stats["is_current"]] if not all_data_for_stats.empty else all_data_for_stats

# Stats row
stats = get_stats(current_data)
col1, col2, col3, col4 = st.columns(4)
col1.metric("🎵 Unique Songs", stats["songs"])
col2.metric("👤 Players", stats["players"])
col3.metric("🏟️ Teams", stats["teams"])
col4.metric("🎧 On Spotify", stats["spotify_songs"])

st.divider()

# Spotify authentication
spotify = get_spotify_client()

if not spotify:
    sp_oauth = spotify_login()
    if sp_oauth:
        auth_url = sp_oauth.get_authorize_url()
        # Spotify login card
        st.markdown(f'''
        <div style="
            background: linear-gradient(135deg, #1DB954 0%, #169c46 100%);
            padding: 2rem;
            border-radius: 16px;
            text-align: center;
            box-shadow: 0 8px 32px rgba(29, 185, 84, 0.3);
        ">
            <h3 style="color: white; margin-bottom: 0.5rem; font-size: 1.5rem;">
                🎧 Connect to Spotify
            </h3>
            <p style="color: rgba(255,255,255,0.9); margin-bottom: 1.5rem;">
                Login to create playlists from walkup songs
            </p>
            <a href="{auth_url}" target="_self" style="text-decoration: none;">
                <button style="
                    background-color: white;
                    color: #1DB954;
                    padding: 14px 48px;
                    font-size: 16px;
                    font-weight: bold;
                    border: none;
                    border-radius: 50px;
                    cursor: pointer;
                    transition: transform 0.2s;
                ">
                    Login with Spotify
                </button>
            </a>
        </div>
        ''', unsafe_allow_html=True)
else:
    try:
        user = spotify.me()
        display_name = user.get("display_name", "Unknown") if user else "Unknown"

        # Show celebration GIF if just logged in
        if st.session_state.get("just_logged_in"):
            st.balloons()
            import base64
            with open("celebration.gif", "rb") as f:
                gif_data = base64.b64encode(f.read()).decode()
            st.markdown(f"""
            <div style="display: flex; justify-content: center; margin-bottom: 1rem;">
                <img src="data:image/gif;base64,{gif_data}"
                     alt="Baseball celebration"
                     style="max-height: 200px; border-radius: 12px;">
            </div>
            """, unsafe_allow_html=True)
            del st.session_state["just_logged_in"]

        # Connected user card
        st.markdown(f'''
        <div style="
            background: linear-gradient(135deg, #1DB954 0%, #169c46 100%);
            padding: 1rem 1.5rem;
            border-radius: 12px;
            display: flex;
            align-items: center;
            justify-content: space-between;
        ">
            <span style="color: white; font-weight: 500;">
                ✓ Connected as <strong>{display_name}</strong>
            </span>
        </div>
        ''', unsafe_allow_html=True)
        if st.button("Logout", type="secondary"):
            del st.session_state["spotify_token"]
            st.rerun()
    except Exception:
        del st.session_state["spotify_token"]
        st.rerun()

st.divider()

# Initialize playlist in session state
if "playlist_songs" not in st.session_state:
    st.session_state.playlist_songs = []

# Main content - reuse data loaded for stats
all_data = all_data_for_stats

if all_data.empty:
    st.warning("No walkup songs found. Run the scraper to populate the database.")
    st.stop()

# Filters in a styled container
st.subheader("🔍 Filter Songs")

# Date range filter - use most recent data date to avoid timezone issues
# This ensures users always see data even if server is in a different timezone
most_recent_date = all_data["last_updated_date"].max() if not all_data.empty else datetime.date.today()
date_col1, date_col2 = st.columns(2)
with date_col1:
    start_date = st.date_input(
        "Active from",
        value=most_recent_date,
        help="Show songs active on or after this date"
    )
with date_col2:
    end_date = st.date_input(
        "Active until",
        value=most_recent_date,
        help="Show songs active on or before this date"
    )

with st.container():
    filter_col1, filter_col2, filter_col3 = st.columns([2, 1, 1])

    with filter_col1:
        teams = ["All Teams"] + sorted(all_data["team"].unique().tolist())
        selected_team = st.selectbox("Team", teams, label_visibility="collapsed",
                                      help="Filter by MLB team")

    with filter_col2:
        filter_explicit = st.checkbox("Hide explicit", help="Filter out explicit songs")

    with filter_col3:
        filter_spotify_only = st.checkbox("Spotify only", help="Only show songs available on Spotify")

# Apply filters
filtered_data = all_data.copy()

# Date filter: song was active during the date range
# A song is active if first_seen_date <= end_date AND last_updated_date >= start_date
filtered_data = filtered_data[
    (filtered_data["first_seen_date"] <= end_date) &
    (filtered_data["last_updated_date"] >= start_date)
]

if selected_team != "All Teams":
    filtered_data = filtered_data[filtered_data["team"] == selected_team]
if filter_explicit:
    filtered_data = filtered_data[~filtered_data["explicit"].fillna(False)]
if filter_spotify_only:
    filtered_data = filtered_data[filtered_data["spotify_uri"].notna()]

# Add selection column - check against session state playlist
filtered_data = filtered_data.reset_index(drop=True)

# Create unique key for each song
def get_song_key(row):
    return f"{row['team']}|{row['player']}|{row['song_name']}"

filtered_data["_key"] = filtered_data.apply(get_song_key, axis=1)
filtered_data.insert(0, "Add", filtered_data["_key"].isin(st.session_state.playlist_songs))

# Display data
st.subheader(f"🎶 Songs ({len(filtered_data)})")

# Prepare display columns (no Play column - use last added song for preview)
display_cols = ["Add", "team", "player", "song_name", "song_artist", "explicit", "spotify_uri", "_key"]
display_data = filtered_data[display_cols].copy()

# Convert explicit to text
display_data["explicit"] = display_data["explicit"].apply(lambda x: "Explicit" if x else "")

# Store original spotify_uri for player before converting to link
display_data["_spotify_uri_original"] = filtered_data["spotify_uri"].copy()
display_data["spotify_uri"] = display_data["_spotify_uri_original"].apply(
    lambda x: x.replace("spotify:track:", "https://open.spotify.com/track/") if x else None
)

column_config = {
    "Add": st.column_config.CheckboxColumn("Add", default=False, width="small"),
    "team": st.column_config.TextColumn("Team", width="medium"),
    "player": st.column_config.TextColumn("Player", width="medium"),
    "song_name": st.column_config.TextColumn("Song", width="large"),
    "song_artist": st.column_config.TextColumn("Artist", width="medium"),
    "explicit": st.column_config.TextColumn("Explicit", width="small"),
    "spotify_uri": st.column_config.LinkColumn("Spotify", width="small", display_text="Open"),
    "_key": None,
    "_spotify_uri_original": None,
}

# Track the last added song for preview
if "last_added_key" not in st.session_state:
    st.session_state.last_added_key = None

# Store display_data in session state for fragment access
st.session_state._display_data = display_data

@st.fragment
def song_table_with_player():
    """Fragment containing data editor and player - prevents full page rerun."""
    disp_data = st.session_state._display_data

    def handle_table_edit():
        """Handle checkbox changes from data editor."""
        if "songs_table" not in st.session_state:
            return

        edits = st.session_state.songs_table.get("edited_rows", {})

        for row_idx_str, changes in edits.items():
            row_idx = int(row_idx_str)
            if row_idx >= len(disp_data):
                continue

            key = disp_data.iloc[row_idx]["_key"]

            if "Add" in changes:
                if changes["Add"]:
                    if key not in st.session_state.playlist_songs:
                        st.session_state.playlist_songs.append(key)
                        st.session_state.last_added_key = key
                else:
                    if key in st.session_state.playlist_songs:
                        st.session_state.playlist_songs.remove(key)

    st.data_editor(
        disp_data,
        column_config=column_config,
        hide_index=True,
        use_container_width=True,
        height=500,
        disabled=["team", "player", "song_name", "song_artist", "explicit", "spotify_uri", "_key", "_spotify_uri_original"],
        key="songs_table",
        on_change=handle_table_edit,
    )

    # Show embedded player for the last added song
    preview_key = st.session_state.last_added_key
    if not preview_key and st.session_state.playlist_songs:
        preview_key = st.session_state.playlist_songs[-1]

    if preview_key:
        preview_song_data = disp_data[disp_data["_key"] == preview_key]
        if not preview_song_data.empty:
            spotify_uri = preview_song_data.iloc[0]["_spotify_uri_original"]
            if spotify_uri:
                track_id = spotify_uri.replace("spotify:track:", "")
                embed_url = f"https://open.spotify.com/embed/track/{track_id}?utm_source=generator&theme=0"

                st.markdown(f"""
                <div style="display: flex; justify-content: center; margin: 1rem 0;">
                    <iframe src="{embed_url}" width="100%" height="152" frameBorder="0"
                            allowfullscreen="" allow="autoplay; clipboard-write; encrypted-media; fullscreen; picture-in-picture"
                            loading="lazy" style="border-radius: 12px; max-width: 600px;"></iframe>
                </div>
                """, unsafe_allow_html=True)

song_table_with_player()

# Button to sync playlist view (since fragment doesn't trigger full page rerun)
if st.button("⬇️ Add selection to playlist", help="Update the playlist view below"):
    st.rerun()

# Get all playlist songs data for display (outside fragment for playlist section)
playlist_data = all_data[all_data.apply(get_song_key, axis=1).isin(st.session_state.playlist_songs)].copy()

st.divider()

# Playlist creation section
st.markdown("""
<h2 style="margin-bottom: 1rem;">📀 Your Playlist</h2>
""", unsafe_allow_html=True)

if not st.session_state.playlist_songs:
    st.info("Select songs from the table above to add them to your playlist")
else:
    # Show playlist summary
    st.markdown(f"""
    <div style="
        background: linear-gradient(135deg, #1DB954 0%, #169c46 100%);
        padding: 1.5rem;
        border-radius: 12px;
        margin-bottom: 1rem;
    ">
        <span style="color: white; font-size: 1.5rem; font-weight: 600;">
            {len(st.session_state.playlist_songs)} songs selected
        </span>
    </div>
    """, unsafe_allow_html=True)

    # Playlist name input
    playlist_name = st.text_input(
        "Playlist name",
        value=f"MLB Walkup Songs - {datetime.date.today()}",
        max_chars=50,
    )

    # Create playlist button - prominent
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        if not spotify:
            st.warning("Login to Spotify to create playlists")
            create_disabled = True
        else:
            create_disabled = False

        if st.button(
            "🎵 Create Spotify Playlist",
            type="primary",
            disabled=create_disabled,
            use_container_width=True,
        ):
            # Get Spotify URIs for playlist songs
            spotify_tracks = playlist_data[playlist_data["spotify_uri"].notna()]["spotify_uri"].tolist()

            if not spotify_tracks:
                st.error("No selected songs have Spotify links")
            else:
                track_uris = [
                    uri if uri.startswith("spotify:track:") else f"spotify:track:{uri}"
                    for uri in spotify_tracks
                ]

                try:
                    me = spotify.me()
                    if not me:
                        st.error("Could not get Spotify user info")
                        st.stop()
                    user_id = me["id"]
                    playlist = spotify.user_playlist_create(
                        user=user_id,
                        name=playlist_name,
                        public=False,
                        description="Created with MLB Walkup Songs app",
                    )

                    spotify.user_playlist_add_tracks(
                        user=user_id,
                        playlist_id=playlist["id"],
                        tracks=track_uris,
                    )

                    st.success("✓ Playlist created!")
                    st.link_button(
                        "🎵 Open in Spotify",
                        playlist["external_urls"]["spotify"],
                    )
                except Exception as e:
                    st.error(f"Failed to create playlist: {e}")

    # Clear playlist button
    if st.button("Clear playlist", type="secondary"):
        st.session_state.playlist_songs = []
        st.rerun()

    # Show selected songs table
    st.subheader("Selected Songs")
    playlist_display = playlist_data[["team", "player", "song_name", "song_artist"]].copy()
    playlist_display.columns = ["Team", "Player", "Song", "Artist"]
    st.dataframe(
        playlist_display,
        hide_index=True,
        use_container_width=True,
    )

# Footer
st.divider()
st.markdown("""
<div style="text-align: center; color: #888; padding: 1rem 0;">
    <p style="margin: 0;">Data scraped from MLB.com</p>
    <p style="margin: 0.5rem 0 0 0; font-size: 0.85rem;">
        <a href="https://github.com/arvkevi/walkup/issues" style="color: #1DB954; text-decoration: none;">
            Report issues
        </a>
    </p>
</div>
""", unsafe_allow_html=True)

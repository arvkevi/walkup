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
def get_current_walkup_songs():
    """Get all current walkup songs from Supabase."""
    supabase = get_supabase_client()
    response = supabase.table("mlb_walk_up_songs").select("*").eq("is_current", True).execute()

    if not response.data:
        return pd.DataFrame()

    df = pd.DataFrame(response.data)
    df["team"] = df["team"].str.title()
    return df


@st.cache_data(ttl=300)
def get_stats():
    """Get summary statistics."""
    supabase = get_supabase_client()
    response = supabase.table("mlb_walk_up_songs").select("*").eq("is_current", True).execute()

    if not response.data:
        return {"songs": 0, "players": 0, "teams": 0, "spotify_songs": 0}

    df = pd.DataFrame(response.data)
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

# Stats row
stats = get_stats()
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

# Main content
data = get_current_walkup_songs()

if data.empty:
    st.warning("No walkup songs found. Run the scraper to populate the database.")
    st.stop()

# Filters in a styled container
st.subheader("🔍 Filter Songs")
with st.container():
    filter_col1, filter_col2, filter_col3 = st.columns([2, 1, 1])

    with filter_col1:
        teams = ["All Teams"] + sorted(data["team"].unique().tolist())
        selected_team = st.selectbox("Team", teams, label_visibility="collapsed",
                                      help="Filter by MLB team")

    with filter_col2:
        filter_explicit = st.checkbox("Hide explicit", help="Filter out explicit songs")

    with filter_col3:
        filter_spotify_only = st.checkbox("Spotify only", help="Only show songs available on Spotify")

# Apply filters
filtered_data = data.copy()
if selected_team != "All Teams":
    filtered_data = filtered_data[filtered_data["team"] == selected_team]
if filter_explicit:
    filtered_data = filtered_data[~filtered_data["explicit"].fillna(False)]
if filter_spotify_only:
    filtered_data = filtered_data[filtered_data["spotify_uri"].notna()]

# Add selection column
filtered_data = filtered_data.reset_index(drop=True)
filtered_data.insert(0, "Select", False)

# Display data
st.subheader(f"🎶 Songs ({len(filtered_data)})")

# Prepare display columns
display_cols = ["Select", "team", "player", "song_name", "song_artist", "explicit", "spotify_uri"]
display_data = filtered_data[display_cols].copy()

# Convert spotify_uri to clickable links
display_data["spotify_uri"] = display_data["spotify_uri"].apply(
    lambda x: x.replace("spotify:track:", "https://open.spotify.com/track/") if x else None
)

column_config = {
    "Select": st.column_config.CheckboxColumn("✓", default=False, width="small"),
    "team": st.column_config.TextColumn("Team", width="medium"),
    "player": st.column_config.TextColumn("Player", width="medium"),
    "song_name": st.column_config.TextColumn("Song", width="large"),
    "song_artist": st.column_config.TextColumn("Artist", width="medium"),
    "explicit": st.column_config.CheckboxColumn("🔞", width="small"),
    "spotify_uri": st.column_config.LinkColumn("Spotify", width="small", display_text="Open"),
}

edited_df = st.data_editor(
    display_data,
    column_config=column_config,
    hide_index=True,
    width="stretch",
    disabled=["team", "player", "song_name", "song_artist", "explicit", "spotify_uri"],
)

# Selected songs
selected_rows = edited_df[edited_df["Select"]]

st.divider()

# Playlist creation
st.subheader("📀 Create Playlist")

if selected_rows.empty:
    st.info("Select songs above to create a playlist")
else:
    st.write(f"**{len(selected_rows)}** songs selected")

    with st.form("playlist_form"):
        playlist_name = st.text_input(
            "Playlist name",
            value=f"MLB Walkup Songs - {datetime.date.today()}",
            max_chars=50,
        )

        col1, col2 = st.columns([1, 3])
        with col1:
            submit = st.form_submit_button(
                "Create Playlist",
                type="primary",
                disabled=not spotify,
            )

        if not spotify:
            st.warning("Login to Spotify to create playlists")

        if submit and spotify:
            # Filter to songs with Spotify URIs
            spotify_tracks = selected_rows[selected_rows["spotify_uri"].notna()]["spotify_uri"].tolist()

            if not spotify_tracks:
                st.error("No selected songs have Spotify links")
            else:
                # Convert URLs back to URIs for Spotify API
                track_uris = [
                    url.replace("https://open.spotify.com/track/", "spotify:track:")
                    for url in spotify_tracks
                ]

                try:
                    # Create playlist
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

                    # Add tracks
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

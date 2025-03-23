-- Create the mlb_walk_up_songs table
CREATE TABLE IF NOT EXISTS mlb_walk_up_songs (
    id SERIAL PRIMARY KEY,
    team VARCHAR(50) NOT NULL,
    player VARCHAR(100) NOT NULL,
    song_name VARCHAR(255) NOT NULL,
    song_artist VARCHAR(255) NOT NULL,
    walkup_date DATE NOT NULL,
    spotify_uri VARCHAR(255),
    explicit BOOLEAN,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(team, player, song_name)
);
-- Create an index on the unique constraint columns for better performance
CREATE INDEX IF NOT EXISTS idx_mlb_walk_up_songs_unique ON mlb_walk_up_songs(team, player, song_name);
-- Create a trigger to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column() RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = CURRENT_TIMESTAMP;
RETURN NEW;
END;
$$ language 'plpgsql';
CREATE TRIGGER update_mlb_walk_up_songs_updated_at BEFORE
UPDATE ON mlb_walk_up_songs FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
from cassandra.cluster import Cluster
import redis
from uuid import uuid4
from datetime import datetime
import json



# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

# Create Keyspace
session.execute("""
CREATE KEYSPACE IF NOT EXISTS game_keyspace
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
""")

# Connect to the keyspace
session.set_keyspace('game_keyspace')

# Create Player Data table
session.execute("""
CREATE TABLE IF NOT EXISTS player_profiles (
    username TEXT PRIMARY KEY,
    email TEXT,
    profile_picture TEXT,
    achievements LIST<TEXT>,
    inventory LIST<TEXT>,
    friends LIST<TEXT>
)
""")

# Create Game Data table
session.execute("""
CREATE TABLE IF NOT EXISTS game_data (
    game_name TEXT PRIMARY KEY,
    game_type TEXT,
    current_state TEXT,
    world_layout TEXT
)
""")

# Create Game Object Data table
session.execute("""
CREATE TABLE IF NOT EXISTS game_objects (
    object_id UUID PRIMARY KEY,
    object_type TEXT,
    position TEXT,
    attributes MAP<TEXT, TEXT>
)
""")

# Create Game Analytics table
session.execute("""
CREATE TABLE IF NOT EXISTS game_analytics (
    event_id UUID PRIMARY KEY,
    event_type TEXT,
    player_username TEXT,
    event_data MAP<TEXT, TEXT>,
    timestamp TIMESTAMP
)
""")



# Insert player profile
def insert_player_profile(username, email, profile_picture=None, achievements=None, inventory=None, friends=None):
    session.execute(
        """
        INSERT INTO player_profiles (username, email, profile_picture, achievements, inventory, friends)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (username, email, profile_picture, achievements, inventory, friends)
    )

# Retrieve player profile
def get_player_profile(username):
    result = session.execute(
        """
        SELECT * FROM player_profiles WHERE username=%s
        """,
        (username,)
    )
    return result.one()

# Insert game data
def insert_game_data(game_name, game_type, current_state, world_layout):
    session.execute(
        """
        INSERT INTO game_data (game_name, game_type, current_state, world_layout)
        VALUES (%s, %s, %s, %s)
        """,
        (game_name, game_type, current_state, world_layout)
    )

# Insert game object
def insert_game_object(object_type, position, attributes):
    object_id = uuid4()
    session.execute(
        """
        INSERT INTO game_objects (object_id, object_type, position, attributes)
        VALUES (%s, %s, %s, %s)
        """,
        (object_id, object_type, position, attributes)
    )

# Insert game analytics
def insert_game_analytics(event_type, player_username, event_data):
    event_id = uuid4()
    timestamp = datetime.now()
    session.execute(
        """
        INSERT INTO game_analytics (event_id, event_type, player_username, event_data, timestamp)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (event_id, event_type, player_username, event_data, timestamp)
    )

# Example usage
insert_player_profile('player1', 'player1@example.com', 'pic1.png', ['ach1', 'ach2'], ['item1', 'item2'], ['friend1', 'friend2'])
player_profile = get_player_profile('player1')
print(player_profile)
insert_game_data('Game1', 'Action', 'In progress', 'Map1')
insert_game_object('Character', '10,20', {'health': '100', 'speed': '10'})
insert_game_analytics('Game Started', 'player1', {'level': '1', 'score': '0'})







# Connect to Redis
redis_client = redis.Redis(host='localhost', port=6379, db=0)



# Update player location
def update_player_location(player_id, x_location, y_location):
    timestamp = datetime.now().isoformat()
    location_data = {
        'x': x_location,
        'y': y_location,
        'timestamp': timestamp
    }
    location_data_json = json.dumps(location_data)
    # Append the new location data to the player's location history list
    redis_client.rpush(f'player_location:{player_id}', location_data_json)

# Log game event
def log_game_event(player_name, event_data):
    event_data['timestamp'] = datetime.now().isoformat()
    event_data_json = json.dumps(event_data)
    # Append the new event data to the player's event list
    redis_client.rpush(f'game_state:{player_name}', event_data_json)

# Update leaderboard
def update_leaderboard(metric, player_id, score):
    redis_client.zadd(f'leaderboard:{metric}', {player_id: score})

# Send chat message
def send_chat_message(guild_id, player_name, message):
    chat_message = {
        'player_name': player_name,
        'message': message
    }
    # convert to JSON
    chat_message_json = json.dumps(chat_message)
    # Push the JSON string to the Redis list
    redis_client.rpush(f'chat:messages:{guild_id}', chat_message_json)


# Update player statistics
def update_player_stat(player_id, stat_type, value):
    value['timestamp'] = datetime.now().isoformat()
    value_json = json.dumps(value)
    # use hashes to update player statistics
    redis_client.hset(f'player_stats:{player_id}', stat_type, value_json)


# Example usage
update_player_location('handal', '10','10')
log_game_event('handaal',{'type': 'Died', 'details': 'yelkiki assassinated him'})
update_leaderboard('points', 'Diaa', 700)
update_leaderboard('wins', 'player4', 5)
send_chat_message('guild2', 'player1', 'Hello Guild!')
send_chat_message('guild2', 'player2', 'Hi there!')
update_player_stat('player1', 'bomboclat', {'damage_dealt': 500, 'enemies_defeated': 88})

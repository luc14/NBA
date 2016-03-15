import json
import pandas as pd
file = open('0021500575.json')
data = json.load(file)
# dict_keys(['gameid', 'events', 'gamedate'])
postions = []
real_times = set()
for event in data['events']:
    if event['moments']:       
        for moment in event['moments']:
            quarter, real_time, game_time, shot_time, _, player_records = moment
            if real_time in real_times:
                continue
            else:
                real_times.add(real_time)           
            for player_record in player_records:                
                team_id, player_id, x, y, z = player_record
                positions.append({'player_id': player_id,
                                  'game_time': game_time, 
                                  'x': x, 'y': y, 'z': z, 
                                  'real_time': real_time})
                
position_df = pd.DataFrame(positions)
position_df.sort_values(['player_id', 'real_time'], inplace = True)

speed_valid = (position_df['game_time'].diff() < 0) & (position_df['real_time'].diff() < 45) & \
    (position_df['player_id'].shift(1) == position_df['player_id'])

position_df['time_interval'] = position_df['real_time'].diff()
position_df['distance'] = (position_df['x'].diff()**2 +  position_df['y'].diff()**2 +  position_df['z'].diff()**2) ** 0.5
position_df['speed'] = position_df['distance'] / position_df['time_interval']
position_df.loc[ ~speed_valid , 'speed'] = None

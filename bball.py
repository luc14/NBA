import json
import pandas as pd

filename0 = '0021500575.json'

def import_data(filename):
    file = open(filename)
    data = json.load(file)
    # dict_keys(['gameid', 'events', 'gamedate'])
    positions = []
    real_times = set()
    player_id_name = {}
    for event in data['events']:
        
        # edit player_id_name
        for team in ['visitor', 'home']:    
            for player in event[team]['players']: #player is a dict
                name = player['firstname'] + ' ' + player['lastname']
                player_id = player['playerid']
                if player_id not in player_id_name:
                    player_id_name[player_id] = name
        
        if event['moments']:       
            for moment in event['moments']:
                period, real_time, game_time, shot_time, _, player_records = moment
                
                # if this moment is already recorded in another event, skip it
                if real_time in real_times:
                    continue
                else:
                    real_times.add(real_time)  
                 
                record_idx = len(positions) -1    
                for player_record in player_records: 
                    record_idx += 1
                    team_id, player_id, x, y, z = player_record
                    if player_id  == -1:
                        ball_x = x
                        ball_y = y 
                        ball_z = z
                        closest_to_ball = float('inf')
                    else:
                        distance_to_ball = ((x - ball_x)**2 + (y - ball_y)**2)**0.5
                        if distance_to_ball < closest_to_ball:
                            closest_to_ball = distance_to_ball
                            player_withball_idx = record_idx
                    
                    positions.append({'player_id': player_id,
                                      'period': period,
                                      'game_time': game_time, 
                                      'x': x, 'y': y, 'z': z, 
                                      'real_time': real_time})
                if closest_to_ball <4:
                    positions[player_withball_idx]['with_ball'] = True
                    
    position_df = pd.DataFrame(positions)
    position_df.sort_values(['player_id', 'real_time'], inplace = True)
    
    # remove speed in breaks or pauses, and when there is gap in data
    speed_valid = (position_df['game_time'].diff() < 0) & (position_df['real_time'].diff() < 45) & \
        (position_df['player_id'].shift(1) == position_df['player_id'])
    
    position_df['time_interval'] = position_df['real_time'].diff()
    position_df['distance'] = (position_df['x'].diff()**2 +  position_df['y'].diff()**2 +  position_df['z'].diff()**2) ** 0.5
    position_df['speed'] = position_df['distance'] / position_df['time_interval']
    position_df.loc[ ~speed_valid , 'speed'] = None
    
    # add a column indicating who is holding the ball
    #player_lst = set(position_df['player_id']) - {-1}
    #for player_id in player_lst: 
        #rows = position_df['player_id'] == player_id 
        #balls = position_df[position_df['player_id'] == -1 &  position_df['real_time'] == position_df['real_time']].set_index(rows.index)[['x', 'y']]
        
        #position_df.loc[rows ,'distance_ball']= \
            #((position_df.loc[rows,'x'] - balls['x']) **2 + (position_df.loc[rows,'y'] - balls['y']) **2 ) ** 0.5
    return position_df, player_id_name


import json
import pandas as pd
import datetime

filename0 = '0021500575.json'

def import_data(filename):
    file = open(filename)
    data = json.load(file)
    # dict_keys(['gameid', 'events', 'gamedate'])
    positions = []
    timeline = []
    real_times = set()
    player_id_name = {}
    
    for event in data['events']:
        
               # edit player_id_name
        #for team in ['visitor', 'home']: 
            
            #for player in event[team]['players']: #player is a dict
                #name = player['firstname'] + ' ' + player['lastname']
                #player_id = player['playerid']
                #if player_id not in player_id_name:
                    #player_id_name[player_id] = name
        #player_id_name[-1] = 'ball'        
        
        if event['moments']:       
            for moment in event['moments']:
                # save info about players and ball at this moment
                moment_positions = []
                period, real_time, game_time, shot_time, _, player_records = moment
                
                #game_time = datetime.timedelta(seconds = game_time)
                # if this moment is already recorded in another event, skip it
                if real_time in real_times:
                    continue
                real_times.add(real_time)
                timeline.append({'real_time': real_time, 'player_id': -1})
                
                ball_absent = True
                for player_record in player_records: 
                    team_id, player_id, x, y, z = player_record
                    
                    moment_positions.append({'player_id': player_id,
                                             'period': period,
                                             'game_time': game_time, 
                                             'x': x, 'y': y, 'z': z, 
                                             'real_time': real_time})                    
                    if player_id == -1:
                        #ball = player_record # ball !!!!!
                        ball_x = x
                        ball_y = y 
                        ball_z = z
                        closest_to_ball = float('inf')
                        ball_absent = False
                
                
                if ball_absent is False:
                    
                    for idx, moment_position in enumerate(moment_positions):
                        if moment_position['player_id'] == -1:
                            continue
                        
                        distance_to_ball = ((moment_position['x'] - ball_x)**2 + (moment_position['y'] - ball_y)**2)**0.5
                        if distance_to_ball < closest_to_ball:
                            closest_to_ball = distance_to_ball
                            player_withball_id = moment_position['player_id']
                            
                    if closest_to_ball < 4 and ball_z < 11:
                        #moment_positions[player_withball_idx]['maybe_with_ball'] = True                                            
                        timeline[-1]['player_id'] = player_withball_id

                        
                positions += moment_positions
                # moment ends
                    
    position_df = pd.DataFrame(positions)
    position_df.sort_values(['player_id', 'real_time'], inplace = True)
    
    # remove speed in breaks or pauses, and when there is gap in data
    speed_valid = (position_df['game_time'].diff() < 0) & (position_df['real_time'].diff() < 45) & \
        (position_df['player_id'].shift(1) == position_df['player_id'])
    
    position_df['time_interval'] = position_df['real_time'].diff()
    position_df['game_time'] = pd.to_timedelta(position_df['game_time'], unit = 's')
    position_df['distance'] = (position_df['x'].diff()**2 +  position_df['y'].diff()**2 +  position_df['z'].diff()**2) ** 0.5
    position_df['speed'] = position_df['distance'] / position_df['time_interval']
    position_df.loc[ ~speed_valid , 'speed'] = None
    # switch speed from 1 feet/ ms to 1 m/s
    position_df['speed'] = position_df['speed'] * 305
    position_df['speed'] = position_df['speed'].rolling(window=5, center = True).mean()
    position_df['acceleration'] = (1000*position_df['speed'].diff()/position_df['time_interval']).rolling(window=5, center = True).mean()
    
    # add a column indicating who is holding the ball
    #player_lst = set(position_df['player_id']) - {-1}
    #for player_id in player_lst: 
        #rows = position_df['player_id'] == player_id 
        #balls = position_df[position_df['player_id'] == -1 &  position_df['real_time'] == position_df['real_time']].set_index(rows.index)[['x', 'y']]
        
        #position_df.loc[rows ,'distance_ball']= \
            #((position_df.loc[rows,'x'] - balls['x']) **2 + (position_df.loc[rows,'y'] - balls['y']) **2 ) ** 0.5
    
    timeline = pd.DataFrame(timeline)
    timeline.sort_values('real_time', inplace = True)
    # decide who is holding the ball:
    timeline_possession_start = timeline[timeline['player_id'].shift(1) != timeline['player_id']].copy()
    timeline_possession_start['hold_time'] = timeline_possession_start['real_time'].diff().shift(-1)
    timeline = timeline_possession_start[timeline_possession_start['hold_time'] > 500]
    # might have several identical consecutive records
    
    event = data['events'][0]
    players_info = pd.DataFrame(event['visitor']['players'] + event['home']['players'] + [{'playerid': -1, 'firstname': 'ball'}])
    players_info.rename(columns= {'playerid': 'player_id'}, inplace =True)
    players_info['name'] = players_info['firstname'] + ' ' + players_info['lastname']
    players_info.drop(['firstname', 'lastname'], axis=1, inplace=True)
    #players_info.ix[-1, 'name'] = 'ball'
    
    timeline_possession = pd.merge(timeline, players_info, on= ['player_id'], how ='left')
    timeline_possession = pd.merge(timeline_possession, position_df[['player_id', 'real_time', 'game_time', 'period']], on = ['player_id', 'real_time'], how='left')
   
    return position_df, players_info, timeline_possession

def analyze_possession(speed, timeline_possession, timeline_idx, plot = True):
    player_id = timeline_possession.loc[timeline_idx,'player_id']
    time_start = timeline_possession.loc[timeline_idx, 'real_time']
    time_end = time_start + timeline_possession.loc[timeline_idx, 'hold_time']
    player_speed = speed[(speed['player_id'] == player_id) & (speed['real_time'] >= time_start) & (speed['real_time'] < time_end)]
    if plot:
        print(timeline_possession.loc[[timeline_idx]])
        player_speed.plot(x = 'real_time', y = 'acceleration')
        player_speed.plot(x = 'real_time', y = 'speed')
    return player_speed

def main():
    
    speed, player_id_name, timeline = import_data(filename0)
    
if __name__ == '__main__':
    main()
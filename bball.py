import json
import pandas as pd
import datetime

filename0 = '0021500575.json'

def import_data(filename):
    file = open(filename)
    data = json.load(file)
    # dict_keys(['gameid', 'events', 'gamedate'])
    positions = []
    possessions = []
    real_times = set()
    
    for event in data['events']:
         
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
                possessions.append({'real_time': real_time, 'player_id': -1})
                
                ball_absent = True
                for player_record in player_records: 
                    team_id, player_id, x, y, z = player_record
                    
                    moment_positions.append({'player_id': player_id,
                                             'period': period,
                                             'game_time': game_time, 
                                             'x': x, 'y': y, 'z': z, 
                                             'real_time': real_time})                    
                    if player_id == -1:
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
                        possessions[-1]['player_id'] = player_withball_id

                        
                positions += moment_positions
                # moment ends
                    
    speed = pd.DataFrame(positions)
    speed.sort_values(['player_id', 'real_time'], inplace = True)
    
    # remove speed in breaks or pauses, and when there is gap in data
    speed_valid = (speed['game_time'].diff() < 0) & (speed['real_time'].diff() < 45) & \
        (speed['player_id'].shift(1) == speed['player_id'])
    
    speed['time_interval'] = speed['real_time'].diff()
    speed['game_time'] = pd.to_timedelta(speed['game_time'], unit = 's')
    speed['distance'] = (speed['x'].diff()**2 +  speed['y'].diff()**2 +  speed['z'].diff()**2) ** 0.5
    speed['speed'] = speed['distance'] / speed['time_interval']
    speed.loc[ ~speed_valid , ['speed', 'distance']] = None
    # switch speed from 1 feet/ ms to 1 m/s
    speed['speed'] = speed['speed'] * 305
    speed['speed'] = speed['speed'].rolling(window=5, center = True).mean().clip(0, 8)
    speed['acceleration'] = (1000*speed['speed'].diff()/speed['time_interval']).rolling(window=5, center = True).mean().clip(-10, 10)
        
    possessions = pd.DataFrame(possessions)
    possessions.sort_values('real_time', inplace = True)
    # decide who is holding the ball:
    timeline_possession_start = possessions[possessions['player_id'].shift(1) != possessions['player_id']].copy()
    timeline_possession_start['hold_time'] = timeline_possession_start['real_time'].diff().shift(-1)
    possessions = timeline_possession_start[timeline_possession_start['hold_time'] > 500]
    # might have several identical consecutive records
    
    event = data['events'][0]
    players = pd.DataFrame(event['visitor']['players'] + event['home']['players'] + [{'playerid': -1, 'firstname': 'ball'}])
    players.rename(columns= {'playerid': 'player_id'}, inplace =True)
    players['name'] = players['firstname'] + ' ' + players['lastname']
    players.drop(['firstname', 'lastname'], axis=1, inplace=True)
    #players.ix[-1, 'name'] = 'ball'
    
    possessions = pd.merge(possessions, players, on= ['player_id'], how ='left')
    possessions = pd.merge(possessions, speed[['player_id', 'real_time', 'game_time', 'period']], on = ['player_id', 'real_time'], how='left')
   
    return speed[speed['player_id'] != -1], players, possessions

def analyze_possession(speed, possessions, timeline_idx, plot = True):
    player_id = possessions.loc[timeline_idx,'player_id']
    time_start = possessions.loc[timeline_idx, 'real_time']
    time_end = time_start + possessions.loc[timeline_idx, 'hold_time']
    player_speed = speed[(speed['player_id'] == player_id) & (speed['real_time'] >= time_start) & (speed['real_time'] < time_end)]
    if plot:
        print(possessions.loc[[timeline_idx]])
        player_speed.plot(x = 'real_time', y = 'acceleration')
        player_speed.plot(x = 'real_time', y = 'speed')
    return player_speed

def main():
    
    speed, players, possessions = import_data(filename0)
    
if __name__ == '__main__':
    main()
import json
import pandas as pd
import datetime
import sqlite3, glob, subprocess, os, traceback

filename0 = '0021500575.json'

def import_data(filename):
    file = open(filename)
    data = json.load(file)
    # dict_keys(['gameid', 'events', 'gamedate'])
    positions = []
    possessions = []
    real_times = set()
    
    game_id = data['gameid']
    
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
                possessions.append({'real_time': real_time, 'game_time': game_time, 'period': period, 'player_id': -1})
                
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
    speed['cum_game_time'] = speed['period'].clip(upper=4) * 12 * 60 +  (speed['period'].clip(lower=4) - 4) * 5 * 60  - speed['game_time']
    #speed['game_time'] = pd.to_timedelta(speed['game_time'], unit = 's')
    speed['distance'] = (speed['x'].diff()**2 +  speed['y'].diff()**2 +  speed['z'].diff()**2) ** 0.5
    speed['speed'] = speed['distance'] / speed['time_interval']
    speed.loc[ ~speed_valid , ['speed', 'distance']] = None
    # switch speed from 1 feet/ ms to 1 m/s
    speed['speed'] = speed['speed'] * 305
    speed['speed'] = speed['speed'].rolling(window=5, center = True).mean().clip(0, 8)
    speed['acceleration'] = (1000*speed['speed'].diff()/speed['time_interval']).rolling(window=5, center = True).mean().clip(-10, 10)
        
    # decide who is holding the ball:    
    possessions = pd.DataFrame(possessions)
    possessions.sort_values('real_time', inplace = True)
    possessions.loc[possessions['real_time'].diff().shift(-1) > 45, 'player_id'] = -1
    possessions.loc[possessions['game_time'].diff() >= 0, 'player_id'] = -1
    possessions = possessions[possessions['player_id'].shift(1) != possessions['player_id']]
    possessions = possessions[possessions['real_time'].diff().shift(-1)> 500]
    possessions = possessions[possessions['player_id'].shift(1) != possessions['player_id']] 

    event = data['events'][0]
    players = pd.DataFrame(event['visitor']['players'] + event['home']['players'] + [{'playerid': -1, 'firstname': 'ball', 'lastname': ''}])
    players.rename(columns= {'playerid': 'player_id'}, inplace =True)
    players['name'] = players['firstname'] + ' ' + players['lastname']
    players.drop(['firstname', 'lastname'], axis=1, inplace=True)
    #players.ix[-1, 'name'] = 'ball'
    
    speed = speed[speed['player_id'] != -1]
    #speed.loc[:, 'real_time'] = speed['real_time'] - speed['real_time'].min()
    speed['real_time'] -= speed['real_time'].min()
    
    
    players_lst = set(speed['player_id']) 
    players = players[players['player_id'].isin(players_lst)]
    
    possessions = pd.merge(possessions, players, on= ['player_id'], how ='left')
    #speed.drop(['game_time', 'period'], inplace=True)
    
    return speed, players, possessions, game_id

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
    #speed, players, possessions = import_data(filename0)
    create_database()
    
def create_database():
    connection = sqlite3.connect('basketball')
    all_players = pd.DataFrame()
    c = connection.cursor()
    decompress_folder = './decompress/'
    try:
        os.mkdir(decompress_folder)
    except:
        pass
    try:
        c.execute('DROP TABLE players')
        c.execute('DROP TABLE speed')
    except:
        pass
    c.execute('CREATE TABLE players (player_id, name, PRIMARY KEY(player_id))')     
    
    #c.execute('CREATE TABLE players (player_id, player_name, jersey, team)')
    #c.execute('INSERT INTO players VALUES (?, ?, ?, ?)', (player_id, player_name, jersey, team) )
    for i, compressed_filename in enumerate(glob.glob( '*.7z')):
        try:
            for old_file in glob.glob(decompress_folder + '*.json'):
                os.remove(old_file)
            subprocess.run(['./unar', compressed_filename, '-o', 'decompress'])
            game_filename = glob.glob('./decompress/*.json')[0]
            speed, players, possessions, game_id = import_data(game_filename)
            all_players = pd.concat([all_players, players[['player_id', 'name']]], ignore_index=True, axis = 0)
            all_players.drop_duplicates(inplace=True)
            
            if i == 0:
                c.execute('CREATE TABLE speed({}, PRIMARY KEY(real_time, player_id, game_id))'.format(','.join(list(speed.columns) + ['game_id'])))
                c.execute('CREATE TABLE possession({}, PRIMARY KEY(real_time, game_id))'.format(','.join(list(possessions.columns) + ['game_id'])))
            speed['game_id'] = game_id
            speed.to_sql('speed', connection, if_exists='append', index=False)
        except Exception as e:
            print(traceback.format_exc(), file=open('report.txt', 'a'))
            print(compressed_filename, game_filename, file=open('report.txt', 'a'))
    
    all_players.to_sql('players', connection, if_exists='append', index=False)
        
if __name__ == '__main__':
    main()
    
    

import os
import time
import json
import py7zr
import shutil
import sqlite3
from collections import OrderedDict

required_tables = set([
    "AirportCommunication", "AirportLookup", "Airports", "AirwayLegs", "Airways", "config", 
    "Gls", "GridMora", "Holdings", "ILSes", "Markers", "MarkerTypes", "NavaidLookup", 
    "Navaids", "NavaidTypes", "Runways", "SurfaceTypes", "TerminalLegs", "TerminalLegsEx", 
    "Terminals", "TrmLegTypes", "WaypointLookup", "Waypoints"
])

def get_db3_file_path(prompt):
    while True:
        file_path = input(prompt).strip().strip('\'"') 
        if os.path.exists(file_path) and file_path.endswith('.db3'):
            conn = sqlite3.connect(file_path)
            tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
            tables = set([table[0] for table in tables])
            if required_tables.issubset(tables):
                return conn
            else:
                print("所读取文件不是fenix数据库格式，请重新输入db3文件路径：")
                conn.close()
        else:
            print(f"文件路径无效或不是一个.db3文件，请重新输入。")

def get_terminal_id():
    terminal_id = input("请输入要转换的起始TerminalID：")
    return terminal_id

if not os.path.exists('Primary/ProcedureLegs'):
    os.makedirs('Primary/ProcedureLegs')

def fetch_waypoints(cursor):
    cursor.execute('SELECT ID, Ident FROM Waypoints')
    return {row[0]: row[1] for row in cursor.fetchall()}

def format_row(row_dict, table_name, waypoints_dict):
    if 'Longtitude' in row_dict:
        row_dict['Longitude'] = round(row_dict.pop('Longtitude'), 8)
    if 'Latitude' in row_dict:
        row_dict['Latitude'] = round(row_dict['Latitude'], 8)

    if table_name == 'AirportLookup':
        ordered_columns = ["ID", "extID"]
    elif table_name == 'Ilses':
        ordered_columns = ['ID', 'RunwayID', 'Freq', 'GsAngle', 'Latitude', 'Longitude', 'Category', 'Ident', 'LocCourse', 'CrossingHeight', 'Elevation', 'HasDme']
    elif table_name == 'Terminals':
        ordered_columns = ['ID', 'AirportID', 'Proc', 'ICAO', 'FullName', 'Name', 'Rwy', 'RwyID']
    elif table_name == 'Airports':
        if 'TransitionAltitude' in row_dict:
            row_dict['TransAlt'] = row_dict.pop('TransitionAltitude')
        ordered_columns = ["Elevation", "ICAO", "ID", "Latitude", "Longitude", "Name", "PrimaryID", "TransAlt"]
    elif table_name == 'AirwayLegs':
        row_dict['Waypoint1'] = waypoints_dict.get(row_dict['Waypoint1ID'], None)
        row_dict['Waypoint2'] = waypoints_dict.get(row_dict['Waypoint2ID'], None)
        ordered_columns = ["ID", "AirwayID", "Level", "Waypoint1ID", "Waypoint2ID", "IsStart", "IsEnd", "Waypoint1", "Waypoint2"]
    elif table_name == 'Navaids':
        row_dict.pop('MagneticVariation', None)
        row_dict.pop('Range', None)
        ordered_columns = row_dict.keys()
    elif table_name == 'Waypoints':
        ordered_columns = ["ID", "Ident", "Name", "Latitude", "NavaidID", "Longitude", "Collocated"]
    elif table_name == 'Runways':
        ordered_columns = ["ID", "AirportID", "Ident", "TrueHeading", "Length", "Width", "Surface", "Latitude", "Longitude", "Elevation"]
    else:
        ordered_columns = row_dict.keys()

    return {col: row_dict[col] for col in ordered_columns if col in row_dict}

def export_db3_to_json(conn, start_terminal_id):
    cursor = conn.cursor()
    table_names = [
        'AirportLookup', 'Airports', 'AirwayLegs', 'Airways',
        'Ilses', 'NavaidLookup', 'Navaids', 'Runways',
        'Terminals', 'WaypointLookup', 'Waypoints'
    ]

    waypoints_dict = fetch_waypoints(cursor)

    for table_name in table_names:
        cursor.execute(f'SELECT * FROM {table_name}')
        rows = cursor.fetchall()

        data = []
        for row in rows:
            columns = [col[0] for col in cursor.description]
            row_dict = dict(zip(columns, row))
            formatted_row = format_row(row_dict, table_name, waypoints_dict)
            data.append(formatted_row)

        json_file = f'Primary/{table_name}.json'
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, separators=(',', ':'))

    # 处理TerminalLegs和TerminalLegsEx表
    cursor.execute('SELECT * FROM TerminalLegs WHERE TerminalID >= ?', (start_terminal_id,))
    terminal_legs = cursor.fetchall()
    terminal_legs_columns = [col[0] for col in cursor.description]

    cursor.execute('SELECT * FROM TerminalLegsEx')
    terminal_leg_ex = cursor.fetchall()
    terminal_leg_ex_columns = [col[0] for col in cursor.description]

    terminal_leg_ex_dict = {row[0]: dict(zip(terminal_leg_ex_columns, row)) for row in terminal_leg_ex}

    terminal_data = {}
    for row in terminal_legs:
        row_dict = dict(zip(terminal_legs_columns, row))
        terminal_id = row_dict['TerminalID']
        if terminal_id not in terminal_data:
            terminal_data[terminal_id] = []

        # IsFAF和IsMAP默认值
        row_dict['IsFAF'] = 0
        row_dict['IsMAP'] = 0 if row_dict['Alt'] != 'MAP' else -1

        # 从TerminalLegEx中添加IsFlyOver和SpeedLimit
        leg_ex = terminal_leg_ex_dict.get(row_dict['ID'], {})
        row_dict['IsFlyOver'] = leg_ex.get('IsFlyOver', None)
        row_dict['SpeedLimit'] = leg_ex.get('SpeedLimit', None)

        # 检查并填充Lat和Lon
        def fetch_coordinates(cursor, table, id_value):
            cursor.execute(f'SELECT Latitude, Longtitude FROM {table} WHERE ID = ?', (id_value,))
            return cursor.fetchone()
        
        def update_coordinates(row_dict, lat_key, lon_key, coordinates):
            if coordinates:
                row_dict[lat_key] = round(coordinates[0], 8)
                row_dict[lon_key] = round(coordinates[1], 8)

        if all(row_dict[key] is None for key in ['WptID', 'WptLat', 'WptLon']) and row_dict['Alt'] == 'MAP':
            cursor.execute('SELECT RwyID FROM Terminals WHERE ID = ?', (terminal_id,))
            rwy_id = cursor.fetchone()
            if rwy_id:
                coordinates = fetch_coordinates(cursor, 'Runways', rwy_id[0])
                update_coordinates(row_dict, 'WptLat', 'WptLon', coordinates)
        elif row_dict['WptID'] is not None and row_dict['WptLat'] is None and row_dict['WptLon'] is None:
            coordinates = fetch_coordinates(cursor, 'Waypoints', row_dict['WptID'])
            update_coordinates(row_dict, 'WptLat', 'WptLon', coordinates)
        elif row_dict['CenterID'] is not None and row_dict['CenterLat'] is None and row_dict['CenterLon'] is None:
            coordinates = fetch_coordinates(cursor, 'Waypoints', row_dict['CenterID'])
            update_coordinates(row_dict, 'CenterLat', 'CenterLon', coordinates)
        elif row_dict['NavID'] is not None and row_dict['NavLat'] is None and row_dict['NavLon'] is None:
            coordinates = fetch_coordinates(cursor, 'Waypoints', row_dict['NavID'])
            update_coordinates(row_dict, 'NavLat', 'NavLon', coordinates)

        terminal_data[terminal_id].append(row_dict)

    for terminal_id, legs in terminal_data.items():
        for i, leg in enumerate(legs):
            if 0 < i < len(legs) - 1:
                prev_leg = legs[i - 1]
                next_leg = legs[i + 1]
                #判断是否为FAF点                
                valid = True
                for j in range(i, -1, -1):
                    vnav_value = legs[j]['Vnav']
                    if vnav_value is not None and str(vnav_value).replace('.', '', 1).isdigit():
                        vnav_value = float(vnav_value)
                        if vnav_value >= 2.5:
                            valid = False
                            break
                    elif vnav_value is not None:
                        valid = False
                        break

                if valid and next_leg['Vnav'] is not None and str(next_leg['Vnav']).replace('.', '', 1).isdigit():
                    next_vnav_value = float(next_leg['Vnav'])
                    if next_vnav_value > 2.5:
                        leg['IsFAF'] = -1

        ordered_legs = []
        for leg in legs:
            ordered_leg = OrderedDict()
            for col in ['ID', 'TerminalID', 'Type', 'Transition', 'TrackCode', 'WptID', 'WptLat', 'WptLon', 'TurnDir', 'NavID', 'NavLat', 'NavLon', 'NavBear', 'NavDist', 'Course', 'Distance', 'Alt', 'Vnav', 'CenterID', 'CenterLat', 'CenterLon', 'IsFlyOver', 'SpeedLimit', 'IsFAF', 'IsMAP']:
                value = leg.get(col, None)
                if col in ['TurnDir', 'Transition', 'Alt'] and value is None:
                    value = "" 
                if col == 'SpeedLimit' and value is not None:
                    value = int(value)
                if col == 'IsFlyOver' and value == 1 :
                    value = -1
                ordered_leg[col] = value
            ordered_legs.append(ordered_leg)

        json_file = f'Primary/ProcedureLegs/TermID_{terminal_id}.json'
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(ordered_legs, f, ensure_ascii=False, separators=(',', ':'))

    conn.close()

def compress_and_cleanup():
    # 压缩 Primary 文件夹
    archive_path = 'Primary.7z'
    with py7zr.SevenZipFile(archive_path, 'w') as archive:
        for root, dirs, files in os.walk('Primary'):
            
            for file in files:
                file_path = os.path.join(root, file)
                relative_path = os.path.relpath(file_path, 'Primary')
                archive.write(file_path, relative_path)

    shutil.rmtree('Primary')
if __name__ == "__main__":
    conn = get_db3_file_path("请输入Fenix NDB文件路径：")
    if conn:
        terminal_id = get_terminal_id()
        export_db3_to_json(conn, terminal_id)
        compress_and_cleanup()
        print("数据已成功导出并压缩为Primary.7z")
        time.sleep(2)

import mysql.connector
import pandas as pd
import numpy as np

df = pd.read_csv("Traffic_Crashes_-_Crashes.csv", encoding="latin1")

config = {
    'user': 'root',
    'password': 'Octubre03',
    'host': 'localhost',
    'database': 'sys',
}

try:
    conn = mysql.connector.connect(**config)
    if conn.is_connected():
        print("Conexión exitosa a la base de datos MySQL")

except mysql.connector.Error as e:
    print(f"Error al conectar a la base de datos: {e}")

def convert_to_int(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return 0  

df['CRASH_DATE'] = pd.to_datetime(df['CRASH_DATE'], format='%m/%d/%Y %I:%M:%S %p')
df['CRASH_DATE'] = df['CRASH_DATE'].dt.strftime('%Y-%m-%d %H:%M:%S')
df['STATEMENTS_TAKEN_I'] = df['STATEMENTS_TAKEN_I'].apply(lambda x: 1 if x == 'Y' else (0 if x == 'N' else x))
df['DATE_POLICE_NOTIFIED'] = pd.to_datetime(df['DATE_POLICE_NOTIFIED'], format='%m/%d/%Y %I:%M:%S %p').dt.strftime('%Y-%m-%d %H:%M:%S')
df['PHOTOS_TAKEN_I'] = df['PHOTOS_TAKEN_I'].map({'Y': 1, 'N': 0})
boolean_columns = ['CRASH_DATE_EST_I', 'INTERSECTION_RELATED_I', 'NOT_RIGHT_OF_WAY_I', 'HIT_AND_RUN_I', 'DOORING_I', 'WORK_ZONE_I', 'WORKERS_PRESENT_I']
for column in boolean_columns:
    df[column] = df[column].apply(lambda x: 1 if x else 0)

numeric_columns = [
    'POSTED_SPEED_LIMIT', 'LANE_CNT', 'STREET_NO', 'PHOTOS_TAKEN_I',
    'STATEMENTS_TAKEN_I', 'NUM_UNITS', 'INJURIES_TOTAL', 'INJURIES_FATAL',
    'INJURIES_INCAPACITATING', 'INJURIES_NON_INCAPACITATING',
    'INJURIES_REPORTED_NOT_EVIDENT', 'INJURIES_NO_INDICATION',
    'INJURIES_UNKNOWN', 'CRASH_HOUR', 'CRASH_DAY_OF_WEEK', 'CRASH_MONTH'
]

df[numeric_columns] = df[numeric_columns].fillna(0)

text_columns = ['TRAFFIC_CONTROL_DEVICE', 'DEVICE_CONDITION', 'WEATHER_CONDITION', 'LIGHTING_CONDITION', 'FIRST_CRASH_TYPE', 'TRAFFICWAY_TYPE', 'ALIGNMENT', 'ROADWAY_SURFACE_COND', 'ROAD_DEFECT', 'REPORT_TYPE', 'CRASH_TYPE', 'PRIM_CONTRIBUTORY_CAUSE', 'SEC_CONTRIBUTORY_CAUSE', 'STREET_DIRECTION', 'STREET_NAME', 'BEAT_OF_OCCURRENCE', 'MOST_SEVERE_INJURY', 'LOCATION']
df[text_columns] = df[text_columns].fillna('')


create_table_query = """
CREATE TABLE IF NOT EXISTS Datb (
    CRASH_RECORD_ID VARCHAR(250),
    RD_NO VARCHAR(250),
    CRASH_DATE_EST_I BOOLEAN,
    CRASH_DATE TIMESTAMP,
    POSTED_SPEED_LIMIT INTEGER,
    TRAFFIC_CONTROL_DEVICE VARCHAR(250),
    DEVICE_CONDITION VARCHAR(250),
    WEATHER_CONDITION VARCHAR(250),
    LIGHTING_CONDITION VARCHAR(250),
    FIRST_CRASH_TYPE VARCHAR(250),
    TRAFFICWAY_TYPE VARCHAR(250),
    LANE_CNT INTEGER,
    ALIGNMENT VARCHAR(250),
    ROADWAY_SURFACE_COND VARCHAR(250),
    ROAD_DEFECT VARCHAR(250),
    REPORT_TYPE VARCHAR(250),
    CRASH_TYPE VARCHAR(250),
    INTERSECTION_RELATED_I BOOLEAN,
    NOT_RIGHT_OF_WAY_I BOOLEAN,
    HIT_AND_RUN_I BOOLEAN,
    DAMAGE VARCHAR(250),
    DATE_POLICE_NOTIFIED TIMESTAMP,
    PRIM_CONTRIBUTORY_CAUSE VARCHAR(100),
    SEC_CONTRIBUTORY_CAUSE VARCHAR(100),
    STREET_NO INTEGER,
    STREET_DIRECTION VARCHAR(250),
    STREET_NAME VARCHAR(250),
    BEAT_OF_OCCURRENCE VARCHAR(250),
    PHOTOS_TAKEN_I BOOLEAN,
    STATEMENTS_TAKEN_I BOOLEAN,
    DOORING_I BOOLEAN,
    WORK_ZONE_I BOOLEAN,
    WORK_ZONE_TYPE VARCHAR(250),
    WORKERS_PRESENT_I BOOLEAN,
    NUM_UNITS INTEGER,
    MOST_SEVERE_INJURY VARCHAR(250),
    INJURIES_TOTAL INTEGER,
    INJURIES_FATAL INTEGER,
    INJURIES_INCAPACITATING INTEGER,
    INJURIES_NON_INCAPACITATING INTEGER,
    INJURIES_REPORTED_NOT_EVIDENT INTEGER,
    INJURIES_NO_INDICATION INTEGER,
    INJURIES_UNKNOWN INTEGER,
    CRASH_HOUR INTEGER,
    CRASH_DAY_OF_WEEK INTEGER,
    CRASH_MONTH INTEGER,
    LATITUDE NUMERIC,
    LONGITUDE NUMERIC,
    LOCATION TEXT
);

"""

insert_query = """
INSERT INTO Datb (CRASH_RECORD_ID, RD_NO, CRASH_DATE_EST_I, CRASH_DATE, POSTED_SPEED_LIMIT, 
    TRAFFIC_CONTROL_DEVICE, DEVICE_CONDITION, WEATHER_CONDITION, LIGHTING_CONDITION, FIRST_CRASH_TYPE,
    TRAFFICWAY_TYPE, LANE_CNT, ALIGNMENT, ROADWAY_SURFACE_COND, ROAD_DEFECT, REPORT_TYPE, CRASH_TYPE, 
    INTERSECTION_RELATED_I, NOT_RIGHT_OF_WAY_I, HIT_AND_RUN_I, DAMAGE, DATE_POLICE_NOTIFIED, 
    PRIM_CONTRIBUTORY_CAUSE, SEC_CONTRIBUTORY_CAUSE, STREET_NO, STREET_DIRECTION, STREET_NAME, 
    BEAT_OF_OCCURRENCE, PHOTOS_TAKEN_I, STATEMENTS_TAKEN_I, DOORING_I, WORK_ZONE_I, WORK_ZONE_TYPE, 
    WORKERS_PRESENT_I, NUM_UNITS, MOST_SEVERE_INJURY, INJURIES_TOTAL, INJURIES_FATAL, 
    INJURIES_INCAPACITATING, INJURIES_NON_INCAPACITATING, INJURIES_REPORTED_NOT_EVIDENT, 
    INJURIES_NO_INDICATION, INJURIES_UNKNOWN, CRASH_HOUR, CRASH_DAY_OF_WEEK, CRASH_MONTH, LATITUDE, 
    LONGITUDE, LOCATION) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s
);
"""

try:
    if conn.is_connected():
        cursor = conn.cursor()

        cursor.execute(create_table_query)
        
        for index, row in df.iterrows():
            values = (
                row['CRASH_RECORD_ID'], row['RD_NO'], row['CRASH_DATE_EST_I'], row['CRASH_DATE'],
                row['POSTED_SPEED_LIMIT'], row['TRAFFIC_CONTROL_DEVICE'], row['DEVICE_CONDITION'],
                row['WEATHER_CONDITION'], row['LIGHTING_CONDITION'], row['FIRST_CRASH_TYPE'],
                row['TRAFFICWAY_TYPE'], row['LANE_CNT'], row['ALIGNMENT'], row['ROADWAY_SURFACE_COND'],
                row['ROAD_DEFECT'], row['REPORT_TYPE'], row['CRASH_TYPE'], row['INTERSECTION_RELATED_I'],
                row['NOT_RIGHT_OF_WAY_I'], row['HIT_AND_RUN_I'], row['DAMAGE'], row['DATE_POLICE_NOTIFIED'],
                row['PRIM_CONTRIBUTORY_CAUSE'], row['SEC_CONTRIBUTORY_CAUSE'], row['STREET_NO'],
                row['STREET_DIRECTION'], row['STREET_NAME'], row['BEAT_OF_OCCURRENCE'], row['PHOTOS_TAKEN_I'],
                row['STATEMENTS_TAKEN_I'], row['DOORING_I'], row['WORK_ZONE_I'], row['WORK_ZONE_TYPE'],
                row['WORKERS_PRESENT_I'], row['NUM_UNITS'], row['MOST_SEVERE_INJURY'], row['INJURIES_TOTAL'],
                row['INJURIES_FATAL'], row['INJURIES_INCAPACITATING'], row['INJURIES_NON_INCAPACITATING'],
                row['INJURIES_REPORTED_NOT_EVIDENT'], row['INJURIES_NO_INDICATION'], row['INJURIES_UNKNOWN'],
                row['CRASH_HOUR'], row['CRASH_DAY_OF_WEEK'], row['CRASH_MONTH'], row['LATITUDE'],
                row['LONGITUDE'], row['LOCATION']
            )
            cursor.execute(insert_query, values)

        conn.commit()

        print("Datos insertados exitosamente en la tabla Datb.")

except mysql.connector.Error as e:
    print("Error al ejecutar la consulta:", e)

finally:
    if conn.is_connected():
        cursor.close()
        conn.close()
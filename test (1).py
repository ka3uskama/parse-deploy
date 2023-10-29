import psycopg2
from psycopg2 import sql
import requests
import datetime as dt
from datetime import datetime, timedelta
import time

host = "localhost"
port = "5432"
user = "postgres"
password = "r%ibB3f0#h0i"
database = "db_peach"

connection = psycopg2.connect(
    host=host,
    port=port,
    user=user,
    password=password,
    database=database
)

api_key = 's3m9p47eccaxcb2dko88ondt65sawbyf5'
List_Events = "https://wickads-ld.irev.com/api/external/v1/events"
List_Conversions = "https://wickads-ld.irev.com/api/external/v1/conversions"
Rotation_details = "https://wickads-ld.irev.com/api/external/v1/rotation"
Statistics_Drilldown = "https://wickads-ld.irev.com/api/external/v1/statistics"

headers = {
    "Authorization": api_key,
    "Content-Type": "application/json"
}

conversions_payload = {
    "page": 1,
    "limit": 100,
    "filters": {
        #"country": ["AU"],
        #"isTest": [False]
    },
    "order_values": {
        #"country": "asc"
    }
}

events_payload = {
    "page": 1,
    "limit": 100,
    "filters": {
        #"country": ["AU"],
        #"isTest": [False]
    },
    "order_values": {
        #"country": "asc"
    }
}



def send_error(text):
    TOKEN = "6032236467:AAGj33GpBBssYSDmLyG7RBJpxCeu7NQaej8"
    chat_id = "579340957"
    now = dt.datetime.now()
    message = text + f". Time: {now}"
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"
    requests.get(url).json()

def check_table_exists(cursor, table_name):
    try:
        # Проверка существования таблицы
        query = sql.SQL("SELECT EXISTS ("
                        "SELECT 1 FROM information_schema.tables "
                        "WHERE table_schema = 'public' AND table_name = %s)")
        cursor.execute(query, (table_name,))
        exists = cursor.fetchone()[0]
        return exists
    except psycopg2.Error as e:
        send_error(f"Error while query attempt: {e}")
        return False

def create_table(cursor, table_name):
    sql_code_events = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        uuid VARCHAR(1000),
        adv_sub VARCHAR(1000),
        aff_sub VARCHAR(1000),
        aff_sub10 VARCHAR(1000),
        aff_sub11 VARCHAR(1000),
        aff_sub12 VARCHAR(1000),
        aff_sub13 VARCHAR(1000),
        aff_sub14 VARCHAR(1000),
        aff_sub15 VARCHAR(1000),
        aff_sub16 VARCHAR(1000),
        aff_sub17 VARCHAR(1000),
        aff_sub18 VARCHAR(1000),
        aff_sub19 VARCHAR(1000),
        aff_sub2 VARCHAR(1000),
        aff_sub20 VARCHAR(1000),
        aff_sub3 VARCHAR(1000),
        aff_sub4 VARCHAR(1000),
        aff_sub5 VARCHAR(1000),
        aff_sub6 VARCHAR(1000),
        aff_sub7 VARCHAR(1000),
        aff_sub8 VARCHAR(1000),
        aff_sub9 VARCHAR(1000),
        affiliate_id INTEGER,
        offer_id INTEGER,
        hash VARCHAR(1000),
        advertiserUuid VARCHAR(1000),
        advertiser VARCHAR(1000),
        country VARCHAR(1000),
        saleStatus VARCHAR(1000),
        crmSaleStatus VARCHAR(1000),
        fsmState VARCHAR(1000),
        manualStatusType VARCHAR(1000),
        wasRejected BOOLEAN,
        listOfAdvertisers VARCHAR(1000),
        allRejectionReasons VARCHAR(1000),
        rejectionReason VARCHAR(1000),
        autoLoginSuccess BOOLEAN,
        autoLoginUrl VARCHAR(1000),
        autoLoginDomain VARCHAR(1000),
        stateUpdatedAt TIMESTAMP,
        payout DECIMAL(10, 2),
        revenue DECIMAL(10, 2),
        leadSource VARCHAR(1000),
        funnelGroupUuid VARCHAR(1000),
        funnelGroup VARCHAR(1000),
        funnelUuid VARCHAR(1000),
        funnelLanguage VARCHAR(1000),
        tpUuid VARCHAR(1000),
        trafficProvider VARCHAR(1000),
        externalId VARCHAR(1000),
        ip VARCHAR(1000),
        apiToken VARCHAR(1000),
        apiTokenName VARCHAR(1000),
        fsmUuid VARCHAR(1000),
        fsm VARCHAR(1000),
        comment TEXT,
        isRisky BOOLEAN,
        isHasConversions BOOLEAN,
        isLate BOOLEAN,
        isTest BOOLEAN,
        afmDate TIMESTAMP,
        createdAt TIMESTAMP,
        PRIMARY KEY (uuid)
    );
    """

    sql_code_conversions = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        uuid UUID,
        lead_adv_sub VARCHAR(700),
        lead_aff_sub VARCHAR(700),
        lead_aff_sub10 VARCHAR(700),
        lead_aff_sub11 VARCHAR(700),
        lead_aff_sub12 VARCHAR(700),
        lead_aff_sub13 VARCHAR(700),
        lead_aff_sub14 VARCHAR(700),
        lead_aff_sub15 VARCHAR(700),
        lead_aff_sub16 VARCHAR(700),
        lead_aff_sub17 VARCHAR(700),
        lead_aff_sub18 VARCHAR(700),
        lead_aff_sub19 VARCHAR(700),
        lead_aff_sub2 VARCHAR(700),
        lead_aff_sub20 VARCHAR(700),
        lead_aff_sub3 VARCHAR(700),
        lead_aff_sub4 VARCHAR(700),
        lead_aff_sub5 VARCHAR(700),
        lead_aff_sub6 VARCHAR(700),
        lead_aff_sub7 VARCHAR(700),
        lead_aff_sub8 VARCHAR(700),
        lead_aff_sub9 VARCHAR(700),
        lead_affiliate_id VARCHAR(700),
        lead_offer_id VARCHAR(700),
        lead_hash VARCHAR(700),
        leadUuid VARCHAR(700),
        payout DECIMAL(10, 2),
        revenue DECIMAL(10, 2),
        funnelGroupUuid VARCHAR(700),
        funnelGroup VARCHAR(700),
        funnelUuid VARCHAR(700),
        funnel_name VARCHAR(700),
        tpUuid VARCHAR(700),
        trafficProvider VARCHAR(700),
        country VARCHAR(700),
        goalTypeUuid VARCHAR(700),
        goalType VARCHAR(700),
        goalUuid VARCHAR(700),
        goalName VARCHAR(700),
        advertiserUuid VARCHAR(700),
        advertiser VARCHAR(700),
        manualStatusType VARCHAR(700),
        isApproved BOOLEAN,
        isLate BOOLEAN,
        isTest BOOLEAN,
        afmDate TIMESTAMP,
        createdAt TIMESTAMP
    );
    """
    try:
        if table_name == "events":
            cursor.execute(sql_code_events)
            connection.commit()
        elif table_name == "conversions":
            cursor.execute(sql_code_conversions)
            connection.commit()
    except Exception as e:
        send_error(f"Error creating table '{table_name}': {e}")


def check_table(cursor, name):
    query = sql.SQL("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    cursor.execute(query)
    tables = cursor.fetchall()
    for table in tables:
        if table[0] == name:
            return True
    return False

def format_date(input_date):
    # Преобразовать строку в объект datetime
    dt_object = dt.strptime(input_date, "%Y-%m-%dT%H:%M:%S.%fZ")
    # Преобразовать объект datetime обратно в строку с нужным форматом
    formatted_date = dt_object.strftime("%Y-%m-%d %H:%M:%S.%f")
    return formatted_date[:-3] + "000"

def compare_items(item1, item2):
    try:
        if item1['uuid'] == item2[0] and str(format_date(item1['createdAt'])) == str(item2[-1]):
            return True
        return False
    except:
        return False

def check_last_row(name, cursor):
    cursor.execute(f"SELECT * FROM {name} ORDER BY createdAt DESC LIMIT 1")
    row = cursor.fetchone()
    if row:
        return row
    return 0

def add_row_to_table(connection, cursor, table_name, data):
    # Ensure data keys match the table columns
    if table_name == 'events':
        valid_columns = [
            'uuid', 'adv_sub', 'aff_sub', 'aff_sub10', 'aff_sub11', 'aff_sub12',
            'aff_sub13', 'aff_sub14', 'aff_sub15', 'aff_sub16', 'aff_sub17',
            'aff_sub18', 'aff_sub19', 'aff_sub2', 'aff_sub20', 'aff_sub3',
            'aff_sub4', 'aff_sub5', 'aff_sub6', 'aff_sub7', 'aff_sub8', 'aff_sub9',
            'affiliate_id', 'offer_id', 'hash', 'advertiserUuid', 'advertiser',
            'country', 'saleStatus', 'crmSaleStatus', 'fsmState',
            'manualStatusType', 'wasRejected', 'listOfAdvertisers',
            'allRejectionReasons', 'rejectionReason', 'autoLoginSuccess',
            'autoLoginUrl', 'autoLoginDomain', 'stateUpdatedAt', 'payout',
            'revenue', 'leadSource', 'funnelGroupUuid', 'funnelGroup',
            'funnelUuid', 'funnelLanguage', 'tpUuid', 'trafficProvider',
            'externalId', 'ip', 'apiToken', 'apiTokenName', 'fsmUuid',
            'fsm', 'comment', 'isRisky', 'isLate', 'isHasConversions',
            'isTest', 'createdAt'
        ]
    # Filter data dictionary to include only valid columns
        special = ['affiliate_id', 'offer_id', 'wasRejected', 'autoLoginSuccess', 'stateUpdatedAt', 'payout', 'revenue', 'isRisky', 'isLate', 'isHasConversions', 'isTest',  'createdAt']
        data = {key: data[key] for key in valid_columns}
        for i in data:
            if i not in special:
                data[i] = str(data[i])
    # Generate the SQL query
        query = sql.SQL(f"INSERT INTO {table_name} ({', '.join(data.keys())}) "
                    f"VALUES ({', '.join(['%s']*len(data))}) RETURNING *")
        # Execute the query
        cursor.execute(query, list(data.values()))
        # Commit the transaction
        connection.commit()
        inserted_row = cursor.fetchone()

    elif table_name == 'conversions':
        valid_columns1 = ['uuid', 'lead_adv_sub', 'lead_aff_sub', 'lead_aff_sub10', 'lead_aff_sub11', 'lead_aff_sub12', 'lead_aff_sub13', 'lead_aff_sub14', 'lead_aff_sub15', 'lead_aff_sub16', 'lead_aff_sub17', 'lead_aff_sub18', 'lead_aff_sub19', 'lead_aff_sub2', 'lead_aff_sub20', 'lead_aff_sub3', 'lead_aff_sub4', 'lead_aff_sub5', 'lead_aff_sub6', 'lead_aff_sub7', 'lead_aff_sub8', 'lead_aff_sub9', 'lead_affiliate_id', 'lead_offer_id', 'lead_hash', 'leadUuid', 'payout', 'revenue', 'funnelGroupUuid', 'funnelGroup', 'funnelUuid', 'tpUuid', 'trafficProvider', 'country', 'goalTypeUuid', 'goalType', 'goalUuid', 'goalName', 'advertiserUuid', 'advertiser', 'manualStatusType', 'isApproved', 'isLate', 'isTest', 'afmDate', 'createdAt']
        special1 = ['payout', 'revenue', 'isApproved', 'isLate', 'isTest', 'afmDate', 'createdAt']
        data = {key: data[key] for key in valid_columns1}
        for i in data:
            if i not in special1:
                data[i] = str(data[i])
        query = sql.SQL(f"INSERT INTO {table_name} ({', '.join(data.keys())}) "
                    f"VALUES ({', '.join(['%s']*len(data))}) RETURNING *")
        # Execute the query
        cursor.execute(query, list(data.values()))
        # Commit the transaction
        connection.commit()
        inserted_row = cursor.fetchone()

def parse(url, payload, cursor, name):
    current_payload = payload.copy()
    last_row_db = check_last_row(name, cursor)
    data_to_add = []
    while True:
        response = requests.post(url, headers=headers, json=current_payload)
        if response.status_code == 200:
            data = response.json()
            if data['rows'] != []:
                for dictionary in data['rows']:
                    print(dictionary)
                    if not compare_items(dictionary, last_row_db):
                        data_to_add.append(dictionary)
                    else:
                        print('загружаем в БД, нашли совпадение')
                        for dictionary in data_to_add:
                            add_row_to_table(connection, cursor, name, dictionary)
                        return
            else:
                print('загружаем в БД, нет совпадения')
                for dictionary in data_to_add:
                    add_row_to_table(connection, cursor, name, dictionary)
                return
        else:
            send_error(f"Error: {response.status_code} - {response.text}")
            return
        current_payload['page'] += 1

if __name__ == "__main__":
    interval = 0.5  # interval in hours
    while True:
        connection = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
        cursor = connection.cursor()
        tables = ["events", "conversions"]
        for name in tables:
            if not check_table(cursor, name):
                create_table(cursor, name)
        parse(List_Events, events_payload, cursor, tables[0])
        print('events done')
        parse(List_Conversions, conversions_payload, cursor, tables[1])
        print('conversions done')
        cursor.close()
        connection.close()

        next_hour = dt.datetime.now() + timedelta(hours=interval)
        time.sleep((next_hour - dt.datetime.now()).seconds)
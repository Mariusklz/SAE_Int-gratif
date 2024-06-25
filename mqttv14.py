import paho.mqtt.client as mqtt
import pymysql
from datetime import datetime
from cachetools import TTLCache
import threading
import time

broker = "test.mosquitto.org"
topic = "IUT/Colmar2024/SAE2.04/Maison1"
port = 1883

# Configuration du cache avec TTL (Time-To-Live)
cache = TTLCache(maxsize=1000, ttl=300)  # Max 1000 éléments, durée de vie 300 secondes

db_config = {
    'host': '10.252.13.66',
    'user': 'toto',
    'password': 'toto',
    'database': 'sae'
}

sensors = {}
nom2_counter = 0
db = None
cursor = None
stop_threads = False

def connect_db():
    return pymysql.connect(**db_config)

def on_connect(client, userdata, flags, rc):
    print("Connecté avec le code de retour: " + str(rc))
    client.subscribe(topic)

def on_message(client, userdata, msg):
    message = msg.payload.decode('utf-8')
    print(f"Message reçu sur le topic {msg.topic}: {message}")
    process_message(message)

def process_message(message):
    global nom2_counter
    global db, cursor

    data = {}
    for item in message.split(','):
        key, value = item.split('=')
        data[key.strip()] = value.strip()

    sensor_id = data['Id']
    piece = data['piece']
    timestamp = datetime.strptime(f"{data['date']} {data['time']}", "%d/%m/%Y %H:%M:%S")
    value = float(data['temp'])

    if sensor_id not in sensors:
        sensors[sensor_id] = {
            'nom': sensor_id,
            'piece': piece,
            'emplacement': ''
        }

    if db is None or cursor is None:
        # Stocker les données dans le cache si la base de données n'est pas disponible
        cache_key = f"{sensor_id}_{timestamp}"
        cache[cache_key] = (sensor_id, piece, timestamp, value)
        print(f"Données mises en cache: {cache_key}")
        return

    try:
        cursor.execute("SELECT ID FROM mqtt_capteurs WHERE Nom = %s AND Piece = %s", (sensor_id, piece))
        existing_sensor = cursor.fetchone()

        if existing_sensor:
            print(f"mqtt_capteurs {sensor_id} pour la pièce {piece} existe déjà dans la base de données.")
        else:
            nom2_counter += 1
            nom2_value = f"default_name_{nom2_counter}"

            cursor.execute("INSERT INTO mqtt_capteurs (nom, piece, emplacement, nom2) VALUES (%s, %s, %s, %s)",
                           (sensor_id, piece, '', nom2_value))
            db.commit()
            print(f"mqtt_capteurs {sensor_id} inséré dans la base de données pour la pièce {piece} avec nom2 = {nom2_value}")
    except pymysql.Error as e:
        print(f"Erreur lors de l'insertion ou vérification du capteur {sensor_id} : {e}")
        db.rollback()

    try:
        cursor.execute("INSERT INTO mqtt_donnees (capteurID_id, timestamp, valeur) VALUES ((SELECT ID FROM mqtt_capteurs WHERE Nom = %s AND Piece = %s), %s, %s)",
                       (sensor_id, piece, timestamp, value))
        db.commit()
        print("Données insérées avec succès")
    except pymysql.Error as e:
        print(f"Erreur lors de l'insertion des données : {e}")
        db.rollback()
        # Stocker les données dans le cache en cas d'échec
        cache_key = f"{sensor_id}_{timestamp}"
        cache[cache_key] = (sensor_id, piece, timestamp, value)
        print(f"Données mises en cache: {cache_key}")

def retry_cached_data():
    global db, cursor
    while not stop_threads:
        if db is not None and cursor is not None:
            for key in list(cache):
                sensor_id, piece, timestamp, value = cache[key]
                try:
                    cursor.execute("INSERT INTO mqtt_donnees (capteurID_id, timestamp, valeur) VALUES ((SELECT ID FROM mqtt_capteurs WHERE Nom = %s AND Piece = %s), %s, %s)",
                                   (sensor_id, piece, timestamp, value))
                    db.commit()
                    print(f"Données insérées avec succès depuis le cache: {key}")
                    del cache[key]  # Supprimer du cache après insertion réussie
                except pymysql.Error as e:
                    print(f"Erreur lors de la réinsertion des données depuis le cache: {e}")
                    # Si une erreur se produit, tenter de reconnecter la base de données
                    db, cursor = None, None
                    break
        time.sleep(5)  # Attendre 5 secondes avant de réessayer

def reconnect_db():
    global db, cursor
    while not stop_threads:
        if db is None or cursor is None:
            try:
                db = connect_db()
                cursor = db.cursor()
                print("Connexion à la base de données rétablie.")
            except pymysql.Error as e:
                print(f"Erreur lors de la reconnexion à la base de données : {e}")
        time.sleep(5)  # Attendre 5 secondes avant de réessayer

reconnect_thread = threading.Thread(target=reconnect_db)
reconnect_thread.daemon = True
reconnect_thread.start()

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(broker, port, 60)

client.loop_start()

cache_thread = threading.Thread(target=retry_cached_data)
cache_thread.daemon = True
cache_thread.start()

try:
    print("Démarrage de la boucle MQTT. Appuyez sur Ctrl+C pour arrêter.")
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Interruption par l'utilisateur. Arrêt du programme.")
    stop_threads = True
    client.disconnect()
    reconnect_thread.join()
    cache_thread.join()
    if cursor:
        cursor.close()
    if db:
        db.close()
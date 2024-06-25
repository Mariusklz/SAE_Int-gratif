


import paho.mqtt.client as mqtt #pip install paho.mqtt
import pymysql #pip install pymysql
from datetime import datetime
#pip install cryptography

# Connexion MySQL
db = pymysql.connect(
    host="10.252.13.66",
    user="toto",
    password="toto",
    database="sae"
)
cursor = db.cursor()

sensors = {}


# Paramètres MQTT
broker = "test.mosquitto.org"
topic = "IUT/Colmar2024/SAE2.04/Maison1"
port = 1883

sensors = {}

# Callback de connexion MQTT
def on_connect(client, userdata, flags, rc):
    print("Connecté avec le code de retour: " + str(rc))
    client.subscribe(topic)

# Callback de réception de message MQTT
def on_message(client, userdata, msg):
    message = msg.payload.decode('utf-8')
    print(f"Message reçu sur le topic {msg.topic}: {message}")
    
    # Vérifier si le message remplit les critères attendus
    if 'Id=' not in message or 'piece=' not in message or 'date=' not in message or ('time=' not in message and 'heure=' not in message) or 'temp=' not in message:
        print("Message MQTT incomplet ou mal formé. Ignorer le message.")
        return
    process_message(message)










# Fonction pour traiter les messages reçus
def process_message(message):
    data = {}
    for item in message.split(','):
        key_value = item.split('=')
        if len(key_value) == 2:
            key = key_value[0].strip()
            value = key_value[1].strip()
            data[key] = value

    # Vérifier si les clés nécessaires sont présentes
    if 'Id' not in data or 'piece' not in data or ('date' not in data and ('time' not in data and 'heure' not in data)) or 'temp' not in data:
        print("Message MQTT incomplet ou mal formé.")
        return

    sensor_id = data['Id']
    piece = data['piece']
    
    # Récupération de la date et de l'heure
    if 'date' in data and 'time' in data:
        timestamp_str = data['date']
        time_str = data['time']
    elif 'date' in data and 'heure' in data:
        timestamp_str = data['date']
        time_str = data['heure']
    else:
        print("Les clés 'date' et 'time'/'heure' ne sont pas présentes dans les données MQTT.")
        return

    try:
        timestamp = datetime.strptime(f"{timestamp_str} {time_str}", "%d/%m/%Y %H:%M:%S")
    except ValueError as e:
        print(f"Erreur lors du parsing de la date/heure : {e}")
        return

    try:
        value = float(data['temp'])
    except ValueError as e:
        print(f"Erreur lors de la conversion de la température en nombre : {e}")
        return

    # Vérifier si le capteur existe déjà pour cette pièce
    try:
        capteur, created = Capteurs.objects.get_or_create(nom=sensor_id, defaults={'piece': piece, 'emplacement': ''})
        if created:
            print(f"Capteur {sensor_id} inséré dans la base de données pour la pièce {piece}")
        else:
            print(f"Capteur {sensor_id} pour la pièce {piece} existe déjà dans la base de données.")
    except Exception as e:
        print(f"Erreur lors de l'insertion ou vérification du capteur {sensor_id} : {e}")
        return

    try:
        Donnees.objects.create(capteurID=capteur, timestamp=timestamp, valeur=value)
        print("Données insérées avec succès")
    except Exception as e:
        print(f"Erreur lors de l'insertion des données : {e}")

# Fonction pour supprimer les données anciennes
def delete_old_data():
    try:
        # Calculer la date et heure actuelle moins 1 heure
        cutoff_time = datetime.now() - timedelta(hours=1)

        # Supprimer les données anciennes
        old_data = Donnees.objects.filter(timestamp__lt=cutoff_time)
        count = old_data.delete()[0]
        print(f"{count} enregistrements supprimés avant {cutoff_time}")
    except Exception as e:
        print(f"Erreur lors de la suppression des données : {e}")

# Configuration et lancement du client MQTT
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(broker, port, 60)

# Boucle principale
try:
    print("Démarrage du programme MQTT avec suppression des données anciennes toutes les 30 minutes.")
    while True:
        client.loop_start()  # Démarrer la boucle MQTT
        
        # Attendre 30 minutes
        time.sleep(1800)  # 30 minutes = 1800 secondes
        
        # Arrêter la boucle MQTT
        client.loop_stop()
        
        # Supprimer les données anciennes
        delete_old_data()

except KeyboardInterrupt:
    print("Interruption par l'utilisateur. Arrêt du programme.")
    client.disconnect()

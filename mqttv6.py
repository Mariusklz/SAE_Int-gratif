import paho.mqtt.client as mqtt #pip install paho.mqtt
import pymysql #pip install pymysql
from datetime import datetime
#pip install cryptography

broker = "test.mosquitto.org"
topic = "IUT/Colmar2024/SAE2.04/Maison1"
port = 1883

# Connexion MySQL
db = pymysql.connect(
    host="10.252.13.66",
    user="toto",
    password="toto",
    database="sae"
)
cursor = db.cursor()

sensors = {}

# Callback de connexion MQTT
def on_connect(client, userdata, flags, rc):
    print("Connecté avec le code de retour: " + str(rc))
    client.subscribe(topic)

# Callback de réception de message MQTT
def on_message(client, userdata, msg):
    message = msg.payload.decode('utf-8')
    print(f"Message reçu sur le topic {msg.topic}: {message}")
    process_message(message)

# Fonction pour traiter les messages reçus
def process_message(message):
    data = {}
    for item in message.split(','):
        key, value = item.split('=')
        data[key.strip()] = value.strip()

    capteurID= data['Id']
    piece = data['piece']
    timestamp = datetime.strptime(f"{data['date']} {data['time']}", "%d/%m/%Y %H:%M:%S")
    valeur = float(data['temp'])

    # Vérifier si le capteur existe déjà pour cette pièce
    if capteurID not in sensors:
        sensors[capteurID] = {
            'Nom': capteurID,
            'Piece': piece,
            'Emplacement': ''
        }

    try:
        cursor.execute("SELECT ID FROM Capteurs WHERE Nom = %s AND Piece = %s", (capteurID, piece))
        existing_sensor = cursor.fetchone()

        if existing_sensor:
            # Capteur existant, ne rien faire ici
            print(f"Capteur {capteurID} pour la pièce {piece} existe déjà dans la base de données.")
        else:
            # Capteur n'existe pas encore pour cette pièce, l'ajouter
            cursor.execute("INSERT INTO Capteurs (Nom, Piece, Emplacement) VALUES (%s, %s, %s)",
                           (capteurID, piece, ''))
            db.commit()
            print(f"Capteur {capteurID} inséré dans la base de données pour la pièce {piece}")
    except pymysql.Error as e:
        print(f"Erreur lors de l'insertion ou vérification du capteur {capteurID} : {e}")
        db.rollback()

    try:
        cursor.execute("INSERT INTO Donnees (CapteurID, Timestamp, Valeur) VALUES ((SELECT ID FROM Capteurs WHERE Nom = %s AND Piece = %s), %s, %s)",
                       (capteurID, piece, timestamp, valeur))
        db.commit()
        print("Données insérées avec succès")
    except pymysql.Error as e:
        print(f"Erreur lors de l'insertion des données : {e}")
        db.rollback()

# Configuration et lancement du client MQTT
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(broker, port, 60)

# Boucle de réception des messages MQTT
try:
    print("Démarrage de la boucle MQTT. Appuyez sur Ctrl+C pour arrêter.")
    client.loop_forever()
except KeyboardInterrupt:
    print("Interruption par l'utilisateur. Arrêt du programme.")
    client.disconnect()
    cursor.close()
    db.close()
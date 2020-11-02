# Taller2BI


from datetime import timezone, datetime

from spotipy.oauth2 import SpotifyClientCredentials

from sqlalchemy import create_engine

import spotipy
import requests
import pandas as pd
import psycopg2
from sqlalchemy.sql.functions import now

CLIENT_ID = '12934672ffe24c80a76292d39307c4d6'
CLIENT_SECRET = 'dab4fb1334df411aa9379c565ae41334'

AUTH_URL = 'https://accounts.spotify.com/api/token'

# POST
auth_response = requests.post(AUTH_URL, {
    'grant_type': 'client_credentials',
    'client_id': CLIENT_ID,
    'client_secret': CLIENT_SECRET,
})

# convert the response to JSON
auth_response_data = auth_response.json()

# save the access token
access_token = auth_response_data['access_token']

headers = {
    'Authorization': 'Bearer {token}'.format(token=access_token)
}
BASE_URL = 'https://api.spotify.com/v1/'

artist_id = ['74ASZWbe4lXaubB36ztrGX']


# pull all artists albums
for artista_id in artist_id:
            r = requests.get(BASE_URL + 'artists/' + artista_id + '/top-tracks?market=ES' ,
                             headers=headers,
                             params={'include_groups': 'album', 'limit': 50})
            data = requests.get(BASE_URL + 'artists/' + artista_id ,
                             headers=headers,
                             )


            artista_data=data.json()
            tracks_data=r.json()["tracks"]

nombre=[]
nombre.append(artista_data["name"])
uri=[]
uri.append(artista_data["uri"])
tipo=[]
tipo.append(artista_data["type"])
popularidad=[]
popularidad.append(artista_data["popularity"])
cant_seguidores=[]
cant_seguidores.append(artista_data["followers"]["total"])
origen=[]
origen.append(artista_data["href"])
#carga=[]
#carga.append(now.replace(tzinfo=timezone.utc).timestamp())

n={'nombre':nombre,
   'uri':uri,
   'tipo':tipo,
   'popularidad':popularidad,
   'cant_seguidores':cant_seguidores,
   'origen':origen
 }
artista=pd.DataFrame(data=n)

#variables para acceder a los datos  necesarios de las canciones que me entrega la api en forma de json
nombresTracks = []
TipoTrack = []
PopularidadTracks = []
Artistas = []
Fecha = []
id = []
urlTracks = []
Album = []
Generos = []
cargaTrack = []
OrigenTracks = []

##Estas 2 lineas son las que crean la tabla



conexion1 = psycopg2.connect(database="BodegaDeDatos", user="postgres", password="12345")
cursor1=conexion1.cursor()
sql="insert into artist(nombre, uri,popularidad, cant_seguidores, tipo, origen) values (%s,%s,%s,%s,%s,%s)"
datos=(nombre, uri,popularidad, cant_seguidores, tipo, origen)
cursor1.execute(sql, datos)

tracks=[]
for track in tracks_data:

                OrigenTracks.append(track['href'])
                nombresTracks.append(track['name'])
                TipoTrack.append(track['type'])
                Artistas.append(track["artists"][0]["name"])
                Album.append(track["album"]["name"])
                PopularidadTracks.append(track["popularity"])
                Fecha.append(track["album"]["release_date"])
                id.append(track["id"])
                Generos.append(artista_data["genres"])
                urlTracks.append(track["uri"])
                now = datetime.now()
                cargaTrack.append(now.replace(tzinfo=timezone.utc).timestamp())
                OrigenTracks.append(track['href'])

datosCanciones = {'ID':id,'nombre': nombresTracks, 'Tipo': TipoTrack, 'Uri': urlTracks, 'popularidad': PopularidadTracks,'artista':Artistas,'FechaDeLanzamiento':Fecha,'Generos':Generos,'FechaCarga':cargaTrack}
Tracks = pd.DataFrame(data=datosCanciones)

engine = create_engine('postgresql://postgres:12345@127.0.0.1:5432/BodegaDeDatos')
Tracks.to_sql('tracks', con=engine, index=False)

print(Tracks)

conexion1.commit()
conexion1.close()

print(artista)


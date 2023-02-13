import requests
import pymongo
import json
#---------------------------------------   Load data using page numbers    --------------------------------------------#
#---------------produces a list for each page so not as useful --------------------------------------------------------#
#As the data is spread over multiple pages, have to iterate over the pages for that resource (e.g. starships, people etc.)
# def API_scrape(resource):
#     """ Call on SWAPI and create a list of data. This will get the data from all pages"""
#     data_list = []
#     for number in range(1,30):
#         response = requests.get(f'https://swapi.dev/api/{resource}/?page={number}')  # get star ships data
#         if response.status_code != 200: # error handling if page doesn't exist as will error code 404
#             continue
#         else:
#             data = response.json()  # put data into json format
#             filtered_data = data['results']  # filter data to get only results values
#             data_list.append(filtered_data)
#     print(data_list)
#     return data_list

#----------------------------------------------------------------------------------------------------------------------#
#--------------------------------------------------- ANSWER -----------------------------------------------------------#

# # Function to extract info from any resource (e.g. starships, people etc.)
# def API_scrape(resource):
#     """ Call on SWAPI and create a list of data. This will get the data from all pages"""
#     data_list = []
#     for number in range(1,80):
#         response = requests.get(f'https://swapi.dev/api/{resource}/{number}/')  # get star ships data for one ship
#         if response.status_code != 200: # error handling if page doesn't exist as will error code 404
#             continue
#         else:
#             data = response.json()  # put data into json format
#             data_list.append(data)  # add dictionary to the list of dictionaries
#     return data_list
#
# # Dump data onto a local json file for ease of access
# ship_list = API_scrape('starships')
# file = 'star_wars_api_data.json'
# with open(file, 'w') as f:
#     json.dump(ship_list, f)
#
# # Load data into python to be used
# file = 'star_wars_api_data.json'
# with open(file,'r') as f:
#     starships = json.load(f)
# #print(starships)
# # print(type(starships))
#
# # Testing data to get pilot url info
# urls = starships[4]['pilots'][0]
# print(urls)
# print(len(starships))
# print(len(starships[4]['pilots']))
#
# client = pymongo.MongoClient()
# db = client['starwars']
#
#
# for number in range(0,len(starships)):
#     for num in range(0,len(starships[number]['pilots'])):
#         pilots_url = starships[number]['pilots'][num]
#         pilot_info = requests.get(pilots_url).json()
#         starships[number]['pilots'][num] = pilot_info
#         pilot_name = pilot_info['name']
#         pilot_id = db.characters.find({'name': f'{pilot_name}'}, {'_id': 1})
#         for i in pilot_id:
#             starships[number]['pilots'][num]['_id'] = i["_id"]
#
# print(starships)
#
# def write_to_db(data,database,collection):
#     client = pymongo.MongoClient()
#     db = client.get_database(database)
#     db.Starships1.insert_many(data)
#
# write_to_db(starships,'starwars','Starships1')

#write_to_db(ss,'starwars','Starships1')
# client = pymongo.MongoClient()
# # print(client.list_database_names())
#
# db = client.get_database('starwars')
# # col = db.create_collection('Starships')
# print(db.list_collection_names())
#
# db.Starships.insert_many(starships)

#---------------------------------------------------------------------------------------------------------------------#
#-------------------------------------------------As a function-------------------------------------------------------#
def API_get_and_clean(resource: str):
    """ Call on SWAPI and create a list of data. This will get the data from all starships"""
    starships1 = []
    for number in range(1,80): # 80 is more tha enough for the starships collection
        response = requests.get(f'https://swapi.dev/api/{resource}/{number}/')  # get star ships data for one ship
        if response.status_code != 200: # error handling - if page doesn't exist, will return error code 404
            continue
        else:
            data = response.json()  # put data into json format
            starships1.append(data)  # add dictionary to the list of dictionaries

    """ Iterate through the starships1 list and for each ship, substitute the pilot url with pilot info including the 
    character ID for each pilot from the characters collection """
    client = pymongo.MongoClient()
    db = client['starwars']

    for number in range(0, len(starships1)): # iterate through all starships
        for num in range(0, len(starships1[number]['pilots'])): # iterate through for all pilots for that ship
            pilots_url = starships1[number]['pilots'][num] # obtain url for pilot
            pilot_info = requests.get(pilots_url).json() # get pilot info from the url and convert into json format
            starships1[number]['pilots'][num] = pilot_info # replace the pilot url with the pilot info in the
            pilot_name = pilot_info['name'] # identify pilot name
            pilot_id = db.characters.find({'name': f'{pilot_name}'}, {'_id': 1}) # query the characters database using the character name to get the pilot ID
            for i in pilot_id: # adds the pilot ID to the information for the pilot
              starships1[number]['pilots'][num]['_id'] = i["_id"]

    return starships1


def write_to_db2(data, database: str, collection: str):
    """ Function writes data to a collection in a given database"""
    client = pymongo.MongoClient() #
    db = client.get_database(database) # connect to the database
    col = db[collection] # connect to the collection. If collection doesn't exist, it is created
    return col.insert_many(data) # insert the list of documents using insert many function

ss = API_get_and_clean('starships')
write_to_db2(ss,'starwars','Starships2')
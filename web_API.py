import requests
import pandas as pd
import numpy as np
import datetime
import requests


def getCoreData(core_li):
    Block, ReusedCount, Serial, Outcome = [],[],[],[]
    Flights, GridFins, Reused, Legs, LandingPad = [],[],[],[],[]
    for core in core_li:
            if core['core'] != None:
                response = requests.get("https://api.spacexdata.com/v4/cores/"+core['core']).json()
                Block.append(response['block'])
                ReusedCount.append(response['reuse_count'])
                Serial.append(response['serial'])
            else:
                Block.append(None)
                ReusedCount.append(None)
                Serial.append(None)
            Outcome.append(str(core['landing_success'])+' '+str(core['landing_type']))
            Flights.append(core['flight'])
            GridFins.append(core['gridfins'])
            Reused.append(core['reused'])
            Legs.append(core['legs'])
            LandingPad.append(core['landpad'])
    return Block, ReusedCount, Serial, Outcome, Flights, GridFins, Reused, Legs, LandingPad

# Takes the dataset and uses the payloads column to call the API and append the data to the lists
def getPayloadData(load_li):
    PayloadMass, Orbit = [],[]
    for load in load_li:
        response = requests.get("https://api.spacexdata.com/v4/payloads/"+load).json()
        PayloadMass.append(response['mass_kg'])
        Orbit.append(response['orbit'])
    return PayloadMass, Orbit

# Takes the dataset and uses the launchpad column to call the API and append the data to the list
def getLaunchSite(lunchsite_li):
    Longitude, Latitude, LaunchSite = [],[],[]
    for x in lunchsite_li:
        response = requests.get("https://api.spacexdata.com/v4/launchpads/"+str(x)).json()
        Longitude.append(response['longitude'])
        Latitude.append(response['latitude'])
        LaunchSite.append(response['name'])
    return Longitude, Latitude, LaunchSite

# Takes the dataset and uses the rocket column to call the API and append the data to the list
def getBoosterVersion(booster_li):
    BoosterVersion = []
    for x in booster_li:
       if x:
        response = requests.get("https://api.spacexdata.com/v4/rockets/"+str(x)).json()
        BoosterVersion.append(response['name'])
    return BoosterVersion

''' Get data from API'''
def call_api(base_url):
   # get the data from API
    response = requests.get(base_url)

    # Use json_normalize meethod to convert the json result into a dataframe
    response_json = response.json()
    data = pd.json_normalize(response_json)

    # Feature selection and formating 
    data_ = feature_selection(data)

    return data_


''' Filter data got it throw API, keep only required feature and reformat other feature'''
def feature_selection(data):
    # keep only the features we want .
    data = data[['rocket', 'payloads', 'launchpad', 'cores', 'flight_number', 'date_utc']]    

    # We will remove rows with multiple cores because those are falcon rockets with 2 extra rocket boosters and rows that have multiple payloads in a single rocket.
    data = data[data['cores'].map(len)==1]
    data = data[data['payloads'].map(len)==1]

    # Since payloads and cores are lists of size 1 we will also extract the single value in the list and replace the feature.
    data['cores'] = data['cores'].map(lambda x : x[0])
    data['payloads'] = data['payloads'].map(lambda x : x[0])

    # We also want to convert the date_utc to a datetime datatype and then extracting the date leaving the time
    data['date'] = pd.to_datetime(data['date_utc']).dt.date

    # Using the date we will restrict the dates of the launches
    data = data[data['date'] <= datetime.date(2020, 11, 13)]
    return data 


''' Create dictionary that involve all list necassery '''
def create_global_vars():
    BoosterVersion, PayloadMass, Orbit, LaunchSite, flight_number, date = [],[],[],[],[],[]
    Outcome, Flights, GridFins, Reused, Longitude, Latitude = [],[],[],[],[],[]
    Legs, LandingPad, Block, ReusedCount, Serial= [],[],[],[],[]

    launch_dict = {'FlightNumber': flight_number,
    'Date': date, 'BoosterVersion':BoosterVersion, 'PayloadMass':PayloadMass,
    'Orbit':Orbit, 'LaunchSite':LaunchSite, 'Outcome':Outcome,
    'Flights':Flights, 'GridFins':GridFins, 'Reused':Reused, 'Legs':Legs, 
    'LandingPad':LandingPad, 'Block':Block, 'ReusedCount':ReusedCount, 
    'Serial':Serial, 'Longitude': Longitude, 'Latitude': Latitude}

    return launch_dict

''' Apply wrangling data  ''' 
def data_wrangle(data_lunch):
    # Filter dataframe to make it just include Falcon 9 data
    #data_lunch_falcon9 = data_lunch[data_lunch['BoosterVersion'] == 'Falcon 9']

    # Check and ipute missing values
    data_ = impute_missing_val(data_lunch)

    return data_


def impute_missing_val(data):
    for col in data.columns:
        sum_missing = data[col].isnull().sum()
        if sum_missing > 0:  # in case there missing value found in column
            data[col].replace( np.nan,data[col].mean(), inplace=True)
    
    return data


def main():
    # Base API URL 
    spacex_url="https://api.spacexdata.com/v4/launches/past"

    # Fetch data  from API
    data = call_api(spacex_url)

    # create dictionary for Lunch dictionary
    lunch_dict = create_global_vars()
    #lunch_dict = {'playload':[], 'orbit':[]}
    lunch_dict['FlightNumber'].append(list(data['flight_number']))
    lunch_dict['Date'].append(list(data['date_utc']))
    
    # Get playload & orbit data 
    payload_mass, orbit = getPayloadData(data['payloads'])
    lunch_dict['PayloadMass'].append(payload_mass)
    lunch_dict['orbit'] = orbit
    lunch_dict['playload'] = payload_mass
    df = pd.DataFrame({'playload':payload_mass, 'orbit':orbit})

    # Get Block, ReusedCount, Serial, Outcome, Flights ..... data.  
    block, reused_count, serial, outcome, flights, grid_fins, reused, legs, landingpad = getCoreData(data['cores'])
    lunch_dict['Block'].append(block), lunch_dict['ReusedCount'].append(reused_count), lunch_dict['Serial'].append(serial), 
    lunch_dict['Outcome'].append(outcome), lunch_dict['Flights'].append(flights), lunch_dict['GridFins'].append(grid_fins), 
    lunch_dict['Reused'].append(reused), lunch_dict['Legs'].append(legs), lunch_dict['LandingPad'].append(landingpad), 

    # Get Lunchsite data 
    longitude, latitude, lunchsite = getLaunchSite(data['launchpad'])
    lunch_dict['Longitude'].append(longitude)
    lunch_dict['Latitude'].append(latitude)
    lunch_dict['LaunchSite'].append(lunchsite)


    # Get Booster ver data
    booster = getBoosterVersion(data['rocket'])
    lunch_dict['BoosterVersion'].append(booster)

    # Convert to dataframe
    lunch_df = pd.DataFrame(lunch_dict)
    lunch_df.head()
    
    # Data Wrangling 
    lunche_data = data_wrangle(lunch_df)

    lunche_data.head() 

    # Export data to .csv file 
    lunche_data.to_csv('API_.csv', index= False)
        
if __name__ == '__main__':
  main()
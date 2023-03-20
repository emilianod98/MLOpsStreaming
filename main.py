#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#      ╔═╗╦═╗╔═╗ ╦╔═╗╔═╗╔╦╗  ╔╦╗╦  ╔═╗┌─┐┌─┐  ╔═╗╔╦╗╦═╗╔═╗╔═╗╔╦╗╦╔╗╔╔═╗
#      ╠═╝╠╦╝║ ║ ║║╣ ║   ║   ║║║║  ║ ║├─┘└─┐  ╚═╗ ║ ╠╦╝║╣ ╠═╣║║║║║║║║ ╦
#      ╩  ╩╚═╚═╝╚╝╚═╝╚═╝ ╩   ╩ ╩╩═╝╚═╝┴  └─┘  ╚═╝ ╩ ╩╚═╚═╝╩ ╩╩ ╩╩╝╚╝╚═╝
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


# Import the libraries.
import numpy as np
import pandas as pd
import pickle
import re
from fastapi import FastAPI


# I instantiate the module to call FastApi in a variable.
app = FastAPI()
# I create another variable to read the dataset I'm going to work with.
dfPlatform = pd.read_parquet('./StreamingFA.parquet')

#with open('trainingmodel.pkl', 'wb') as file:
#    model = pickle.load(file)


#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#      ┌──┐┬ ┬┌─┐┬─┐┬  ┌┬┐┌─┐  ┌─┐┌─┐┌─┐┌┬┐┌─┐┌─┐┬
#      │  ││ │├┤ ├┬┘│   │ │ │  ├┤ ├─┤└─┐ │ ├─┤├─┘│
#      └─\┘└─┘└─┘┴└─┴   ┴ └─┘  └  ┴ ┴└─┘ ┴ ┴ ┴┴  ┴
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                                                                              


#     FIRST QUERY:     GET_MAX_DURATION.


# Define a route to the API with the requested parameters.
@app.get('/get_max_duration/{year}/{platform}/{duration_type}')
# Define a function called get_max_duration that takes three optional arguments: 'year', 'platform', and 'duration_type'.
def get_max_duration(year: int = None, platform: str = None, duration_type: str = None):
    # Check if the "platform" variable is not "None".
    if platform != None:
        # Create a regular expression object (filterPlatform) using the compile() function of the Python "re" module,
        # which will look for the given platform in the "platform" column of the DataFrame. 
        # The "re.IGNORECASE" option is used to make the search case insensitive...
        filterPlatform = re.compile(platform, re.IGNORECASE)
        # Then, I use the "startswith()" method on the "platform" column of the DataFrame to select only the rows that start with the desired platform,
        # and assign the result to the "filterDb" variable.
        filterDb = dfPlatform.loc[dfPlatform['platform'].str.startswith(filterPlatform.pattern)]
    # If the "platform" variable is "None", the entire DataFrame is assigned to the "filterDb" variable.
    else:
        filterDb = dfPlatform
    # I perform the same steps for duration_type...
    if duration_type != None:
        filterDurationType = re.compile(duration_type, re.IGNORECASE)
        filterDb = filterDb.loc[filterDb['duration_type'].str.startswith(filterDurationType.pattern)]
    else:
        pass
    # Check if the variable 'year' is not 'None'.
    if year != None:
        # Select only the rows from the filtered DataFrame (filterDb) whose value in the 'release_year' column is equal to the supplied year (year),
        # and assign the result back to the variable "filterDb".
        filterDb = filterDb.loc[filterDb['release_year'] == year]
    # If the variable "year" is "None", no further operation is performed on "filterDb", and the code continues to the next step.
    else:
        pass
    # I sort the DataFrame "filterDb" by the 'duration_int' column, in descending order (ie longest to shortest duration), 
    # and assign the result to the 'resultSorted' variable.
    resultSorted = filterDb.sort_values(by='duration_int', ascending=False)
    # With the variable 'resultSorted' I tell it to return the title.
    return resultSorted.iloc[0][['title']]



#     SECOND QUERY:     GET_SCORE_COUNT.


# Define a route to the API with the requested parameters.
@app.get('/get_score_count//{year}/{platform}/{score}')
# Define a function called get_score_count that takes three arguments: 'year', 'platform', and 'score'.
def get_score_count(year: int, platform: str, score: float):
    # Create a variable called 'filterDb' that is equal to my dataset.
    filterDb = dfPlatform 
    # Check if the "platform" variable is not "None".   
    if platform is not None:
        # Develop a regular expression filterPlatform that ignores platform case. 
        filterPlatform = re.compile(platform, re.IGNORECASE)
        # This regular expression is then used to filter filterDb, keeping only the rows where the 'platform' column matches the regular expression filterPlatform.
        filterDb = filterDb.loc[filterDb['platform'].str.match(filterPlatform)]
    # The last two lines of code perform these steps:
    # 1. The value of the "platform" column starts with a string given in the "platform" variable.
    # 2. The value of the "average" column is greater than a numeric value given in the "score" variable.
    # 3. The value of the "release_year" column is equal to a numeric value given in the "year" variable.
    # The np.count_nonzero() function from the NumPy library counts the number of elements that are true 
    # in the boolean array resulting from applying the three conditions, and the result is stored in the 
    # resultCount variable. Finally, the function returns the value of resultCount.
    resultCount = np.count_nonzero(filterDb['platform'].str.startswith(platform) & (filterDb['average'] > score) & (filterDb['release_year'] == year))
    return resultCount



#     THIRD QUERY:     GET_COUNT_PLATFORM. 


# Define a route to the API with the requested parameters.
@app.get('/get_count_platform/{platform}')
# define a function called "get_count_platform" which takes a string of text "platform" as an argument.
def get_count_platform(platform: str):
    filterDb = dfPlatform
    # With the returned platform value, if it's not null, I tell it to do these steps:
    if platform is not None:
        # Create a regular expression object (filterPlatform) using the compile() function of the Python "re" module,
        # which will look for the given platform in the "platform" column of the DataFrame. 
        # The "re.IGNORECASE" option is used to make the search case insensitive...
        filterPlatform = re.compile(platform, re.IGNORECASE)
        # This regular expression is then used to filter filterDb, keeping only the rows where the 'platform' column matches the regular expression filterPlatform.
        filterDb = filterDb.loc[filterDb['platform'].str.match(filterPlatform)]
    # Finally, the function counts the number of records in "filterDb" whose column "platform" starts with the string "platform", using the NumPy function np.count_nonzero()
    # and the string operation .str.startswith(). The resulting count is stored in the "resultCountPlatform" variable, and the function returns that value.
    resultCountPlatform = np.count_nonzero(filterDb['platform'].str.startswith(platform))
    return resultCountPlatform



#     FOURTH QUERY:     GET_ACTOR. 


# Define a route to the API with the requested parameters.
@app.get("/get_actor/{platform}/{release_year}")
# define a function called "get_actor" that takes two arguments: "platform", which is a string indicating 
# the platform a movie or series was released on, and "release_year", which is an integer indicating the year launch.
def get_actor(platform: str, release_year: int):
    # First, the "dfPlatform" DataFrame is filtered to retain only those records where the "release_year" column is equal to the integer given in "release_year",
    # and the result is stored in the "filterDb" variable.
    filterDb = dfPlatform[dfPlatform['release_year'] == release_year]
    # Then, it checks if the "platform" variable is not null (None). 
    # If it is not null.the text string "platform" is converted to lower case and the DataFrame "filterDb" is filtered to retain only those records where the column "platform" 
    # begins with the text string given in "platform". If the "platform" variable is null, the original unfiltered "dfPlatform" DataFrame is used.
    if platform is not None:
        platform = platform.lower()
        filterDb = filterDb.loc[filterDb['platform'].str.lower().str.startswith(platform)]
    else:
        filterDb = dfPlatform
    # Next, a variable called "cast" is created that contains all the names of the actors that appear in the "cast" column of the DataFrame "filterDb". 
    # To create this string, you perform a series of string operations: split the string in the "cast" column for each comma (',') using the .str.split(',') 
    # method, "explode" " each record in the resulting list is placed on a separate row using the .explode() method, stripping whitespace at the beginning and end of each string 
    # using the .str.strip() method, and counting the occurrences of each actor name using the .value_counts() method.
    cast = filterDb['cast'].str.split(',').explode().str.strip().value_counts()
    # Extracts the index (actor names) of the "cast" string into a Python list using the .index.tolist() attribute.
    resultCast = cast.index.tolist()
    # If the "resultCast" list contains at least one actor name and the first element is equal to the text string "no data", the string "no data" is removed from the result list. 
    # If the "resultCast" list has more than one element after removing the "no data" string, the second element is assigned to the "resultCast" variable. 
    # If the "resultCast" list has only one item after removing the "no data" string or never had the "no data" string, the first item in the list is assigned to the "resultCast" variable.
    # If the "resultCast" list is empty, the "resultCast" variable is assigned the null value (None).
    if resultCast and resultCast[0] == 'no data':
        if len(resultCast) > 1:
            resultCast = resultCast[1]
        else:
            resultCast = None
    else:
        resultCast = resultCast[0] if resultCast else None
    # Finally, the function returns the value of the "resultCast" variable, which is the name of the actor who has appeared the most times in movies or series released on the 
    # platform and the given year.
    return resultCast


#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#      ┌──┐┬ ┬┌─┐┬─┐┬  ┌┬┐┌─┐  ┌┬┐┬
#      │  ││ │├┤ ├┬┘│   │ │ │  ││││
#      └─\┘└─┘└─┘┴└─┴   ┴ └─┘  ┴ ┴└─┘
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


'''

#     MACHINE LEARNING MODEL: GET_RECOMMENDED. 
@app.get("/get_recommended/{userId}/{id}")
def recommended(userId: int, id: str):
    prediction = model.predict(userId, id)
    if prediction.est >= 4:
        results = 'Is Totally Recommended!', prediction.est
    elif prediction.est >= 3 and prediction.est < 4:
        results = "Is Recommended!", prediction.est
    else:
        results = "It might not be to your liking!", prediction.est
    return results

'''
# Thanks for your time!






#   88888888ba,                             88              88                        88888888888                      88  88  88                                              ,ad8888ba,
#   88      `"8b                            ""              88                        88                               ""  88  ""                                             d8"'    `"8b                           ,d
#   88        `8b                                           88                        88                                   88                                                d8'                                     88
#   88         88  ,adPPYYba,  8b,dPPYba,   88   ,adPPYba,  88   ,adPPYba,            88aaaaa      88,dPYba,,adPYba,   88  88  88  ,adPPYYba,  8b,dPPYba,    ,adPPYba,       88             ,adPPYYba,  ,adPPYba,  MM88MMM   ,adPPYba,   8b,dPPYba,
#   88         88  ""     `Y8  88P'   `"8a  88  a8P_____88  88  a8P_____88            88"""""      88P'   "88"    "8a  88  88  88  ""     `Y8  88P'   `"8a  a8"     "8a      88      88888  ""     `Y8  I8[    ""    88     a8"     "8a  88P'   `"8a
#   88         8P  ,adPPPPP88  88       88  88  8PP"""""""  88  8PP"""""""   aaa      88           88      88      88  88  88  88  ,adPPPPP88  88       88  8b       d8      Y8,        88  ,adPPPPP88   `"Y8ba,     88     8b       d8  88       88
#   88      .a8P   88,    ,88  88       88  88  "8b,   ,aa  88  "8b,   ,aa   "88      88           88      88      88  88  88  88  88,    ,88  88       88  "8a,   ,a8"       Y8a.    .a88  88,    ,88  aa    ]8I    88,    "8a,   ,a8"  88       88  888
#   88888888Y"'    `"8bbdP"Y8  88       88  88   `"Ybbd8"'  88   `"Ybbd8"'   d8'      88888888888  88      88      88  88  88  88  `"8bbdP"Y8  88       88   `"YbbdP"'         `"Y88888P"   `"8bbdP"Y8  `"YbbdP"'    "Y888   `"YbbdP"'   88       88  888
#                                                                           8"
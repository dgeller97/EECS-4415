# @Author: David Geller 214404255 and Jaskaran Gahra 214314439
# @Course: EECS 4415
# @Assignment: 1
import sys
import pandas as pd

#cmdln inputs for file, cityName
file = sys.argv[1]
cityName = sys.argv[2]

#reads the csv file
df = pd.read_csv(file)

#the number of businesses in the city
def numOfBus(data):
    #calculates the # of businesses by counting the number of businesses in the city (using loc to find them)
    numOfBus = data.loc[data['city'] == cityName].city.count()
    print("numOfBus: " + str(numOfBus))

#the average of stars of a business in the city
def avgStars(data):
    #calculates the avg # of stars in the city by locating all the stars columns in the city and finding the mean
    avgStars = data.loc[data['city'] == cityName].stars.mean()
    print("avgStars: " + str(avgStars))

#the number of restaurants in the city
def numOfRestaurants(data):
    #calculates the number of restaurants by searching if categories contains 'Restaurant' for that city
    #then it counts the number of restaurants for that city 
    numOfRestaurants = data.loc[data['categories'].str.contains("Restaurant")] #quick method for seperating Restaurants
    numOfRestaurants =  numOfRestaurants.loc[numOfRestaurants['city'] == cityName].categories.count()
    print("numOfRestaurants: " + str(numOfRestaurants))

#the average number of stars of restaurants in the city
def avgStarsRestaurants(data):
    #Calculates by searching the same way as before and then calculating the mean of the stars column
    avgStarsRestaurants = data.loc[data['categories'].str.contains("Restaurant")]
    avgStarsRestaurants =  avgStarsRestaurants.loc[avgStarsRestaurants['city'] == cityName].stars.mean()
    print("avgStarsRestaurants: " + str(avgStarsRestaurants))

#the average number of reviews for all businesses in the city
def avgNumOfReviews(data):
    #Calculates by finding all riviews for the city and then finding the mean of the review_count column
    avgNumOfReviews = data.loc[data['city'] == cityName].review_count.mean()
    print("avgNumOfReviews: " + str(avgNumOfReviews))

#the average number of reviews for restaurants in the city
def avgNumOfReviewsBus(data):
    #Calculates by searching for 'Restaurants' in categories (again) and then takes the mean of the review_count column
    avgNumOfReviewsBus = data.loc[data['categories'].str.contains("Restaurant")]
    avgNumOfReviewsBus =  avgNumOfReviewsBus.loc[avgNumOfReviewsBus['city'] == cityName].review_count.mean()
    print("avgNumOfReviewsBus: " + str(avgNumOfReviewsBus))

#Here to run each method 
numOfBus(df)
avgStars(df)
numOfRestaurants(df)
avgStarsRestaurants(df)
avgNumOfReviews(df)
avgNumOfReviewsBus(df)

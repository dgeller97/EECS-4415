# @Author = Jaskaran Garha 214314439 and David Geller 214404255
# @Course: EECS 4415
# @Assignment: 1
import sys
import pandas as pd
import matplotlib.pyplot as plt


file = sys.argv[1]
city = sys.argv[2]

def plot_bar_chart(restaurants):
	ax = restaurants[['#restaurants']].plot(kind='bar', title='Frequency', legend=True, fontsize=12)
	ax.set_xlabel('Category', fontsize=16)
	ax.set_ylabel('#Restaurants', fontsize=16)
	plt.tight_layout()
	plt.show()

# Reference used: https://medium.com/@sureshssarda/pandas-splitting-exploding-a-column-into-multiple-rows-b1b1d59ea12e
def restaurantCategoryDist(restaurants):
        #Breaking up categories so that each row has one category. 
        #This is as known as plitting (Exploding) a column into multiple rows
        #Then we just groupby category.
        new_restaurants = pd.DataFrame(restaurants.categories.str.split(';').tolist(), index=restaurants.business_id).stack()
        new_restaurants = new_restaurants.reset_index([0, 'business_id'])
        new_restaurants.columns = ['#restaurants','category']
        #Gets rid of the categories Restaurant, Food and Nightlife since they don't seem to fit under restaurants
        new_restaurants = new_restaurants.loc[~new_restaurants['category'].str.contains('Restaurant|Food|Nightlife')]
        final = new_restaurants.groupby('category').count().sort_values('#restaurants',ascending=False)
        final_format = new_restaurants.groupby(['category'], as_index=False).count().sort_values('#restaurants',ascending=False)
        #Makes the whole dataframe of string data type and then joins the data using ':' to get the required format
        #Prints the result without the index showing (to get the desired form)
        final_format = final_format.applymap(str)
        final_format = final_format.apply(lambda x: ':'.join(x), axis=1)
        print(final_format.to_string(index=False))

        chartfinal = final[:10]
        plot_bar_chart(chartfinal)

def restaurantReviewDist(restaurants):
        new_restaurants = pd.DataFrame(restaurants.categories.str.split(';').tolist(), index=restaurants.business_id).stack()
        new_restaurants = new_restaurants.reset_index([0, 'business_id'])
        new_restaurants = restaurants.merge(new_restaurants)
        new_restaurants.rename(columns={new_restaurants.columns[-1]: "category"}, inplace = True)
        new_restaurants = new_restaurants.groupby(['category'], as_index=False).agg(
    	{
    	'categories':"count",
    	'review_count':[sum],
    	'stars':["mean"]
    	})#.sort_values('review_count', ascending=False, inplace=True)
        #new_restaurants.columns = ['category', '#restaurants', '']
        new_restaurants.columns = ['category','#restaurants', 'reviews', 'avg_stars']
        new_restaurants = new_restaurants.sort_values(['reviews'], ascending=False)
        new_restaurants = new_restaurants[['category', 'reviews', 'avg_stars']]
        #Gets rid of the categories Restaurant, Food and Nightlife since they don't seem to fit under restaurants
        new_restaurants = new_restaurants.loc[~new_restaurants['category'].str.contains('Restaurant|Food|Nightlife')]
        #Makes the whole dataframe of string data type and then joins the data using ':' to get the required format
        #Prints the result without the index showing (to get the desired form)
        new_restaurants = new_restaurants.applymap(str)
        new_restaurants = new_restaurants.apply(lambda x: ':'.join(x), axis=1)
        print(new_restaurants.to_string(index=False))
    





data = pd.read_csv(file)
#catcount = data['categories'].value_counts()
#y = catcount.filter(regex='Restaurants').to_frame()
data = data[data['city'].str.contains(city, na=False)]
data = data[data['categories'].str.contains('Restaurants')] 
restaurantCategoryDist(data)
print('----------------------------------------------------------------')
restaurantReviewDist(data)
#x = data[data['categories'].str.contains('Restaurants')]
#print(x['categories'])







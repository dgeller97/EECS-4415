# @Author: David Geller 214404255 and Jaskaran Gahra 214314439
# @Course: EECS 4415
# @Assignment: 1
import sys
import pandas as pd

#cmdln inputs for file
file = sys.argv[1]

#reads the csv file and seperates into columns by spaces
df = pd.read_csv(file, sep = " ", header = None)
#adds headers the the columns
df.columns = ["node1", "node2"]

#counts # of nodes by stacking the columns, dropping duplicates and counting
nodes = df.stack().drop_duplicates().count()
#counts number of edges by counting total # of pairs of nodes
edges = df.node1.count()

#determines the degree for each node
def nodeDegreeDist(data):
    #stacks the columns and then counts the # of times the same node appears to get frequency
    #ordered by descending frequency
    nodes = data.stack().value_counts().reset_index()
    #Makes the whole dataframe of string data type and then joins the data using ':' to get the required format
        #Prints the result without the index showing (to get the desired form)
    nodes = nodes.applymap(str)
    nodes = nodes.apply(lambda x: ':'.join(x), axis=1)
    print(nodes.to_string(index=False))

#determines the avg node degree for the graph
def avgNodeDegree(data):
     #same as in nodeDegreeDist 
     nodes = data.stack().value_counts().reset_index()
     nodes.columns = ["Nodes", "NodesDegree"]
     #calculates the mean for all the unique nodes to get the avgNodeDegree
     nodes = nodes.NodesDegree.mean()
     print("avgNodeDegree:" + str(nodes))

#print(df.to_string(index=False))
print("Nodes:" + str(nodes) + " Edges:" + str(edges))

#Here to run each method
nodeDegreeDist(df)
avgNodeDegree(df)

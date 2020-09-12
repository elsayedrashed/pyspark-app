"""
  Spark Utility component to support dataframes operations
"""

"""
* Performs the union between the two dataframes.
*
* @param df1 Left Dataframe to union on
* @param df2 Right Dataframe to union from
"""
def combineDataframes(df1, df2):
    return df1.unionByName(df2)



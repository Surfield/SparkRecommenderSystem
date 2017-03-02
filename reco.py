import sys
from contextlib import contextmanager
from pyspark import SparkConf, SparkContext
from math import sqrt
import csv


SPARK_MASTER='local[*]'
SPARK_APP_NAME='ClothSimilarities'
SPARK_EXECUTOR_MEMORY='500m'

@contextmanager
def spark_manager():
    conf = SparkConf().setMaster(SPARK_MASTER) \
                      .setAppName(SPARK_APP_NAME) \
                      .set("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
    spark_context = SparkContext(conf=conf)

    try:
        yield spark_context
    finally:
        spark_context.stop()

# r = {"responce": []}

def coder(x): return unicode(x.encode('utf-8'))

def splitter(line):
    reader = csv.reader([line])
    for fields in reader:
        return map(coder, fields)
    
def loadClothNames():
    clothNames = {}
    with open("/Users/Surf/reco/reco_data.csv") as f:
        reader = csv.reader(f)
        for fields in reader:
            clothNames[int(fields[8])] = str(fields[7]).decode('ascii', 'ignore')
    return clothNames

def makePairs((user, ratings)):
    (cloth1, rating1) = ratings[0]
    (cloth2, rating2) = ratings[1]
    return ((cloth1, cloth2), (rating1, rating2))

def filterDuplicates( (userID, ratings) ):
    (cloth1, rating1) = ratings[0]
    (cloth2, rating2) = ratings[1]
    return cloth1 < mcloth2

def computeCosineSimilarity(ratingPairs):
    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in ratingPairs:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))

    return (score, numPairs)

def recommend(id):
    # conf = SparkConf().setMaster("local[*]").setAppName("ClothSimilarities")
    # sc = SparkContext(conf = conf)

    print("\nLoading products names...")
    nameDict = loadClothNames()

    with spark_manager() as sc:
        data = sc.textFile("file:///Users/Surf/reco/reco_data.csv").cache()

        # Map ratings to key / value pairs: user ID => cloth ID, rating
        ratings = data.map(lambda l: l.split(',')).map(lambda l: (int(l[0]), (int(l[8]), 1)))

        # Emit every cloth rated together by the same user.
        # Self-join to find every combination.
        joinedRatings = ratings.join(ratings)

        # At this point our RDD consists of userID => ((clothID, rating), (clothID, rating))

        # Filter out duplicate pairs
        uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

        # Now key by (cloth1, cloth2) pairs.
        clothPairs = uniqueJoinedRatings.map(makePairs)

        # We now have (cloht1, cloth2) => (rating1, rating2)
        # Now collect all ratings for each cloth pair and compute similarity
        clothPairRatings = clothPairs.groupByKey()

        # We now have (cloth1, cloth2) = > (rating1, rating2), (rating1, rating2) ...
        # Can now compute similarities.
        clothPairSimilarities = clothPairRatings.mapValues(computeCosineSimilarity).cache()

        # Save the results if desired
        #clothPairSimilarities.sortByKey()
        #clothPairSimilarities.saveAsTextFile("cloth-sims")

        # Extract similarities for the cloth we care about that are "good".
        # if (len(sys.argv) > 1):
        r = {"responce": []}

        if id:

            scoreThreshold = 0.97
            coOccurenceThreshold = 1

            # clothID = int(sys.argv[1])
            clothID = int(id)

            # Filter for cloths with this sim that are "good" as defined by
            # our quality thresholds above
            filteredResults = clothPairSimilarities.filter(lambda((pair,sim)): \
                (pair[0] == clothID or pair[1] == clothID) \
                and sim[0] > scoreThreshold and sim[1] > coOccurenceThreshold)

            # Sort by quality score.
            results = filteredResults.map(lambda((pair,sim)): (sim, pair)).sortByKey(ascending = False).take(20)

            print("Top 20 similar products for " + nameDict[clothID])
            for result in results:
                (sim, pair) = result
                # Display the similarity result that isn't the cloth we're looking at
                similarClothID = pair[0]
                if (similarClothID == clothID):
                    similarClothID = pair[1]
                print(nameDict[similarClothID] +" "+str(similarClothID)+ "\tscore: " + str(sim[0]) + "\tstrength: " + str(sim[1]))
                r["responce"].append({"name" : nameDict[similarClothID], "id" :str(similarClothID), "score":  str(sim[0]), "strength": str(sim[1])})

        return r

def hey(id):
    return str(id)

# if __name__ == "__main__":
    

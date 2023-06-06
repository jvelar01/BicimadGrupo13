import json
from pyspark import SparkContext, SparkConf
import datetime
import sys
#import matplotlib.pyplot as plt




# Set station, district, and filters (also can be done with sys.argv)

FILES = [
    "/public/bicimad/201704_movements.json",
    "/public/bicimad/201705_movements.json",
    "/public/bicimad/201706_movements.json",
    "/public/bicimad/201707_movements.json",
    "/public/bicimad/201708_movements.json",
    "/public/bicimad/201709_movements.json",
    "/public/bicimad/201710_movements.json",
    "/public/bicimad/201711_movements.json",
    "/public/bicimad/201712_movements.json",
    "/public/bicimad/201801_movements.json",
    "/public/bicimad/201802_movements.json",
    "/public/bicimad/201803_movements.json",
    "/public/bicimad/201804_movements.json",
    "/public/bicimad/201805_movements.json",
    "/public/bicimad/201806_movements.json"]
  #  "/public_data/bicimad/201807_movements.json",
#    "/public_data/bicimad/201808_movements.json",
 #   "/public_data/bicimad/201809_movements.json",
  #  "/public_data/bicimad/201810_movements.json",
  #  "/public_data/bicimad/201811_movements.json",
  #  "/public_data/bicimad/201812_movements.json",
  #  "/public_data/bicimad/201901_movements.json",
   # "/public_data/bicimad/201902_movements.json",
   # "/public_data/bicimad/201903_movements.json",
   # "/public_data/bicimad/201804_movements.json",
    #"/public_data/bicimad/201905_movements.json",
    #"/public_data/bicimad/201906_movements.json"]
    
    
PERSONAL_FILES = [
    "/user/pabfer19/Bicimad/201704_Usage_Bicimad.json",
    "/user/pabfer19/Bicimad/201705_Usage_Bicimad.json",
    "/user/pabfer19/Bicimad/201706_Usage_Bicimad.json",
    "/user/pabfer19/Bicimad/201707_Usage_Bicimad.json",
    "/user/pabfer19/Bicimad/201708_Usage_Bicimad.json",
    "/user/pabfer19/Bicimad/201709_Usage_Bicimad.json",
    "/user/pabfer19/Bicimad/201710_Usage_Bicimad.json",
    "/user/pabfer19/Bicimad/201711_Usage_Bicimad.json",
    "/user/pabfer19/Bicimad/201712_Usage_Bicimad.json",
    "/user/pabfer19/Bicimad/201801_Usage_Bicimad.json",
    "/user/pabfer19/Bicimad/201802_Usage_Bicimad.json",
    "/user/pabfer19/Bicimad/201803_Usage_Bicimad.json",
    "/user/pabfer19/Bicimad/201804_Usage_Bicimad.json",
    "/user/pabfer19/Bicimad/201805_Usage_Bicimad.json",
    "/user/pabfer19/Bicimad/201806_Usage_Bicimad.json",
    "/user/pabfer19/Bicimad/201807_Usage_Bicimad.json",
    "/user/pabfer19/Bicimad/201808_Usage_Bicimad.json",
    "/user/pabfer19/Bicimad/201809_Usage_Bicimad.json",
    "/user/pabfer19/Bicimad/201810_Usage_Bicimad.json",
    "/user/pabfer19/Bicimad/201811_Usage_Bicimad.json",
    "/user/pabfer19/Bicimad/201812_Usage_Bicimad.json",
    "/user/pabfer19/Bicimad/201901_Usage_Bicimad.json",
    "/user/pabfer19/Bicimad/201902_Usage_Bicimad.json",
    "/user/pabfer19/Bicimad/201903_Usage_Bicimad.json",
    "/user/pabfer19/Bicimad/201904_Usage_Bicimad.json",
    "/user/pabfer19/Bicimad/201905_Usage_Bicimad.json",
    "/user/pabfer19/Bicimad/201906_Usage_Bicimad.json"
]

STATION = 'Spring'
DISTRICT = [i for i in range(64,92)]  # Retiro district
WANT_TO_FILTER_DISTRICT = True
WANT_TO_FILTER_STATION = False

# Function to extract information from each line
def line_info(line, year):
    data = json.loads(line)
    ageRange1 = data['ageRange']
    id1 = data['user_day_code']
    start = data['idunplug_station']
    end = data['idplug_station']
    day = data['unplug_hourTime']['$date']
    duration = data["travel_time"]
    user_type = data['user_type']
    return (ageRange1, id1, start, end, date_converter(day, year), duration, user_type)

# Date conversion
def date_converter(date, year):
    """Converts the date received from the BiciMad files.
    Returns a tuple with the date in tuple format.
    The tuple contains the weekday, the day of the month, the month, and the year."""
    
    month = int(date[5:7])
    day = int(date[8:10])
    week_day = datetime.datetime(year, month, day).weekday()
    return (week_day, day, month, year)

def day_converter(week_day):
    """Given a weekday from the datetime module, i.e., a number from 0 to 6, 
    converts the number to human-readable format."""
    
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    return days[week_day]

def season(date_tuple):
    """Given a tuple returned by date_converter, it returns the
    season for that date."""
    
    (name, day, month, year) = date_tuple
    if month in (4, 5) or (month == 3 and day >=20) or (month == 6 and day <=20):
        return 'Spring'
    elif month in (7, 8) or (month == 6 and day >=21) or (month == 9 and day <=21):
        return 'Summer'
    elif month in (10, 11) or (month == 9 and day >=22) or (month == 12 and day <=20):
        return 'Fall'
    else:
        return 'Winter'

# Functions related to filtering the information prior to analysis

def filter_by_season():
    return WANT_TO_FILTER_STATION

def filter_by_district():
    return WANT_TO_FILTER_DISTRICT
        
def is_in_district(x):
    return x in DISTRICT

def filter_district(rdd):
    return rdd.filter(lambda x: is_in_district(int(x[2])))

def filter_season(rdd):
    return rdd.filter(lambda x : season(x[4]) == STATION)

# Auxiliary filters for the main functions

# Age filters

def filter_by_age_start(rdd, age):
    """Given an age, returns an RDD where only the trips made by users of that age are included,
    with the start as key. Other functions in this section are analogous."""
    
    return rdd.filter(lambda x : x[0] == age).map(lambda x: (x[2], (x[1], x[3], x[4])))


def filter_by_age_end(rdd, age):
    # Returns the end as key for records of a given age
    rdd_age = rdd.filter(lambda x : x[0] == age).map(lambda x: (x[3], (x[1], x[2], x[4])))
    return rdd_age

# Functions to filter by user type
def filter_type_start(rdd, type):
    rdd_type = rdd.filter(lambda x : x[6] == type).map(lambda x: (x[2], (x[1], x[3], x[4])))
    return rdd_type

def filter_type_end(rdd, type):
    rdd_type = rdd.filter(lambda x : x[6] == type).map(lambda x: (x[3], (x[1], x[2], x[4])))
    return rdd_type

# Functions to filter by day of the week
def filter_day_start(rdd, day):
    rdd_day = rdd.filter(lambda x : x[4][0] == day).map(lambda x: (x[2], (x[1], x[3], x[4])))
    return rdd_day

def filter_day_end(rdd, day):
    rdd_day = rdd.filter(lambda x : x[4][0] == day).map(lambda x: (x[3], (x[1], x[2], x[4])))
    return rdd_day

# Function to return the station with the most departures for each day of the week
def spot_more_starts_per_day(rdd,f):
    rdd_day = [0] * 7
    for i in range(7):
        rdd_day[i] = filter_day_end(rdd, i).countByKey()
        max_station = max(rdd_day[i], key = rdd_day[i].get)
        f.write(f'The station with the most departures on {day_converter(i)} is {max_station} with {rdd_day[i][max_station]} departures.')

# Function to return the station with the most arrivals for each day of the week
def spot_more_ends_per_day(rdd,f):
    rdd_day = [0] * 7
    for i in range(7):
        rdd_day[i] = filter_day_start(rdd, i).countByKey()
        max_station = max(rdd_day[i], key = rdd_day[i].get)
        f.write(f'The station with the most arrivals on {day_converter(i)} is {max_station} with {rdd_day[i][max_station]} arrivals.')

# Function to return the station with the most departures for each user type 
def spot_more_starts_per_type(rdd,f):
    rdd_type = [0] * 4
    for i in range(4):
        rdd_type[i] = filter_type_start(rdd, i).countByKey()
        if len(list(rdd_type[i]))>1:
            max_station = max(rdd_type[i], key = rdd_type[i].get)
            f.write(f'The station with the most departures for user type {i} is {max_station} with {rdd_type[i][max_station]} departures.')
        else:
            f.write(f'No departures for user type {i}.')

# Function to return the station with the most arrivals for each user type
def spot_more_ends_per_type(rdd,f):
    rdd_type = [0] * 4
    for i in range(4):
        rdd_type[i] = filter_type_start(rdd, i).countByKey()
        if len(list(rdd_type[i]))>1:
            max_est = max(rdd_type[i], key = rdd_type[i].get)
            f.write(f'La estación en la que llegan más bicicletas para el tipo de usuario {i} es {max_est} y han salido {rdd_type[i][max_est]} bicis')
        else:
            f.write(f'para el tipo de usuario {i} no llegan bicicletas')
       
  
# Similar to the previous but for each age range
def spot_more_starts_per_age(rdd,f):
    rdd_age = [0] * 7
    for i in range(7):
        rdd_age[i] = filter_by_age_start(rdd, i).countByKey()
        if len(list(rdd_age[i]))>1:
            max_est = max(rdd_age[i], key = rdd_age[i].get)
            f.write(f'The station from which most bikes leave for the age group {i} is {max_est} and {rdd_age[i][max_est]} bikes have left')
        else:
            f.write(f'for the age range {i}, no bikes leave')

def spot_more_ends_per_age(rdd,f):
    rdd_age = [0] * 7
    for i in range(7):
        rdd_age[i] = filter_by_age_end(rdd, i).countByKey()
        if len(list(rdd_age[i]))>1:
            max_est = max(rdd_age[i], key = rdd_age[i].get)
            f.write(f'The station where most bikes arrive for the age group {i} is {max_est} and {rdd_age[i][max_est]} bikes have arrived')
        else:
            f.write(f'for the age range {i}, no bikes arrive')

# Function that prints how many trips are made by age
def trips_per_age(rdd,f): 
    rdd_counted_ages = rdd.countByKey()
    f.write('The age groups and the bicycles they use are:', '\n')
    for i in rdd_counted_ages:
        f.write(f'The age group {i} used {rdd_counted_ages[i]} bicycles')
    """
    # BAR CHART
    axes = [[0,1,2,3,4,5,6],[rdd_counted_ages[i] for i in range(7)]]hdfs://
    plt.bar(axes[0],axes[1])
    plt.ylabel('Number of users')
    plt.xlabel('Age Ranges')hdfs://public_data/bicimad/201704_movements.json
    plt.title('Number of users depending on age')
    plt.savefig('tripsagebar',format='png')        
    
    # PIE CHART
    labels = axes[0]
    sizes = axes[1]
    fig1, ax1 = plt.subplots()
    ax1.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
    ax1.axis('equal')  
    plt.savefig('tripsagepie',format='png')        
    """
    
# Function that prints the average time of each trip by age.
def time_per_age(rdd_base,f):
    rdd_duration= rdd_base.map(lambda x:(x[0],x[5])).groupByKey().map(lambda x : (sum(list(x[1]))/len(list(x[1])))).collect()
    f.write('The average time that bicycles are used according to the age range is:')
    for i in range(len(list(rdd_duration))):
        f.write(f'The age group {i} usprinted bicycles for an average of {rdd_duration[i]/60} minutes')
    
    """
    # BAR CHART
    axes = [[0,1,2,3,4,5,6],list(rdd_duration)]
    plt.bar(axes[0],axes[1])
    plt.ylabel('average user time')
    plt.xlabel('user age')
    plt.title('average user time depending on the user age')
    plt.savefig('timeage',format='png')        
    """
    
# Function that prints how many trips are made by user type
def trips_per_type(rdd,f): 
    rdd_counted_type = rdd.map(lambda x: (x[6],(x[0:5]))).countByKey()
    f.write('The types of users and the bicycles they use are:', '\n')  
    for i in rdd_counted_type:
        f.write(f'the user type {i} used {rdd_counted_type[i]} bicycles')    
    """
    # BAR CHART
    axes = [[1,2,3],[rdd_counted_type[i] for i in range(1,4)]]
    plt.bar(axes[0],axes[1])
    plt.ylabel('Number of users')
    plt.xlabel('types of users')
    plt.title('Number of users depending on user type')
    plt.savefig('tripstypesbar',format='png')        
    
    # PIE CHART
    labels = axes[0]
    sizes = axes[1]
    fig1, ax1 = plt.subplots()
    ax1.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
    ax1.axis('equal')  
    plt.savefig('tripstypepie',format='png')        
    """
    
# Function that prints the average time of each trip by user type
def time_per_type(rdd_base,f):
    rdd_duration= rdd_base.map(lambda x:(x[6],x[5])).groupByKey().map(lambda x : (sum(list(x[1]))/len(list(x[1])))).collect()
    f.write('The average time that bicycles are used according to the user type is:')
    for i in range(len(list(rdd_duration))):
        f.write(f'The user type {i+1} used bicycles for an average of {rdd_duration[i]/60} minutes')   
    
    # BAR CHART
    """
    axes = [[1,2,3],[rdd_duration[i] for i in range(3)]]
    plt.bar(axes[0],axes[1])
    plt.ylabel('average time of the user type')
    plt.xlabel('user type')
    plt.title('average user time depending on the user type')
    plt.savefig('durationtype',format='png')
    """

# Function that prints the number of trips made per month, in case of annual analysis
def trips_per_month(rdd,year,f):
    rdd_trips=rdd.map(lambda x: (x[4][2], (x[1], x[3], x[4]))).countByKey()
    if year == 2017:
        months = range(4,13)
    elif year == 2018:
        months = range(1,13)
    elif year == 2019:
        months = range(1,7)
        
    for i in months:
        f.write(f'in the month {i} of the year, {rdd_trips[i]} bicycles were used')    
    """
    axes = [[i for i in months],[rdd_trips[i] for i in months]]
    plt.bar(axes[0],axes[1])
    plt.ylabel('Number of trips')
    plt.xlabel('Month of the year')
    plt.title(f'Number of trips depending on the month {YEAR}')
    plt.savefig('tripsyear',format='png')
    """


def main(files, year):
    
    # Initialize Spark and load the JSON files
    conf = SparkConf().setAppName("Routes")
    # Abre un archivo de texto en modo escritura
    

    # Create SparkContext with our configuration
    with SparkContext(conf = conf) as sc:
        sc.setLogLevel("ERROR")  # Set log level to 'ERROR' to avoid cluttering output with log messages

        rdd_base = sc.emptyRDD()  # Initialize an empty RDD
        i=0  # Counter for files processed
        # Iterate over each file in the directory
        for filename in files:
            i+=1  # Increment the file counter
            # Process only JSON files
            if filename.endswith(".json"):
                print(f"Processing file: {filename}")  # Inform about the file being processed
                file_rdd = sc.textFile(f'hdfs://{filename}')  # Load the JSON file into an RDD
                rdd_base = rdd_base.union(file_rdd)  # Merge the loaded RDD with the base RDD
            else:
                pass  # If the file is not a JSON file, ignore it

        # Transform the loaded data
        rdd = rdd_base.map(lambda line: line_info(line, year))
        f = open('bicimad_results.txt', 'w')
        # Apply the district filter, if requested
        if filter_by_district():
            rdd = filter_district(rdd)  # Apply the district filter
            f.write('\n Analyzing district data\n')
        else:
            f.write('\n Analyzing Madrid data \n')  # Inform about the full data analysis
            
        # Apply the season filter, if requested
        if filter_by_season():
            f.write('The data will be filtered by season')
            rdd = filter_season(rdd)  # Apply the season filter
        else:
            f.write('\nThe data will not be filtered by season')  # Inform about not applying season filter
        
        # Start the analysis
        f.write(f'----------------------------------- Analysis {year} --------------------------------- \n\n')
        f.write('\nStatistics of bike usage by day\n')
        spot_more_starts_per_day(rdd,f)  # Determine the spots with the most starts per day
        f.write('\n')
        spot_more_ends_per_day(rdd,f)  # Determine the spots with the most ends per day

        f.write('\nStatistics of bike usage by user type\n')
        f.write('The types of users are:\n')   
        f.write('0: User type could not be determined \n') 
        f.write('1: Annual user (possessor of an annual pass) \n') 
        f.write('2: Occasional user \n') 
        f.write('3: Company worker \n')

        spot_more_starts_per_type(rdd,f)  # Determine the spots with the most starts per user type
        f.write('\n')
        spot_more_ends_per_type(rdd,f)  # Determine the spots with the most ends per user type

        f.write('\n Statistics of bike usage by age groups \n')
        f.write('The age groups are: \n')
        f.write('0: User age group could not be determined \n')
        f.write('1: User is between 0 and 16 years old \n')
        f.write('2: User is between 17 and 18 years old \n')
        f.write('3: User is between 19 and 26 years old \n')
        f.write('4: User is between 27 and 40 years old \n')
        f.write('5: User is between 41 and 65 years old \n')
        f.write('6: User is 66 years old or more \n')

        spot_more_starts_per_age(rdd,f)  # Determine the spots with the most starts per age group
        f.write('\n')
        spot_more_ends_per_age(rdd,f)  # Determine the spots with the most ends per age group

        f.write('\n Usage according to age \n')
        trips_per_age(rdd,f)  # Determine the number of trips per age group
        time_per_age(rdd,f)  # Determine the duration of trips per age group

        f.write('\n Statistics of bike usage by user type \n')
        trips_per_type(rdd,f)  # Determine the number of trips per user type
        time_per_type(rdd,f)  # Determine the duration of trips per user type

        # If yearly analysis is enabled, perform it

        f.write('Yearly user evolution')
        trips_per_month(rdd, year, f)  # Determine the number of trips per month
            
        f.close()
        
        sc.stop()  # Stop the SparkContext to free resources


if __name__ == "__main__":
    
    # Interact with the user for changing initial filters, if arguments provided.
    if len(sys.argv) > 2:
        if isinstance(sys.argv[1], bool):
            if sys.argv[1]:
                WANT_TO_FILTER_STATION=True
            else:
                WANT_TO_FILTER_STATION=False
                
        if isinstance(sys.argv[2], str):
            STATION=sys.argv[2]
            
        if isinstance(sys.argv[3], bool):
            if sys.argv[3]:
                WANT_TO_FILTER_DISTRICT=Trsc = SparkContext()
            else:
                WANT_TO_FILTER_DISTRICT=False
                
        if isinstance(sys.argv[4], list):
            DISTRICT=sys.argv[4]
    
    for year in [2017,2018,2019]:
        print(f'In the year {year} ')
        if year == 2017:
            files = PERSONAL_FILES[:9]
        elif year == 2018:
            files = PERSONAL_FILES[9:21]
        elif year == 2019:
            files = PERSONAL_FILES[21:] 
            
        main(files, year)  # Call the main function

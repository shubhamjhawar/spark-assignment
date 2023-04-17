# Pyspark-Assignment

## covid_data.py

```
def get_data():
    url = "https://covid-19-india2.p.rapidapi.com/details.php"
    headers = {
        "X-RapidAPI-Key": KEY,
        "X-RapidAPI-Host": "hostname"
    }
    response = requests.request("GET", url, headers=headers)
    return response

```


## app.py:

```
@app.route('/')#the home route displays a menu of all the information to gain
def home():
     return jsonify({ '/'  : "HOME",
                     '/show_api_data' : " SHOW API RETURNED DATA",
                     '/get_most_affected':" MOST AFFECTED STATE( total death/total covid cases)",
                     '/get_least_affected':" LEAST AFFECTED STATE ( total death/total covid cases)",
                     '/highest_covid_cases':" STATE WITH THE HIGHEST COVID CASES",
                     '/least_covid_cases':" STATE WITH THE LEAST COVID CASES",
                     '/total_cases':" TOTAL CASES",
                     '/handle_well':" STATE THAT HANDLED THE MOST COVID CASES EFFICIENTLY( total recovery/ total covid cases)",
                     '/least_well':" STATE THAT HANDLED THE MOST COVID CASES LEAST EFFICIENTLY( total recovery/ total covid cases)"
                     })
```


* This is a Flask route that allows the user to export the data in the Spark dataframe to a CSV file. The data is first repartitioned into one partition and then saved as a CSV file with a header at the specified path. The function returns a JSON response confirming that the results were successfully stored at the given path.


#### Question:2

##### 2.1
```
@app.route('/most_affected_state')
sql_query = """SELECT STATE,TOTAL/DEATH
    AS AFFECTED 
    FROM COVID ORDER BY AFFECTED DESC 
    LIMIT 1 """
    result = spark.sql(sql_query).collect()
    most_affected_state = result[0].__getitem__('STATE')
    return jsonify({'most_affected_state ': most_affected_state})
```

* This code defines an endpoint in the Flask app that returns the state with the highest death-to-covid ratio. The Spark DataFrame is sorted by the death-to-covid ratio in descending order, and the state with the highest ratio is selected and returned in a JSON format.


##### 2.2
```
@app.route('/least_affected_state')
 sql_query = """SELECT STATE,TOTAL/DEATH
    AS AFFECTED 
    FROM COVID ORDER BY AFFECTED 
    LIMIT 1 """
    result = spark.sql(sql_query).collect()
    least_affected_state = result[0].__getitem__('STATE')
    return jsonify({'least_affected_state ': least_affected_state})
```

* This code defines an endpoint '/least_affected_state' which returns a JSON response containing the name of the state with the lowest death-to-covid ratio. It does this by sorting the dataframe 'data' by the death-to-covid ratio in ascending order, selecting the state column from the first row (i.e., the state with the lowest ratio), and returning it as a JSON object.


##### 2.3
```
@app.route('/highest_covid_cases')
def get_highest_covid_cases():
 sql_query = """SELECT STATE,CONFIRM
        AS COVID_CASES
        FROM COVID ORDER BY CONFIRM DESC 
        LIMIT 1  """
    result = spark.sql(sql_query).collect()
    highest_covid_cases_state = result[0].__getitem__('STATE')
    return jsonify({'highest_covid_cases_state ': highest_covid_cases_state})
```

* This endpoint returns the state with the highest number of COVID cases in India. It sorts the data by the confirmed cases column in descending order, selects the state column, and returns the first row (which has the highest number of cases).


##### 2.4
```
@app.route('/least_covid_cases')
def least_covid_cases():
    sql_query = """SELECT STATE,CONFIRM
    AS COVID_CASES
    FROM COVID ORDER BY CONFIRM ASC 
    LIMIT 1  """
    result = spark.sql(sql_query).collect()
    print(result)
    least_covid_cases_state = result[0].__getitem__('STATE')
    return jsonify({'least_covid_cases_state ': least_covid_cases_state})
```

* This code defines a route for the Flask app to get the state with the lowest number of COVID-19 cases. It sorts the Spark dataframe by the number of confirmed cases and selects the state with the lowest value. The state is then returned as a JSON object.


##### 2.5
```
@app.route('/total_cases')
def total_cases():
    sql_query = """SELECT SUM(CONFIRM)
        AS TOTAL_CASES
        FROM COVID """
    result = spark.sql(sql_query).collect()
    total_cases = result[0].__getitem__('TOTAL_CASES')
    return jsonify({'total_cases ': total_cases})
```

* This endpoint returns the total number of COVID-19 cases in all Indian states combined. It does so by selecting the 'confirm' column from the cleaned Spark DataFrame, summing up all the values in this column and returning the result as a JSON object with the key 'Total Cases'.


##### 2.6
```
@app.route('/handle_well')
def handle_well():
    sql_query = """ SELECT STATE ,CURED/CONFIRM
        AS EFFICIENCY FROM COVID ORDER BY EFFICIENCY DESC LIMIT 1 """
    result = spark.sql(sql_query).collect()
    handle_well_state = result[0].__getitem__('STATE')
    efficacy = result[0].__getitem__('EFFICIENCY')
    return jsonify({'handle_well_state ': handle_well_state, 'efficiency ': efficacy})


```

* This function sorts the data by the ratio of total recoveries to total COVID cases, and returns the name of the state with the highest ratio. This state is considered to have handled the COVID situation most efficiently.


##### 2.7
```
@app.route('/least_well')
def least_well():
    sql_query = """ SELECT STATE ,CURED/CONFIRM
        AS EFFICIENCY FROM COVID ORDER BY EFFICIENCY ASC LIMIT 1 """
    result = spark.sql(sql_query).collect()
    least_well_state = result[0].__getitem__('STATE')
    efficacy = result[0].__getitem__('EFFICIENCY')
    return jsonify({'least_well_state ': least_well_state, 'efficiency ': efficacy})


```

* This function returns the state with the lowest efficiency in handling COVID-19, which is calculated as the ratio of total recoveries to total COVID-19 cases. The function sorts the data in ascending order of this ratio and selects the state with the lowest value. It then returns the name of that state in JSON format.

### Dataframe:


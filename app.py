from flask import Flask,jsonify, render_template
import requests
from pyspark.sql import SparkSession
from apikeys import data,hostname
app = Flask(__name__)
spark = SparkSession.builder.appName("covid").getOrCreate()

def create_df_from_csv():
    df_ass = spark.read.format("csv").load("coviddata.csv", inferSchema=True, header=True)
    df_ass.printSchema()
    return df_ass

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

@app.route('/show_api_data')
def show_data():
    url = "https://covid-19-india2.p.rapidapi.com/details.php"

    headers = {
        "X-RapidAPI-Key": data,
        "X-RapidAPI-Host": hostname
    }

    response = requests.request("GET", url, headers=headers)
    return jsonify(response.json())


@app.route('/get_most_affected')
def get_most_affected():
    df = create_df_from_csv()
    df.createOrReplaceTempView("covid")
    sql_query = """SELECT STATE,TOTAL/DEATH
    AS AFFECTED 
    FROM COVID ORDER BY AFFECTED DESC 
    LIMIT 1 """
    result = spark.sql(sql_query).collect()
    most_affected_state = result[0].__getitem__('STATE')
    return jsonify({'most_affected_state ': most_affected_state})

@app.route('/get_least_affected')
def get_least_affected():
    df = create_df_from_csv()
    df.createOrReplaceTempView("covid")
    sql_query = """SELECT STATE,TOTAL/DEATH
    AS AFFECTED 
    FROM COVID ORDER BY AFFECTED 
    LIMIT 1 """
    result = spark.sql(sql_query).collect()
    least_affected_state = result[0].__getitem__('STATE')
    return jsonify({'least_affected_state ': least_affected_state})
    

@app.route('/highest_covid_cases')
def highest_covid_cases():
    df = create_df_from_csv()
    df.createOrReplaceTempView("COVID")
    sql_query = """SELECT STATE,CONFIRM
        AS COVID_CASES
        FROM COVID ORDER BY CONFIRM DESC 
        LIMIT 1  """
    result = spark.sql(sql_query).collect()
    highest_covid_cases_state = result[0].__getitem__('STATE')
    return jsonify({'highest_covid_cases_state ': highest_covid_cases_state})

@app.route('/least_covid_cases')
def least_covid_cases():
    df = create_df_from_csv()
    df.createOrReplaceTempView("COVID")
    sql_query = """SELECT STATE,CONFIRM
    AS COVID_CASES
    FROM COVID ORDER BY CONFIRM ASC 
    LIMIT 1  """
    result = spark.sql(sql_query).collect()
    print(result)
    least_covid_cases_state = result[0].__getitem__('STATE')
    return jsonify({'least_covid_cases_state ': least_covid_cases_state})


@app.route('/total_cases')
def total_cases():
    df = create_df_from_csv()
    df.createOrReplaceTempView("COVID")
    sql_query = """SELECT SUM(CONFIRM)
        AS TOTAL_CASES
        FROM COVID """
    result = spark.sql(sql_query).collect()
    total_cases = result[0].__getitem__('TOTAL_CASES')
    return jsonify({'total_cases ': total_cases})

@app.route('/handle_well')
def handle_well():
    df = create_df_from_csv()
    df.createOrReplaceTempView("COVID")
    sql_query = """ SELECT STATE ,CURED/CONFIRM
        AS EFFICIENCY FROM COVID ORDER BY EFFICIENCY DESC LIMIT 1 """
    result = spark.sql(sql_query).collect()
    handle_well_state = result[0].__getitem__('STATE')
    efficacy = result[0].__getitem__('EFFICIENCY')
    return jsonify({'handle_well_state ': handle_well_state, 'efficiency ': efficacy})

@app.route('/least_well')
def least_well():
    df = create_df_from_csv()
    df.createOrReplaceTempView("COVID")
    sql_query = """ SELECT STATE ,CURED/CONFIRM
        AS EFFICIENCY FROM COVID ORDER BY EFFICIENCY ASC LIMIT 1 """
    result = spark.sql(sql_query).collect()
    least_well_state = result[0].__getitem__('STATE')
    efficacy = result[0].__getitem__('EFFICIENCY')
    return jsonify({'least_well_state ': least_well_state, 'efficiency ': efficacy})


if __name__ == '__main__':
    app.run(debug=True, port=8001)
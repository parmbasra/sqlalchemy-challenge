import numpy as np
import datetime as dt
from datetime import date
from dateutil.relativedelta import relativedelta


# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///../Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(autoload_with=engine)

# Save reference to the table
Measurement_tbl = Base.classes.measurement
Station_tbl = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes along-with the description:<br/>"
        f'Use this route to get the JSON for all the percipation data for whole year according to recent date: /api/v1.0/precipitation<br/>'
        f'Use this route to get the JSON for all the Stations: /api/v1.0/stations<br/>'
        f'Use this route to get the JSON for all the temperature for whole year according to recent date: /api/v1.0/tobs<br/>'
        f'Use this route to find min, max and avg temperature from certain date: /api/v1.0/YYYY-MM-DD<br/>'
        f'Use this route to find min, max and avg temperature between specific date(s): /api/v1.0/YYYY-MM-DD/YYYY-MM-DD<br/>'
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Jsonify precipitation data for one year"""
    # Get the most recent date from the dataset
    recent_date = session.query(Measurement_tbl.date).order_by(Measurement_tbl.date.desc()).first()

    # Get the 12 months date according to the recent date.
    last_12_months_ago_date = dt.datetime.strptime(recent_date[0],'%Y-%m-%d') - relativedelta(months=12)

    last_12_months_ago_date_format = last_12_months_ago_date.strftime('%Y-%m-%d')
    
    # Get the year long percipitaion record(s)
    year_long_precipitation = session.query(Measurement_tbl.date, Measurement_tbl.prcp).\
                                    filter(Measurement_tbl.date.between(last_12_months_ago_date_format,recent_date[0])).\
                                    order_by(Measurement_tbl.date).all()
       
    # Create a dictionary from the row data and append to a list of year_long_precipitation
    precipitation_list = []
    for date, prcp in year_long_precipitation:
        precipitation_dict = {}
        precipitation_dict["date"] = date
        precipitation_dict["prcp"] = prcp
        precipitation_list.append(precipitation_dict)

     
    session.close()

    return jsonify(precipitation_list)


@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Jsonify a list of the stations"""
    all_stations = session.query(Station_tbl.station).all()
    session.close()
    
    station_list = list(np.ravel(all_stations))

    return jsonify(station_list)


@app.route("/api/v1.0/tobs")
def tobs():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return the temperature list from the most active station"""

    recent_date = session.query(Measurement_tbl.date).order_by(Measurement_tbl.date.desc()).first()

    # Get the 12 months date according to the recent date.
    last_12_months_ago_date = dt.datetime.strptime(recent_date[0],'%Y-%m-%d') - relativedelta(months=12)

    last_12_months_ago_date_format = last_12_months_ago_date.strftime('%Y-%m-%d')


    most_active_stations = [Measurement_tbl.station, func.count(Measurement_tbl.id)]

    active_stations_list = session.query(*most_active_stations).group_by(Measurement_tbl.station).\
                            order_by(func.count(Measurement_tbl.id).desc()).all()
    
    most_active_station_id = active_stations_list[0][0]

    temperature_query_result = session.query(Measurement_tbl.date, Measurement_tbl.tobs).\
                                        filter(Measurement_tbl.date.between(last_12_months_ago_date_format,recent_date[0])).\
                                        filter(Measurement_tbl.station == most_active_station_id).all()
    
    session.close()
    
    temperature_list = []

    for date, tobs in temperature_query_result:
        temperature_dict = {}
        temperature_dict["date"] = date
        temperature_dict["tobs"] = tobs
        temperature_list.append(temperature_dict)


    return jsonify(temperature_list)


@app.route("/api/v1.0/<start>")
def start_date(start):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return the temperature data for the input date"""

    start_date_temp_list = []
    start_date_temp_dict = {}

    # Find the max tempertaure as per the date input
    max_temp = session.query(func.max(Measurement_tbl.tobs)).\
                            filter(Measurement_tbl.date == start).scalar()

    start_date_temp_dict["max temp"] = max_temp

    # Find the min tempertaure as per the date input
    min_temp = session.query(func.min(Measurement_tbl.tobs)).\
                            filter(Measurement_tbl.date == start).scalar()

    start_date_temp_dict["min temp"] = min_temp

    # Find the avg tempertaure as per the date input
    avg_temp = session.query(func.avg(Measurement_tbl.tobs)).\
                            filter(Measurement_tbl.date == start).scalar()

    start_date_temp_dict["avg temp"] = avg_temp

    session.close()
    
    start_date_temp_list.append(start_date_temp_dict)
     

    return jsonify(start_date_temp_list)

@app.route("/api/v1.0/<start>/<end>")
def start_end_date(start, end):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return the temperature data for the input date"""

    start_end_date_temp_list = []
    start_end_date_temp_dict = {}

    # Find the max tempertaure as per the start and end date input
    max_temp = session.query(func.max(Measurement_tbl.tobs)).\
                            filter(Measurement_tbl.date.between(start, end)).scalar()

    start_end_date_temp_dict["max temp"] = max_temp

    # Find the min tempertaure as per the start and end date input
    min_temp = session.query(func.min(Measurement_tbl.tobs)).\
                            filter(Measurement_tbl.date.between(start, end)).scalar()

    start_end_date_temp_dict["min temp"] = min_temp

    # Find the avg tempertaure as per the start and end date input
    avg_temp = session.query(func.avg(Measurement_tbl.tobs)).\
                            filter(Measurement_tbl.date.between(start, end)).scalar()

    start_end_date_temp_dict["avg temp"] = avg_temp

    session.close()
    
    start_end_date_temp_list.append(start_end_date_temp_dict)
     

    return jsonify(start_end_date_temp_list)
     

if __name__ == '__main__':
    app.run(debug=True)
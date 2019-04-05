import numpy as np
import pandas as pd
import datetime as dt
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, and_

from flask import Flask, jsonify

# Set up database
engine = create_engine("sqlite:///Resources/hawaii.sqlite")
Base = automap_base()
Base.prepare(engine, reflect=True)

# Prepare references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

### Flask setup
app = Flask(__name__)


### Flask routes

# Home page
# List all available routes
@app.route("/")
def welcome():
	return (
		f"Available routes:"
		f"<ul><li>/api/v1.0/precipitation</li>"
		f"<li>/api/v1.0/stations</li>"
		f"<li>/api/v1.0/tobs</li>"
		f"<li>/api/v1.0/<i>start</i></li>"
		f"<li>/api/v1.0/<i>start</i>/<i>end</i></li></ul>")



# Convert the query results to a dictionary using date as the key and prcp as the value
# Return the JSON representation of the dictionary
@app.route("/api/v1.0/precipitation")
def precipitation():
	# Create session link to the DB
	session = Session(engine)
	# Query the DB
	prcp_query = session.query(Measurement.date, Measurement.prcp)\
    	.order_by(Measurement.date)\
    	.all()
	# Convert to pandas dataframe
	prcp_df = pd.DataFrame(prcp_query).set_index('date').dropna()
	# Use GroupBy to convert to a dictionary of lists
	prcp_dict = prcp_df.groupby('date')['prcp'].apply(list).to_dict()
	session.close()
	return jsonify(prcp_dict)
	



# Return a JSON list of stations from the dataset
@app.route("/api/v1.0/stations")
def stations():
	# Create session link to the DB
	session = Session(engine)
	# Query the DB
	station_query = session.query(Measurement.station)\
		.distinct().all()
	# Flatten the list
	station_list = [s[0] for s in station_query]
	# Return as single-keyed dictionary
	station_dict = {'station': station_list}
	session.close()
	return jsonify(station_dict)




# Query for the dates and temperature observations from a year from the last datapoint
# Return a JSON list of Temperature OBServations (tobs) for the previous year
@app.route("/api/v1.0/tobs")
def tobs():
	# Create session link to the DB
	session = Session(engine)
	# Calculate final date
	last_date = session.query(func.max(Measurement.date)).scalar()
	last_date_dt = dt.date.fromisoformat(last_date)
	year_prior_dt = last_date_dt - dt.timedelta(days=365)
	year_prior = year_prior_dt.isoformat()

	# Perform a query to retrieve the dates and tobs
	tobs_query = session.query(Measurement.date, Measurement.tobs)\
		.filter(Measurement.date >= year_prior).all()
	# Convert to pandas df
	tobs_df = pd.DataFrame(tobs_query).set_index('date').dropna()
	# Use GroupBy to convert to a dictionary of lists
	tobs_dict = tobs_df.groupby('date')['tobs'].apply(list).to_dict()
	session.close()
	return jsonify(tobs_dict)



# Return a JSON list of the min/avg/max temps
# for all dates after <start>
@app.route("/api/v1.0/<start>")
def tobs_after(start):
	# Create session link to the DB
	session = Session(engine)
	start_tobs_query = session.query(Measurement.date, func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs))\
		.filter(Measurement.date >= start)\
    	.group_by(Measurement.date)\
    	.all()
	start_tobs_dict = {u[0] : {'min': u[1], 'avg': u[2], 'max': u[3]} for u in start_tobs_query}
	session.close()
	return jsonify(start_tobs_dict)



# Return a JSON list of the min/avg/max temps
# for all dates after <start> and until <end>
@app.route("/api/v1.0/<start>/<end>")
def tobs_between(start, end):
	# Create session link to the DB
	session = Session(engine)
	start_tobs_query = session.query(Measurement.date, func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs))\
		.filter(and_(Measurement.date >= start, Measurement.date <= end))\
    	.group_by(Measurement.date)\
    	.all()
	start_tobs_dict = {u[0] : {'min': u[1], 'avg': u[2], 'max': u[3]} for u in start_tobs_query}
	session.close()
	return jsonify(start_tobs_dict)



if __name__ == '__main__':
	app.run(debug=True)
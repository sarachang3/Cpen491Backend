import os

import pandas as pd
from flask import Flask, request, jsonify, Response,send_file
import h5pyd
from rex import NSRDBX, WindX


app = Flask(__name__)

solar2020_data = None
wind2020_data = None

assets_folder = os.path.dirname(__file__)

@app.route('/sample', methods=['GET'])
def getSample():
    from rex import WindX

    wtk_file = '/nrel/wtk/conus/wtk_conus_2014.h5'
    nwtc = (39.913561, -105.222422)
    print("before")
    with WindX(wtk_file, hsds=True) as f:
        print("after")
        nwtc_sam_vars = f.get_SAM_lat_lon(hub_height=100,lat_lon=nwtc)
        print(nwtc_sam_vars)

#take average of past 3 years

@app.route('/get_wind_data', methods=['GET'])
def get_wind_data():
    latitude = request.args.get('latitude')
    longitude = request.args.get('longitude')

    year = request.args.get('year')# not a parameter used  (past 3 year average)
    country = request.args.get('country')   # determine country with only latitudes

    current_working_directory = os.getcwd()
    wind_file = 'wind{}_file_{}_{}.csv'.format(year,latitude,longitude)
    wind_path = os.path.join(current_working_directory, wind_file)

    if os.path.isfile(wind_file):
        print("file already exist")

    else:

        if not all([latitude, longitude, year]):
            return jsonify({'error': 'Missing parameters'}), 400
            # Ensure latitude and longitude are of correct type
        try:
            latitude = float(latitude)
            longitude = float(longitude)
        except ValueError:
            return jsonify({'error': 'Invalid latitude or longitude'}), 400

        try:
            # Using rex's WindX class to access the NREL Wind data
            print("before")
            if country == "america":
                dataSet = '/nrel/wtk/conus/wtk_conus_{}.h5'.format(year)
            else:
                dataSet = '/nrel/wtk/canada/wtk_canada_{}.h5'.format(year)
            print(dataSet)

            location = (latitude, longitude)

            with WindX(dataSet, hsds=True) as f:
                # Use the get_SAM_lat_lon method to retrieve data for the specified location
                print("accessed WindToolkit")

                data = f.get_SAM_lat_lon(hub_height=100, lat_lon = location, out_path= wind_path)
                print("The CSV file will be saved in:", current_working_directory)

        except Exception as e:
            # Handle exceptions such as file  not found, HSDS service errors, etc.
            return jsonify({'error': str(e)}), 500

    # return send_file(wind_path, as_attachment=True, download_name=wind_file)
    #return as jason
   # data= pandas.read_csvfile
    return send_file(wind_path, as_attachment=False, mimetype='text/csv')


@app.route('/get_solar_data', methods=['GET'])
def get_solar_data():
    latitude = request.args.get('latitude')
    longitude = request.args.get('longitude')
    year = request.args.get('year')
    country = request.args.get('country')   # america or canada

    current_working_directory = os.getcwd()
    solar_file = 'solar{}_file_{}_{}.csv'.format(year,latitude,longitude)
    solar_path=os.path.join(current_working_directory, solar_file)

    if os.path.isfile(solar_file) :
        print("file already exist")

    else:

        # Check if all required parameters are provided
        if not all([latitude, longitude, year]):
            return jsonify({'error': 'Missing parameters'}), 400

        # Ensure latitude and longitude are of correct type
        try:
            latitude = float(latitude)
            longitude = float(longitude)
        except ValueError:
            return jsonify({'error': 'Invalid latitude or longitude'}), 400

        # Set up HSDS
        # You will need to configure h5pyd to use the HSDS service by setting the HSDS endpoint and your API keys
        # in a configuration file or environment variables.

        try:
            # Using rex's NSRDBX class to access the NREL NSRDB data
            print("before")

            with NSRDBX('/nrel/nsrdb/v3/tmy/nsrdb_tmy-{}.h5'.format(year), hsds=True) as f:
                # Use the get_SAM_lat_lon method to retrieve data for the specified location
                print("accessed NSRDB")
                data = f.get_SAM_lat_lon(lat_lon=(latitude, longitude),out_path=solar_path)

                # data.to_csv('solar{}_file.csv'.format(year))  # Save the DataFrame to a CSV file

                print("The CSV file will be saved in:", current_working_directory)

        except Exception as e:
            # Handle exceptions such as file not found, HSDS service errors, etc.
            return jsonify({'error': str(e)}), 500

    return send_file(solar_path, as_attachment=False, mimetype='text/csv')


@app.route('/')
def hello_world():
    hsds_config_path = os.getenv('HSDS_CONFIG')
    if hsds_config_path and os.path.exists(hsds_config_path):
        return jsonify({'message': 'HSDS config found!'})
    else:
        return jsonify({'error': 'HSDS config not found'}), 500
    return 'Hello, World!'

if __name__ == '__main__':
    app.run(debug=True)
    # print(solar2020_data.head(10))
    # print(wind2020_data.head(10))

#!/usr/bin/python3
import json
import time
import requests
from datetime import datetime, timedelta

# quick var setup 
ds_url = "https://api.darksky.net/forecast"
ds_token = "832a29d9f7de4f05e8de840e563be398"
num_days = 30
lat = 47.620403
lng = -122.349322 

# files and paths
file = 'dark_sky_backfill_' + str(lat) + '_' + str(lng) + '_' + str(num_days) + 'days.json'
f = open(file, "w+")


def main():
    # go back in time number of days
    for x in range(1, num_days + 1):
        # calculate date and convert to epoch
        date = datetime.today() - timedelta(days=x)
        date = date.strftime('%s')
        print('Day: ' + str(x) + ' Epoch: ' + date)

        try:
            # fetch the data using epoch
            weather_data = fetchDarkSkyAPI(date)

            # shape for splunk
            for item in weather_data['hourly']['data']:
                item['lat'] = lat
                item['lng'] = lng

                # pause for 1 second to avoid hitting rate limits
                # time.sleep(1)

                # finally, write this JSON event into disk
                f.write(json.dumps(item) + "\r\n")

        except Exception as e:
            print("There was an error backfilling the data. " + str(e))
            break


def fetchDarkSkyAPI(epoch):
    # this bit is a feeble attempt to fend off rate limiting!
    max_attempts = 10
    attempts = 0

    while attempts < max_attempts:
        # Make a request to Dark Sky API
        params = str(lat) + ',' + str(lng) + ',' + str(epoch) + '?exclude=currently,daily,flags'
        response = requests.get(ds_url + '/' + ds_token + '/' + params)

        if response.status_code < 400:
            # All is well

            json_response = response.json()
            return json_response

        elif response.status_code == 429:
            # We're being rate-limited

            print("HTTP 429 - Too Many Requests. Backing off and retying... Attempt #" + str(attempts))

            # wait and try again, slowing more each time
            time.sleep((2 ** attempts))
            attempts = attempts + 1

        else:
            # Something else went wrong

            print("HTTP {} - Aborting".format(response.status_code))
            raise ConnectionError("HTTP {} - Aborting".format(response.status_code))

    raise ConnectionRefusedError("Maximum attempts reached. Aborting.")


# grip it and rip it
if __name__ == "__main__":
    main()

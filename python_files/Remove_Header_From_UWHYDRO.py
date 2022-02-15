import os
import csv

# This script removes the header from the UWHYDRO files lines 1-38 (0-37) and overwrites file with no header data
# https://stackoverflow.com/questions/2987433/how-to-import-csv-file-data-into-a-postgresql-table

# ADD IN THE ID number for each row
time_frame = 'historic'
data_folder = 'F:/data/fluvial/nooksack/uw_hydro/historic/'
for file in os.listdir(data_folder):
    if file.endswith(".csv"):
        # Get Model Name
        to_pop = '-NOOFE-streamflow-1.0.csv'
        model_name = file.replace(to_pop, '')
        to_pop = 'historical_livneh_'
        model_name = model_name.replace(to_pop, '')
        # Open file and get contents
        with open(data_folder + file, 'r') as csvfile:
            content = csvfile.readlines()
            header = content[0:38]
            rows = content[39:]
            csvfile.close()
        # Write model name to list
        data = []
        for element in rows:
            element = element.replace('\n', '')
            element = element + ',' + model_name + ',' + time_frame + '\n'
            data.append(element)

            # rows[rows.index(element)] = element + ',' + model_name
        # Write new file with no header
        with open(data_folder + file, 'w') as newfile:
            header = 'date,streamflow,model,time_frame\n'
            newfile.writelines(header)
            newfile.writelines(data)

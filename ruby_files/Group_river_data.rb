# Models for each river 
models = ['CanESM2', 'CCSM4', 'CNRM-CM5', 'CSIRO-Mk3-6-0', 'GFDL-ESM2M','HadGEM2-CC', 'IPSL-CM5A-MR', 'inmcm4', 'MIROC5']
# RCP Scenarios
rcp = ['RCP45', 'RCP85']
# Hydrology models 
hydro_model = ['PRMS_P1', 'VIC_P1', 'VIC_P2', 'VIC_P3']
# Statistical downscaling method used 
downscale = ['BCSD', 'MACA']
# Rivers to analyze 
rivers = ['AUB', 'CED', 'CS2', 'DUNDU', 'ELWPO', 'LAG', 'NFSTI', 'NOOFE', 'PUY', 'SKAMO', 'SKOPO', 'SNOMO']


# Step 1. Get intermodel max, mean, min and std for each river across the hydro models 
river = rivers[0]
downscale = downscale[0]
rcp_scenario = rcp[1]
model = models[0]
hydro_model.each do |hydro|
	table_name = "#{river}_#{model}_#{rcp_scenario}_BCSD_#{hydro}"
	puts table_name
end


# Test case I will merge the AUB data into a single table 

            # table_name = "#aub_#{model}_#{rcp_scenario}_BCSD_#{hydro_scenario}"
            # puts table_name





# # Connect to PSQL database
# conn = PG.connect(
#     :host => "localhost",
#     :port => 5432, 
#     :dbname => "discharge", 
#     :user => "postgres", 
#     :password => "password"
#     )






# # Close connection
# conn.close
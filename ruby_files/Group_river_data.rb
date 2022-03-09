require 'pg'

class River 
	
	attr_accessor :name, :rcp, :hydro, :downscale, :gcm

	def initialize(name, rcp, hydro, downscale, gcm)
		@name = name
		@rcp = rcp
		@hydro = hydro
		@downscale = downscale
		@gcm = gcm
	end

	def db_table_name
		"#{name.downcase}_#{gcm.downcase}_#{rcp.downcase}_#{downscale.downcase}_#{hydro.downcase}"
	end
end


class ConnectPSQL
	
	attr_accessor :connection_status

	def initialize(args)
		@host = args.fetch(:host)
		@port = args.fetch(:port)
		@dbname = args.fetch(:dbname)
		@user = args.fetch(:user)
		@password = args.fetch(:password)
		@connection_status = args.fetch(:connection_status, 'closed')
	end

	def connect
		# Connect to PSQL database
		@connection_status = PG.connect(
			:host => @host,
			:port => @port, 
			:dbname => @dbname, 
			:user => @user,
			:password => @password
			)
	end

	def execute_query(query)
		@connection_status.exec(query)
	end

	def close
		@connection_status.close
	end
end


class WritePSQLMessage
	attr_accessor :message, :conection

	def initialize(args)
		@connection = args.fetch(:connection)
		@message = args.fetch(:message,'')
		@river = args.fetch(:river)
	end

	# Example
	def write_simple_print
		@message = "SELECT * FROM #{@river.db_table_name} LIMIT 10;"
	end

	# Write the join to average all the GCM runs for a single rcp/hydro/downscale scenario 
	def generate_hash_for_gcm_join(gcm_list)
		hash = {}
		alphabet = ('a'..'z').to_a 
		numbers = ('1'..'9').to_a
		arr = []
		
		# Make unique array of letters and numbers 
		alphabet.each do |letter|
			numbers.each do |num|
				arr << letter + num
			end
		end
		arr = arr.shuffle

		gcm_list.each do |gcm|
			key = arr.shift
			hash[key] = gcm
		end
		hash
	end

	def write_gcm_join(gcm_list)
		hash = generate_hash_for_gcm_join(gcm_list)
		# Make alias message for each gcm
		alias = []
		hash.each do |k, v|
			alias << "#{k}.date#{v}"
    end





		@message = "SELECT * FROM #{@river.db_table_name}_#{hash.keys.join} LIMIT 10;"
	end

	# # Example 
	# SELECT 
	# l.date,l.streamflow,r.date,r.streamflow
	# FROM t1 l 
	# INNER JOIN t2 r
	# on l.date = r.date

end











# Example River object
river = River.new('AUB', 'RCP85', 'VIC_P1', 'BCSD', 'CanESM2')

# Connect to PSQL database
db_connection = ConnectPSQL.new(
	:host => 'localhost', 
	:port => 5432,
	:dbname => 'discharge',
	:user => 'postgres',
	:password => 'password'
)
db_connection.connect

# Generate SQL query
query = WritePSQLMessage.new(
	:connection => db_connection,
	:river => river
)
query.write_simple_print
gcm_hash = query.generate_hash_for_gcm_join(['CanESM2', 'CCSM4', 'CNRM-CM5', 'CSIRO-Mk3-6-0', 'GFDL-ESM2M','HadGEM2-CC', 'IPSL-CM5A-MR', 'inmcm4', 'MIROC5'])
# Execute SQL query
# results = db_connection.execute_query(query.message)

# Close connection
db_connection.close

# puts results.to_a



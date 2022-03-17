class WritePSQLMessageJoin
	attr_accessor :message, :conection, :river

	def initialize(args)
		@connection = args.fetch(:connection)
		@message = args.fetch(:message,'')
		@river = args.fetch(:river)
	end

	# Create a hash relating the GCMS to a specific random alphanumeric alias for SQL query 
	def generate_hash_for_gcm_join_alias
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

		@river.gcm_list.each do |gcm|
			key = arr.shift
			hash[key] = gcm
		end
		hash
	end	

	# Write the alias text for SQL Query 
  def write_alias_message(hash)
    message = []
    table_hash = {}
    hash.each_with_index do |(k, v), ii|
			if ii < 1
				@message << "#{k}.date as date,#{k}.streamflow as sf#{ii},"
			elsif ii >=1 && ii < hash.length-1
				@message << "#{k}.streamflow as sf#{ii},"
			else 
				@message << "#{k}.streamflow as sf#{ii}"
			end
      table_hash[k] = "#{@river.name.downcase}_#{replace_hyphens(v)}_#{@river.rcp.downcase}_#{@river.downscale.downcase}_#{@river.hydro.downcase}"
    end
    return @message, table_hash
  end

	# Write SQL query header/SELECT statement to instance of message for join 
  def write_psql_head_message_join(header)
    @message = "CREATE TABLE temp AS\nSELECT\n#{header}\n"
  end	

	# Write the inner join text for SQL Query 
	def write_inner_join_message(hash)
		counter = 1
		message = []
		key_1 = hash.keys.first
		hash.each do |k, v|
			if counter == 1
				message << "FROM #{v} #{k}"
			else
				message << "INNER JOIN #{v} #{k}\nON #{key_1}.date = #{k}.date"
			end
			counter += 1
		end
		message.join("\n")
	end

	# Write join of all tables into single table of just river data by date with single date column 
	def write_gcm_join
		hash = generate_hash_for_gcm_join_alias # Generate a hash of unique letters and numbers for the GCMs
		message_1, table_hash = write_alias_message(hash) # Generate the message and a hash of the table names
		hash_length = table_hash.length

		# Write the header 
		write_psql_head_message_join(message_1)

		# Write the body of the query (INNER JOIN)
		@message += write_inner_join_message(table_hash)
		@message += ";"
	end

	# Helper method 
	def replace_hyphens(string)
		string.gsub('-', '_')
	end
end
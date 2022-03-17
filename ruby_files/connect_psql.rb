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
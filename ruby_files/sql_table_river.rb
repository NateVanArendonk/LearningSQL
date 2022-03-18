class SqlTableRiver

    attr_reader :river, :column_headers 

    def initialize(river, column_headers = [])
        @river = river
        @column_headers = column_headers
    end
    
    # Create name for table of averaged river discharges 
	def table_name
		"#{@river.name.downcase}_#{@river.rcp.downcase}_#{@river.downscale.downcase}_hydro_mean"		
	end
end
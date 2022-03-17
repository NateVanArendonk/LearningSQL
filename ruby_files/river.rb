class River 
	attr_accessor :name, :rcp, :hydro, :downscale, :gcm_list
	def initialize(name, rcp, hydro, downscale, gcm_list = [])
		@name = name
		@rcp = rcp
		@hydro = hydro
		@downscale = downscale
		@gcm_list = gcm_list
	end
end


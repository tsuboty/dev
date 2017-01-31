require "csv"


class TFDataMaker

	@header
	@data


	def initialize(input)
		@input = input
		@data = []
		@header = []
		@prism3_category_list = {}
		@nissan_category_list = {}
		self.file_read()
		self.nissan_category_read()
		self.prism3_read()
	end

	def file_read
		i=0
		CSV.foreach(@input,:encoding => 'windows-1256:utf-8') do |csv|
			if($.-1 == 0 ) then
				@header = csv
			else
				t_hash = {}
				csv.each_with_index do |c,id|
					t_hash[@header[id]] = c
				end
				@data[i] = t_hash
				i = i + 1
			end
		end
		puts "read end"
	end

	def prism3_read
		CSV.foreach("master/prism3_big_category.csv","r") do |line|
			@prism3_category_list[line[0]] = line[1]
		end
	end

	def nissan_category_read
		CSV.foreach("master/nissan_id_category_master.csv","r") do |line|
			@nissan_category_list[line[0]] = line[1]
		end
	end

	#prism3 file write
	def prism3_write(output_file)
		
		CSV.open(output_file,"w") do |line|
			line << ["tuuid","prism3"]

			for i in 0..@data.count-1
				prism3 = @data[i]["prism3_category_ids"].split("*")
				
				prism3.each do |val|
					category = prism3_category_list[val]
					line << [@data[i]["tuuid"],category]
				end
			end
		end 
	end

	#url category
	def urlcategory_write(output_file)
		CSV.open(output_file,"w") do |line|
			line << ["tuuid","category"]

			for i in 0..@data.count-1
				url_ids = @data[i]["url_category_ids"].split("*")
				url_ids.each do |val|
					url_category_name = nissan_category_list[val]
					line << [@data[i]["tuuid"],url_category_name]
				end

			end
		end 

	end


	#url and prism
	def url_prism3_category_write(output_file)
		CSV.open(output_file,"w") do |line|
			line << ["tuuid","category"] #header

			for i in 0..@data.count-1
				url_ids = @data[i]["url_category_ids"].split("*")
				
				#url_category(nissan)
				url_ids.each do |val|
					
					if val.to_i <= 220313 && val.to_i >= 220279 then
						url_category_name = @nissan_category_list[val]
						if url_category_name != nil then
							line << [@data[i]["tuuid"],url_category_name]
						end
					end
				end

				#prism3
				prism3_ids = @data[i]["prism3_category_ids"].split("*")
				prism3_ids.each do |val|
					category = @prism3_category_list[val]
					if category != nil then
						line << [@data[i]["tuuid"],category]
					end
				end

				str = (i*100/(@data.count-1)).to_s
				str = "-----#{str}-----"
				# puts str
				# printf "\e5D"
			end
		end
	end

	#demogra file write
	def demogra_write(output_file)
		CSV.open(output_file,"w") do |line|
			header = ["tuuid","request_uri","gender_age","income","marriage","occupation","frequency_of_ec_buying","amount_of_ec_buying","brand_vs_price_oriented","children_adult","children_university","children_high_school","children_middle_school","children_elementary_school","children_preschooler"]

			line << header
	
			for i in 0..@data.count-1
				array = []

				header.each_with_index do |val,index|
					array << @data[i][header[index]]
				end
				line << array

			end
		end
	end

end

tf = TFDataMaker.new("./src_data/nissan_category_data.csv")
tf.url_prism3_category_write("output/url_prism3.csv")
# tf.nissan_category_read()



# # tf.prism3_write("prism3.csv")
# # tf.demogra_write("demogra.csv")

# # tf.urlcategory_write("category.csv");

# tf.shashu_category("shashu.csv")





# CSV.open("output/nissan_id_category.csv","w") do |file|
# 	file << ["tuuid","category"]

# 	CSV.foreach("src_data/","r") do |line|
# 		if line[1].to_i <= 220313 && line[1].to_i >= 220279 then
# 			newline = [line[0],nissan_category_list[line[1]]]
# 			file << newline
# 		end
# 	end 

# end



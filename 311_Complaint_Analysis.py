#####################################################################################################################################
#
#Goal:
#Provides preliminary data analysis of 311 complaints made in New York City
#Categories: Illegal Parking Complaints per Month per Borough, Noise Complaints per Month, Street Conditions per Month, and Illegal parking per month
#
#How it works:
#Connects to a collection of 311 complaints hosted on an amazon bucket (expected format is CSV)
#Requires a mrjob.config file to allow clusters to be activated
#Private Key to the relevant AWS account is also required
#The sum of the counters will then provide the data required for each category
#
#####################################################################################################################################

from mrjob.job import MRJob
import csv
class MRWordFrequencyCount(MRJob):
	def mapper(self, _, line):
        	#yield "chars", len(line)
		#yield "words", len(line.split())
		#yield "lines", 1
		#if line.find("Noise - ") != -1:
			#yield "noise ", 1
		#if line.find("Street Condition") != -1:
			#yield "street condition", 1
		#if line.find("Illegal Parking") != -1:
			#yield "illegal parking", 1
		#if line.find("29609170") != -1:
			#yield "test", 1

#separating out relevant columns for analysis
		columns = line.split(",")
		seperated_date = columns[1].split("/")
		month = seperated_date[0]
		borough = columns[25]
################################################################################################################
#Total Complaints per borough per month
################################################################################################################

#find method returns '-1' if the string is not found
#if the String is found (and -1 is not returned), then the following executes

#checks that the complaint type is illegal parking, then checks the month, then checks the borough
		
		if line.find("Illegal Parking") != -1:
			if (month.find("01") != -1):
				if (borough.find("QUEENS") != -1):
					yield "Jan Queens IP", 1
				if (borough.find("BRONX") != -1):
					yield "Jan Bronxs IP", 1
				if (borough.find("MANHATTAN") != -1):
					yield "Jan Manhattan IP", 1
				if (borough.find("BROOKLYN") != -1):
					yield "Jan Brooklyn IP", 1
				if (borough.find("STATEN") != -1):
					yield "Jan Staten Island IP", 1
			if (month.find("02") != -1):
				if (borough.find("QUEENS") != -1):
					yield "Feb Queens IP", 1
				if (borough.find("BRONX") != -1):
					yield "Feb Bronxs IP", 1
				if (borough.find("MANHATTAN") != -1):
					yield "Feb Manhattan IP", 1
				if (borough.find("BROOKLYN") != -1):
					yield "Feb Brooklyn IP", 1
				if (borough.find("STATEN") != -1):
					yield "Feb Staten Island IP", 1
			if (month.find("03") != -1):
				if (borough.find("QUEENS") != -1):
					yield "Mar Queens IP", 1
				if (borough.find("BRONX") != -1):
					yield "Mar Bronxs IP", 1
				if (borough.find("MANHATTAN") != -1):
					yield "Mar Manhattan IP", 1
				if (borough.find("BROOKLYN") != -1):
					yield "Mar Brooklyn IP", 1
				if (borough.find("STATEN") != -1):
					yield "Mar Staten Island IP", 1
			if (month.find("04") != -1):
				if (borough.find("QUEENS") != -1):
					yield "Apr Queens IP", 1
				if (borough.find("BRONX") != -1):
					yield "Apr Bronxs IP", 1
				if (borough.find("MANHATTAN") != -1):
					yield "Apr Manhattan IP", 1
				if (borough.find("BROOKLYN") != -1):
					yield "Apr Brooklyn IP", 1
				if (borough.find("STATEN") != -1):
					yield "Apr Staten Island IP", 1
			if (month.find("05") != -1):
				if (borough.find("QUEENS") != -1):
					yield "May Queens IP", 1
				if (borough.find("BRONX") != -1):
					yield "May Bronxs IP", 1
				if (borough.find("MANHATTAN") != -1):
					yield "May Manhattan IP", 1
				if (borough.find("BROOKLYN") != -1):
					yield "May Brooklyn IP", 1
				if (borough.find("STATEN") != -1):
					yield "May Staten Island IP", 1
			if (month.find("06") != -1):
				if (borough.find("QUEENS") != -1):
					yield "Jun Queens IP", 1
				if (borough.find("BRONX") != -1):
					yield "Jun Bronxs IP", 1
				if (borough.find("MANHATTAN") != -1):
					yield "Jun Manhattan IP", 1
				if (borough.find("BROOKLYN") != -1):
					yield "Jun Brooklyn IP", 1
				if (borough.find("STATEN") != -1):
					yield "Jun Staten Island IP", 1
			if (month.find("07") != -1):
				if (borough.find("QUEENS") != -1):
					yield "Jul Queens IP", 1
				if (borough.find("BRONX") != -1):
					yield "Jul Bronxs IP", 1
				if (borough.find("MANHATTAN") != -1):
					yield "Jul Manhattan IP", 1
				if (borough.find("BROOKLYN") != -1):
					yield "Jul Brooklyn IP", 1
				if (borough.find("STATEN") != -1):
					yield "Jul Staten Island IP", 1
			if (month.find("08") != -1):
				if (borough.find("QUEENS") != -1):
					yield "Aug Queens IP", 1
				if (borough.find("BRONX") != -1):
					yield "Aug Bronxs IP", 1
				if (borough.find("MANHATTAN") != -1):
					yield "Aug Manhattan IP", 1
				if (borough.find("BROOKLYN") != -1):
					yield "Aug Brooklyn IP", 1
				if (borough.find("STATEN") != -1):
					yield "Aug Staten Island IP", 1
			if (month.find("09") != -1):
				if (borough.find("QUEENS") != -1):
					yield "Sep Queens IP", 1
				if (borough.find("BRONX") != -1):
					yield "Sep Bronxs IP", 1
				if (borough.find("MANHATTAN") != -1):
					yield "Sep Manhattan IP", 1
				if (borough.find("BROOKLYN") != -1):
					yield "Sep Brooklyn IP", 1
				if (borough.find("STATEN") != -1):
					yield "Sep Staten Island IP", 1
			if (month.find("10") != -1):
				if (borough.find("QUEENS") != -1):
					yield "Oct Queens IP", 1
				if (borough.find("BRONX") != -1):
					yield "OCt Bronxs IP", 1
				if (borough.find("MANHATTAN") != -1):
					yield "Oct Manhattan IP", 1
				if (borough.find("BROOKLYN") != -1):
					yield "Oct Brooklyn IP", 1
				if (borough.find("STATEN") != -1):
					yield "Oct Staten Island IP", 1
			if (month.find("11") != -1):
				if (borough.find("QUEENS") != -1):
					yield "Nov Queens IP", 1
				if (borough.find("BRONX") != -1):
					yield "Nov Bronxs IP", 1
				if (borough.find("MANHATTAN") != -1):
					yield "Nov Manhattan IP", 1
				if (borough.find("BROOKLYN") != -1):
					yield "Nov Brooklyn IP", 1
				if (borough.find("STATEN") != -1):
					yield "Nov Staten Island IP", 1
			if (month.find("12") != -1):
				if (borough.find("QUEENS") != -1):
					yield "Dec Queens IP", 1
				if (borough.find("BRONX") != -1):
					yield "Dec Bronxs IP", 1
				if (borough.find("MANHATTAN") != -1):
					yield "Dec Manhattan IP", 1
				if (borough.find("BROOKLYN") != -1):
					yield "Dec Brooklyn IP", 1
				if (borough.find("STATEN") != -1):
					yield "Dec Staten Island IP", 1

################################################################################################################
#Noise per month
################################################################################################################

#checks if the complaint type is a noise variant, then checks the month

		if ((line.find("Noise - ") != -1) and (month.find("01") != -1)):
			yield "Jan Noise", 1
		if ((line.find("Noise - ") != -1) and (month.find("02") != -1)):
			yield "Feb Noise", 1
		if ((line.find("Noise - ") != -1) and (month.find("03") != -1)):
			yield "Mar Noise", 1
		if ((line.find("Noise - ") != -1) and (month.find("04") != -1)):
			yield "Apr Noise", 1
		if ((line.find("Noise - ") != -1) and (month.find("05") != -1)):
			yield "May Noise", 1
		if ((line.find("Noise - ") != -1) and (month.find("06") != -1)):
			yield "Jun Noise", 1
		if ((line.find("Noise - ") != -1) and (month.find("07") != -1)):
			yield "Jul Noise", 1
		if ((line.find("Noise - ") != -1) and (month.find("08") != -1)):
			yield "Aug Noise", 1
		if ((line.find("Noise - ") != -1) and (month.find("09") != -1)):
			yield "Sep Noise", 1
		if ((line.find("Noise - ") != -1) and (month.find("10") != -1)):
			yield "Oct Noise", 1
		if ((line.find("Noise - ") != -1) and (month.find("11") != -1)):
			yield "Nov Noise", 1
		if ((line.find("Noise - ") != -1) and (month.find("12") != -1)):
			yield "Dec Noise", 1
################################################################################################################
#Street Condition per month
################################################################################################################

#checks if the complaint type is street condition, then checks the month

		if ((line.find("Street Condition") != -1) and (month.find("01") != -1)):
			yield "Jan SC", 1
		if ((line.find("Street Condition") != -1) and (month.find("02") != -1)):
			yield "Feb SC", 1
		if ((line.find("Street Condition") != -1) and (month.find("03") != -1)):
			yield "Mar SC", 1
		if ((line.find("Street Condition") != -1) and (month.find("04") != -1)):
			yield "Apr SC", 1
		if ((line.find("Street Condition") != -1) and (month.find("05") != -1)):
			yield "May SC", 1
		if ((line.find("Street Condition") != -1) and (month.find("06") != -1)):
			yield "Jun SC", 1
		if ((line.find("Street Condition") != -1) and (month.find("07") != -1)):
			yield "Jul SC", 1
		if ((line.find("Street Condition") != -1) and (month.find("08") != -1)):
			yield "Aug SC", 1
		if ((line.find("Street Condition") != -1) and (month.find("09") != -1)):
			yield "Sep SC", 1
		if ((line.find("Street Condition") != -1) and (month.find("10") != -1)):
			yield "Oct SC", 1
		if ((line.find("Street Condition") != -1) and (month.find("11") != -1)):
			yield "Nov SC", 1
		if ((line.find("Street Condition") != -1) and (month.find("12") != -1)):
			yield "Dec SC", 1
################################################################################################################
#Illegal Parking per month
################################################################################################################

#redundant, to make more efficient: make use of the noise complaint numbers (and add an additional counter for noise complaints not in any of the boroughs) from previous section
#keep in place to fulfill assignment requirements

		if ((line.find("Illegal Parking") != -1) and (month.find("01") != -1)):
			yield "Jan IP", 1
		if ((line.find("Illegal Parking") != -1) and (month.find("02") != -1)):
			yield "Feb IP", 1
		if ((line.find("Illegal Parking") != -1) and (month.find("03") != -1)):
			yield "Mar IP", 1
		if ((line.find("Illegal Parking") != -1) and (month.find("04") != -1)):
			yield "Apr IP", 1
		if ((line.find("Illegal Parking") != -1) and (month.find("05") != -1)):
			yield "May IP", 1
		if ((line.find("Illegal Parking") != -1) and (month.find("06") != -1)):
			yield "Jun IP", 1
		if ((line.find("Illegal Parking") != -1) and (month.find("07") != -1)):
			yield "Jul IP", 1
		if ((line.find("Illegal Parking") != -1) and (month.find("08") != -1)):
			yield "Aug IP", 1
		if ((line.find("Illegal Parking") != -1) and (month.find("09") != -1)):
			yield "Sep IP", 1
		if ((line.find("Illegal Parking") != -1) and (month.find("10") != -1)):
			yield "Oct IP", 1
		if ((line.find("Illegal Parking") != -1) and (month.find("11") != -1)):
			yield "Nov IP", 1
		if ((line.find("Illegal Parking") != -1) and (month.find("12") != -1)):
			yield "Dec IP", 1

	def reducer(self, key, values):
		yield key, sum(values)
if __name__ == '__main__':
	MRWordFrequencyCount.run()
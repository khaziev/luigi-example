__author__ = "Rinat Khaziev"
__copyright__ = "Copyright 2016"

import luigi
import requests
import pandas as pd
import datetime

class DownloadTaskDate(luigi.ExternalTask):
	'''
	Download data luigi task
	'''
	date = luigi.DateParameter(default=datetime.date.today())
	def run(self):
		url = 'https://data.cityofchicago.org/api/views/ydr8-5enu/rows.csv?accessType=DOWNLOAD'
		response = requests.get(url)
		self.output().makedirs()
		with self.output().open('w') as out_file:
			out_file.write(response.text)
			
	def output(self):
		return luigi.LocalTarget('data/permits-{0}.csv'.format(str(self.date)))
		
class CleanCsv(luigi.Task):

	'''
	Clean csv file
	'''
	
	def requires(self):
		return DownloadTaskDate(date=datetime.date.today())
		
	def output(self):
		return luigi.LocalTarget('data/permits_clean.csv')
		
	def run(self):
		
		#read data and remove trailing and preceding whitespaces
		df = pd.read_csv('data/permits-{0}.csv'.format(str(datetime.date.today())))
		df.columns = [x.strip() for x in df.columns]
		
		df = df.applymap(lambda x: str(x).replace('$', ''))
		
		with self.output().open('w') as out_file:
			df.to_csv(out_file, index = False)
		
class CountPermitTypes(luigi.Task):

	'''
	Count permits of each type
	'''
	
	def requires(self):
		return CleanCsv()
		
	def output(self):
		return luigi.LocalTarget('data/permit_counts.csv')
		
	def run(self):
		
		df = pd.read_csv('data/permits_clean.csv')
		counts = df['PERMIT_TYPE'].value_counts()
		with self.output().open('w') as out_file:
			counts.to_csv(out_file)
			
	
class PermitMeanPrice(luigi.Task):

	'''
	Calculate estimated permit price of each type
	'''
	
	def requires(self):
		return CleanCsv()
		
	def output(self):
		return luigi.LocalTarget('data/mean_permit_price.csv')
	
	def run(self):
		df = pd.read_csv('data/permits_clean.csv')
		mean_price = df.groupby('PERMIT_TYPE')['ESTIMATED_COST'].mean()
		mean_price.sort(inplace = True, ascending = False)
		with self.output().open('w') as out_file:
			mean_price.to_csv(out_file)
			
class PermitTopPrice(luigi.Task):

	'''
	Find top n most largest permit fees
	'''

	top_n = luigi.IntParameter()
	
	def requires(self):
		return CleanCsv()
		
	def output(self):
		return luigi.LocalTarget('data/top_10_permits.csv')
	
	def run(self):
		df = pd.read_csv('data/permits_clean.csv')

		#check if the dataframe is too small for a given top_n
		if(len(df) < self.top_n):
			self.top_n = len(df)

		df_sorted = df.sort('AMOUNT_PAID', ascending=False).head(self.top_n)
		with self.output().open('w') as out_file:
			df_sorted.to_csv(out_file)
   
class RunAll(luigi.Task):
    '''
    Execute all of the tasks
    '''

    def requires(self):
        return PermitTopPrice(top_n=10), PermitMeanPrice(), CountPermitTypes()
    
		



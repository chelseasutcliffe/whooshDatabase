#! /usr/bin/python

from flask import Flask, render_template, url_for, request
import whoosh
import csv
import pandas as pd
from whoosh.index import create_in
from whoosh.index import open_dir
from whoosh.fields import *
from whoosh.qparser import QueryParser
from whoosh.qparser import MultifieldParser
from whoosh import qparser

# Gobal variables
app = Flask(__name__)

# main route
@app.route('/', methods=['GET', 'POST'])
def index():
	print('Homepage load successful')
	return render_template('homepage.html')

# results route
@app.route('/results/', methods=['GET', 'POST'])
def results():
	global mysearch
	if request.method == 'POST':
		data = request.form
	else:
		data = request.args

	keywordquery = data.get('searchterm')
	limit = data.get('limit')
	conjunctive = data.get('conjunctive')

	print('Keyword Query is: ' + keywordquery + '; conjunctive: ' + conjunctive)

	titles, description = mysearch.search(keywordquery, limit, conjunctive)
	return render_template('results.html', query=keywordquery, results=zip(titles, description))

# index and search the database file
class MyWhooshSearch(object):
	"""docstring for MyWhooshSearch
		conjunction turned on specifies an and clause
		turned off specifies and or clause
	"""
	def __init__(self):
		super(MyWhooshSearch, self).__init__()

	# based on results from homepage form perform an AND/OR query
	def search(self, queryEntered, limit, conjunctive):
		""" search and return the results of the query passed from the user
			query entered: str(query recevied by the post request)
			limit: str(intager of number of results to be returned)
			conjunctive: str(True or False on if the query should be searched conjuncive or disjunctive)
		"""
		title = list()
		description = list()
		wordcount = list()
		with self.indexer.searcher() as search:
			if bool(conjunctive) == True:
				query = QueryParser('description', schema=self.indexer.schema)
			else:
				query = QueryParser('description', schema=self.indexer.schema, group=qparser.OrGroup)
			query = query.parse(queryEntered)
			results = search.search(query, limit=int(limit))

			for x in results:
				title.append(x['title'])
				description.append(x['description'])

		return title, description

	# add each row in database to index storing the ID, title and decription
	def index(self):
		""" index the csv file extracted from wikipedia	"""
		schema = Schema(id=ID(stored=True), title=TEXT(stored=True), description=TEXT(stored=True))
		indexer = create_in('index', schema)
		writer = indexer.writer()

		df = pd.read_csv('index/languages_spoken.csv', engine='python')

		for i in range(len(df)):
			writer.add_document(id=str(df.loc[i,'tokenid']), title=str(df.loc[i,'title']), description=str(df.loc[i,'extract']))

		print(' * Total Documents in index: ' + str(i))
		writer.commit()

		self.indexer = indexer

if __name__ == '__main__':
	global mysearch
	mysearch = MyWhooshSearch()
	mysearch.index()
	app.run(debug=True)
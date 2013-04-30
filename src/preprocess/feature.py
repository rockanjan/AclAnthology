import codecs
import sqlite3
import re
from math import log

def removeUnicode(s): 
	#return re.sub(r'[^\x20-\x7e]', 'UNICODE', s)
	result = re.sub(r'[^\x20-\x7e]', '', s)
	if result == '' : 
		result = "_NIL_"
	return result

def drop_tables(cur):
	cur.execute('drop table if exists paper')
	cur.execute('drop table if exists topic')
	cur.execute('drop table if exists author')
	cur.execute('drop table if exists paper_citation')
	cur.execute('drop table if exists citation_count_year')
	
def create_tables(cur):
	
	cur.execute('''create table paper (
	id TEXT,
	title TEXT,
	venue TEXT,
	year INTEGER,
	max_hindex INTEGER,
	author_count INTEGER,
	max_author_betweenness REAL,
	max_author_closeness REAL,
	max_author_degree REAL,
	max_author_avg_citation REAL
	)
	''')	
	#prob = topic prob distribution
	cur.execute('''create table topic (
	id TEXT,
	prob TEXT,
	diversity TEXT)
	''')
		
	cur.execute('''create table author (
	id TEXT,
	name TEXT,
	hindex TEXT,
	avg_citation_count REAL,
	betweenness REAL,
	closeness REAL,
	degree REAL
	)
	''')	
	
	cur.execute('''create table paper_citation(
	citer TEXT,
	cited TEXT)
	''')	
	#year = the year paper is considered for counting
	cur.execute('''create table citation_count_year(
	paper TEXT,
	published_year INTEGER,
	year INTEGER,
	count INTEGER)
	''')
	
	
def create_data_file(cur):
	print('create data files')
	duration = [1, 2, 5]
	datatype = ['train', 'test']
	fout = {}
	
	venue_filter_pattern = "(\(|\)|\s|:|[0-9]|-|&|#|;|,|\?|\')"
	#create header for ARFF format
	header = "@relation 'citation_count'\n"
	venues = cur.execute('select distinct venue from paper').fetchall()
	venue_list=[]
	for venue in venues:
		venuemod = venue[0]
		venuemod = re.sub(venue_filter_pattern, "", venuemod)
		venuemod = removeUnicode(venuemod)
		if len(venuemod) == 0:
			print('venue fixed to NIL in header')
			venuemod = "_NIL_"
		venue_list.append(venuemod)		
	venue_list_unique = list(set(venue_list)) #unique
	
	print('total venues = %d' %(len(venue_list_unique)))
	venues = "@attribute venue " + "{" + ", ".join(venue_list_unique) + "}"
	header = header + venues + "\n"
	header = header + "@attribute hindex real\n"
	header = header + "@attribute author_count real\n"
	topics = "@attribute topic"
	for i in range(0,100):
		header = header + topics + str(i) + " real\n"
	header = header + "@attribute diversity real\n"
	header = header + "@attribute recency real\n"
	header = header + "@attribute betweenness real\n"
	header = header + "@attribute closeness real\n"
	header = header + "@attribute degree real\n"
	header = header + "@attribute avg_citation real\n"
	header = header + "@attribute class real\n"
	header = header + "@data\n"
	
	#print(header)
	#exit(-1)
	#open files
	for year in duration:
		fout[year] = {}
		for t in datatype:
			fout[year][t] = open('/home/anjan/workspace/AclAnthology/year' + str(year) + "_" + t + ".arff", 'wb')
			fout[year][t].write( bytes(header, 'UTF-8') )
	
	debug_file = open('/home/anjan/workspace/AclAnthology/debug_all.dat', 'wb')
	#debug_file.write( bytes(header, 'UTF-8') )
	combined_file = open('/home/anjan/workspace/AclAnthology/combined_all.arff', 'wb')
	combined_file.write( bytes(header, 'UTF-8') )
	combined_csv = open('/home/anjan/workspace/AclAnthology/combined.csv', 'wb')
	rows = cur.execute('''select p.id, p.venue, p.year, p.max_hindex, p.author_count, t.prob, t.diversity, c.published_year, (c.year-c.published_year+1) as recency,
	p.max_author_betweenness, p.max_author_closeness, p.max_author_degree, p.max_author_avg_citation,
	c.count 
	from citation_count_year c join paper p on c.paper = p.id join topic t on p.id = t.id
	''').fetchall()
	
	iter_count = 1;
	for row in rows:
		if iter_count % 10000 == 0:
			print('processing record number : %d' %iter_count)
		paperid = row[0]
		venue = row[1]
		year = row[2]
		max_hindex = row[3]
		author_count = row[4]
		prob = row[5]
		diversity = row[6]
		published_year = row[7]
		recency = row[8]
		betweenness = row[9]
		closeness = row[10]
		degree = row[11]
		avg_citation = row[12]
		
		count = row[13]
		venuemod = venue
		venuemod = re.sub(venue_filter_pattern, "", venuemod)
		venuemod = removeUnicode(venuemod)
		#print(venuemod)
		if len(venuemod) == 0:
			print('venue fixed to NIL in data')
			venuemod = "_NIL_"
		debug_write_list = [paperid, venuemod, str(year), str(max_hindex), str(author_count), "\t".join(prob.split(',')), str(diversity), str(published_year), str(recency), str(betweenness), str(closeness), str(degree), str(avg_citation), str(count)]		
		debug_file.write( bytes("\t".join(debug_write_list) + "\n", 'UTF-8'))
		feature_write_list = [venuemod, str(max_hindex), str(author_count), "\t".join(prob.split(',')), str(diversity), str(recency), str(betweenness), str(closeness), str(degree), str(avg_citation), str(count)]
		combined_file.write(bytes("\t".join(feature_write_list) + "\n", 'UTF-8'))
		combined_write_list = [str(max_hindex), str(author_count), "\t".join(prob.split(',')), str(diversity), str(recency), str(betweenness), str(closeness), str(degree), str(avg_citation), str(count)]
		combined_csv.write(bytes("\t".join(combined_write_list) + "\n", 'UTF-8'))
		iter_count = iter_count + 1
		'''
		if iter_count > 10000:
			break
		'''
	
	debug_file.close()
	combined_file.close()
	combined_csv.close()
	#close files
	for year in duration:
		for t in datatype:
			fout[year][t].close()
	
	
def insert(cur, row):
	record = (row['id'], row['title'], row['venue'], row['year'], row['max_hindex'], row['author_count'], row['betweenness'], row['closeness'], row['degree'], row['avg_citation_count'])
	cur.execute("insert into paper values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", record)
	
def write_to_file(fout, row):
	#fout.write(bytes("\t".join([row['id'], row['venue'], row['year']], row['max_hindex']) + "\n", 'UTF-8'))
	fout.write(bytes("\t".join([row['id'], row['venue'], str(row['year']), str(row['max_hindex']), str(row['author_count'])]) + "\n", 'UTF-8'))
	
def readmeta(cur):
	'''reads aclmetadata file and populates the records into the table'''
	fmeta = codecs.open('/home/anjan/data/acl_anthology/aan/release/2012/acl-metadata.txt', encoding='Latin-1')
	fout = open('/home/anjan/data/acl_anthology/aan/features.txt', 'wb')
	complete = 0
	row = {}
	for line in fmeta:
		#print(line)
		if len(line.strip()) == 0:
			if complete:
				#insert the row into the table
				insert(cur, row)
				write_to_file(fout, row)
				row = {}
				complete = 0
			else:
				if len(row) != 0:
					print('error: not complete information')
				#else ignore (multiple white space lines)
		else:
			splitted = line.split('=')
			key = re.sub('[{}]', '', splitted[0]).strip()
			if len(splitted) == 2:
				value = re.sub('[{}]', '', splitted[1]).strip()
			else:
				value = "_NIL_"
			if key == 'id':
				row['id'] = value
			elif key == 'title':
				row['title'] = value
			elif key == 'venue':
				row['venue'] = value
			elif key == 'year':
				row['year'] = value
			elif key == 'author':
				authors = value.split(";")
				author_count = len(authors)
				if author_count == 0:
					author_count = 1
				max_hindex = 1
				small = 1e-100
				max_betweenness = small
				max_closeness = small
				max_degree = small
				max_avg_citation_count = 1
				for author in authors:
					author = author.lower()
					author = removeUnicode(author)
					author_info = cur.execute('select hindex, avg_citation_count, betweenness, closeness, degree from author where name = ?', [author]).fetchone()
					if author_info is None:
						max_hindex = 1 #default
						avg_citation_count = 1
						betweenness = small
						closeness = small
						degree = small
					else:
						author_hindex = author_info[0]
						avg_citation_count = author_info[1]
						betweenness = author_info[2]
						closeness = author_info[3]
						degree = author_info[4]
						if author_hindex == 'NA':
							author_hindex = 1 #default value							
						else:
							author_hindex = int(author_hindex)
						if author_hindex > max_hindex:
							max_hindex = author_hindex
						if avg_citation_count > max_avg_citation_count: max_avg_citation_count = avg_citation_count
						if betweenness > max_betweenness : max_betweenness = betweenness
						if closeness > max_closeness : max_closeness = closeness
						if degree > max_degree : max_degree = degree
						
				row['max_hindex'] = max_hindex
				row['author_count'] = author_count
				row['avg_citation_count'] = max_avg_citation_count
				row['betweenness'] = max_betweenness
				row['closeness'] = max_closeness
				row['degree'] = max_degree
			
			if len(row) == 10:
				complete = 1
	#if there is no final empty line, final record maynot have been inserted
	if len(row) != 0 and complete:
		insert(cur, row)
		write_to_file(fout, row)
	fmeta.close()
	fout.close()


def count_rows(cur, table):
	cur.execute("select count(*) from %s" %table)
	count = cur.fetchone()
	print("%s count = %d" %(table, count[0]))
	
def populate_topics(cur):
	flist = open('/home/anjan/workspace/AclAnthology/topics/filelist.dat')
	ftopicprob = open('/home/anjan/workspace/AclAnthology/topics/topic.exact.prob')
	ftopicprobout = open('/home/anjan/workspace/AclAnthology/topics/topic.prob.out', 'wb')
	titles=[]
	for title in flist:
		titles.append(re.sub(".txt\n", '', title))
	index = 0
	for prob in ftopicprob:
		#diversity = entropy of the topic distribution in document
		diversity = 0
		prob = re.sub("\n", '', prob)
		probs = prob.split(',')
		for p in probs:
			p = float(p)
			if p != 0:
				diversity = diversity - p * log(p)
			
		record = (titles[index], prob, diversity)
		cur.execute("insert into topic values (?, ?, ?)", record)
		ftopicprobout.write( bytes("%s\t%s\t%s\n" %record, 'UTF-8'))
		index = index + 1
	flist.close()
	ftopicprob.close()
	ftopicprobout.close()
	count_rows(cur, 'topic')
	
def populate_authors(cur):
	fauthorid = codecs.open('/home/anjan/data/acl_anthology/aan/release/2012/author_ids.txt', encoding='Latin-1')
	#fauthorid = open('/home/anjan/data/acl_anthology/aan/release/2012/author_ids.txt')
	fhindex = codecs.open('/home/anjan/data/acl_anthology/aan/release/2012/authors_hindex.txt', encoding = 'Latin-1')
	fbetweenness = codecs.open('/home/anjan/data/acl_anthology/aan/release/2009/author-citation-network.txt.betweenness-centrality', encoding = 'Latin-1')
	fcloseness = codecs.open('/home/anjan/data/acl_anthology/aan/release/2009/author-citation-network.txt.closeness-centrality', encoding = 'Latin-1')
	fdegree = codecs.open('/home/anjan/data/acl_anthology/aan/release/2009/author-citation-network.txt.degree-centrality', encoding = 'Latin-1')
	small = 1e-100
	
	betweenness_dict = {}
	for line in fbetweenness:
		line = line.strip()
		splitted = line.split(" ")
		if len(splitted) == 0: continue
		author = " ".join(splitted[0:-1]).strip().lower()
		author = removeUnicode(author)
		value = splitted[-1]
		try:
			value = float(value)
		except:
			continue
		if author in betweenness_dict:
			if betweenness_dict[author] < value:
				betweenness_dict[author] = value
		else:
			betweenness_dict[author] = value
	
	closeness_dict = {}
	for line in fcloseness:
		line = line.strip()
		splitted = line.split(" ")
		if len(splitted) == 0: continue
		author = " ".join(splitted[0:-1]).strip().lower()
		author = removeUnicode(author)
		value = splitted[-1]
		try:
			value = float(value)
		except:
			continue
		if author in closeness_dict:
			if closeness_dict[author] < value:
				closeness_dict[author] = value
		else:
			closeness_dict[author] = value
		
	degree_dict = {}
	for line in fdegree:
		line = line.strip()
		splitted = line.split(" ")
		if len(splitted) < 2: continue
		author = " ".join(splitted[0:-1]).strip().lower()
		author = removeUnicode(author)
		value = splitted[-1]
		try:
			value = float(value)
		except:
			continue
		if author in degree_dict:
			if degree_dict[author] < value:
				degree_dict[author] = value
		else:
			degree_dict[author] = value
	
	hindex_dict = {}
	for line in fhindex:
		splitted = line.split("\t")
		if len(splitted) == 2:
			hindex = splitted[0].strip()
			name = splitted[1].strip().lower()
			name = removeUnicode(name)
			if name in hindex_dict:
				print('multiple hindex for %s' %name)
				if hindex_dict[name] == 'NA':
					hindex_dict[name] = hindex
				elif int(hindex) > int(hindex_dict[name]):
					hindex_dict[name] = hindex
			else:
				hindex_dict[name] = hindex
				
	#compute average citation count
	fauthorcitation = open('/home/anjan/data/acl_anthology/aan/release/2012/author_citations.txt')
	author_total_citation_count={}
	for line in fauthorcitation:
		line = line.strip()
		splitted = line.split("\t")
		if len(splitted) < 2 : continue
		authorid = splitted[1].strip().lower()
		authorid = removeUnicode(authorid)
		count = splitted[0]
		try:
			count = int(count)
		except:
			continue
		author_total_citation_count[authorid] = count
	#count number of papers each author has
	fmeta = codecs.open('/home/anjan/data/acl_anthology/aan/release/2012/acl-metadata.txt', encoding='Latin-1')
	author_paper_count = {}
	for line in fmeta:
		if len(line.strip()) == 0:
			continue
		else:
			splitted = line.split('=')
			key = re.sub('[{}]', '', splitted[0]).strip()
			if len(splitted) == 2:
				value = re.sub('[{}]', '', splitted[1]).strip()
			else:
				value = "_NIL_"
			if key == 'author':
				authors = value.split(";")
				for authorid in authors:
					authorid = authorid.strip().lower()
					if authorid in author_paper_count:
						author_paper_count[authorid] = author_paper_count[authorid] + 1
					else:
						author_paper_count[authorid] = 1
	#compute the average
	author_avg_citation_count = {}
	for authorid, total_citation in author_total_citation_count.items():
		if authorid in author_paper_count:
			if author_paper_count[authorid] is not 0:
				author_avg_citation_count[authorid] = 1.0 * total_citation / author_paper_count[authorid]
			else:
				#assume only one paper written
				author_avg_citation_count[authorid] = 1.0 * total_citation
			if author_avg_citation_count[authorid] == 0: author_avg_citation_count[authorid] = 1
	
	duplicate_count = 0
	author_dict = {}
	for line in fauthorid:
		splitted = line.split("\t")
		if len(splitted) == 2:
			author_id = splitted[0].strip()
			name = splitted[1].strip().lower()
			if name in author_dict:
				#print('author is duplicate %s' %name)
				duplicate_count = duplicate_count + 1
			else:
				author_dict[name] = author_id
		else:
			print('could not parse line in authorid : %s' %line)
	
	fout = open('/home/anjan/data/acl_anthology/aan/authors_table.txt', 'wb')
	na_count = 0
	na_betweenness = 0
	na_closeness = 0
	na_degree = 0
	na_avg = 0
	for name, author_id in author_dict.items():
		if name in hindex_dict:
			hindex = hindex_dict[name]
		else:
			hindex = 'NA'
			#print('hindex not available for author : %s' %name)
			na_count = na_count + 1
		if name in betweenness_dict:
			betweenness = betweenness_dict[name]
		else:
			betweenness = small
			na_betweenness = na_betweenness + 1
		if name in closeness_dict:
			closeness = closeness_dict[name]						
		else:
			closeness = small
			na_closeness = na_closeness + 1
		if name in degree_dict:
			degree = degree_dict[name]
		else:
			degree = small
			na_degree = na_degree + 1
		if name in author_avg_citation_count:
			avg_citation_count = author_avg_citation_count[name]
		else:
			avg_citation_count = 1 #default
			na_avg = na_avg + 1
		
		record = (author_id, name, hindex, str(avg_citation_count), str(betweenness), str(closeness), str(degree))
		cur.execute("insert into author values (?, ?, ?, ?, ?, ?, ?)", record)
		fout.write( bytes("%s\t%s\t%s\t%s\t%s\t%s\t%s\n" %record, 'UTF-8'))
	print('author NA count (hindex %d, bet %f, close %f, degree %f, avg %f )' %(na_count, na_betweenness, na_closeness, na_degree, na_avg))
	print('author duplicate count : %d' %duplicate_count)
	fauthorid.close()
	fhindex.close()
	fout.close()
	fbetweenness.close()
	fcloseness.close()
	fdegree.close()
	count_rows(cur, 'author')

def populate_citations(cur):
	paper_citation = open('/home/anjan/data/acl_anthology/aan/release/2012/acl.txt')
	for line in paper_citation:
		splitted = line.split('==>')
		if len(splitted) == 2:
			citer = splitted[0].strip()
			cited = splitted[1].strip()
			record = (citer, cited)
			cur.execute('insert into paper_citation values (?, ?)', record)
		else:
			print('could not parse citation line %s' %line)
	count_rows(cur, 'paper_citation')
	
def populate_citation_year(cur):
	'''populates citation count for paper for each year'''
	cur.execute('select year from paper')
	years = cur.fetchall()
	min_year = 10000
	max_year = -1
	print('number of papers : %d' %(len(years)))
	for year in years:
		if int(year[0]) < min_year:
			min_year = int(year[0])
		if int(year[0]) > max_year:
			max_year = int(year[0])
	print('minyear = %d maxyear = %d' %(min_year, max_year))
	
	'''for each year, for each paper, 
	count the number of citations it got upto that year
	include only the papers published upto that time
	'''
	fout = open('/home/anjan/data/acl_anthology/aan/citation_count.txt', 'wb')
	start_year = max_year - 5
	min_year = 2000 #only consider papers after this year
	print('Considering papers published only after %d' %min_year)
	for current_year in range(start_year, max_year+1):
		print('processing citation count for year : %d' %current_year)
		papers = cur.execute('select id,year from paper where year <= ? and year >= ?', [current_year, min_year]);
		data = papers.fetchall()
		for row in data:
			paper_id = row[0]
			published_year = row[1]
			db_input = [current_year, paper_id]
			citers = cur.execute('''select citer from paper_citation join paper
			on paper_citation.citer = paper.id 
			where paper.year <= ? and cited = ?''', db_input).fetchall()
			citer_count = len(citers)
			db_input = [paper_id, str(published_year), str(current_year), str(citer_count)]
			cur.execute('insert into citation_count_year values (?,?,?,?)', db_input)
			fout.write(bytes("\t".join(db_input) + "\n", 'UTF-8'))
	fout.close()
	
#author rank = rank based on average citation count of the author (count of citations / total papers written)
 
					
														
def main():
	db = sqlite3.connect('/home/anjan/data/acl_anthology/aan/papers.db')
	cur = db.cursor()
	'''
	drop_tables(cur)
	create_tables(cur)
	populate_authors(cur)
	readmeta(cur)
	count_rows(cur, 'paper')
	populate_topics(cur)
	populate_citations(cur)	
	populate_citation_year(cur) #also creates the data/feature files
	db.commit()
	'''
	create_data_file(cur)
	cur.close()
	db.close()

	
if __name__ == "__main__":
	main()

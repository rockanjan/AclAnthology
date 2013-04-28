import codecs
import sqlite3
import re
from math import log

def removeUnicode(s): 
	return re.sub(r'[^\x20-\x7e]', 'UNICODE', s)

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
	author_count INTEGER
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
	hindex TEXT)
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
	fout1_train = open('/home/anjan/workspace/AclAnthology/year1_train.dat')
	fout1_test = open('/home/anjan/workspace/AclAnthology/year1_test.dat')
	fout2_train = open('/home/anjan/workspace/AclAnthology/year2_train.dat')
	fout2_test = open('/home/anjan/workspace/AclAnthology/year2_test.dat')
	fout5_train = open('/home/anjan/workspace/AclAnthology/year5_train.dat')
	fout5_test = open('/home/anjan/workspace/AclAnthology/year5_test.dat')
	duration = [1, 2, 5]
	rows = cur.execute('select p.id from citation_count_year c join paper p on c.paper = p.id join topic t on p.id = t.id').fetchall()
	
	
	
	
def insert(cur, row):
	record = (row['id'], row['title'], row['venue'], row['year'], row['max_hindex'], row['author_count'])
	cur.execute("insert into paper values (?, ?, ?, ?, ?, ?)", record)
	
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
				max_hindex = -1
				for author in authors:
					author_hindex = cur.execute('select hindex from author where name = ?', [author]).fetchone()
					if author_hindex is None:
						max_hindex = 1 #default
					else:
						author_hindex = author_hindex[0]
						if author_hindex == 'NA':
							author_hindex = 1 #default value
						else:
							author_hindex = int(author_hindex)
						if author_hindex > max_hindex:
							max_hindex = author_hindex
				row['max_hindex'] = max_hindex
				row['author_count'] = author_count
			#todo authors
			if len(row) == 6:
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
	fhindex = open('/home/anjan/data/acl_anthology/aan/release/2012/authors_hindex.txt')
	fout = open('/home/anjan/data/acl_anthology/aan/authors_table.txt', 'wb')
	hindex_dict = {}
	for line in fhindex:
		splitted = line.split("\t")
		if len(splitted) == 2:
			hindex = splitted[0].strip()
			name = splitted[1].strip()
			if name in hindex_dict:
				print('multiple hindex for %s' %name)
				if hindex_dict[name] == 'NA':
					hindex_dict[name] = hindex
				elif int(hindex) > int(hindex_dict[name]):
					hindex_dict[name] = hindex
			else:
				hindex_dict[name] = hindex
	
	duplicate_count = 0
	author_dict = {}
	for line in fauthorid:
		splitted = line.split("\t")
		if len(splitted) == 2:
			author_id = splitted[0].strip()
			name = splitted[1].strip()
			if name in author_dict:
				#print('author is duplicate %s' %name)
				duplicate_count = duplicate_count + 1
			else:
				author_dict[name] = author_id
		else:
			print('could not parse line in authorid : %s' %line)
	
	na_count = 0
	for name, id in author_dict.items():
		if name in hindex_dict:
			hindex = hindex_dict[name]
		else:
			hindex = 'NA'
			#print('hindex not available for author : %s' %name)
			na_count = na_count + 1
		record = (author_id, name, hindex)
		cur.execute("insert into author values (?, ?, ?)", record)
		fout.write( bytes("%s\t%s\t%s\n" %record, 'UTF-8'))
	print('author hindex NA count : %d' %na_count)
	print('author duplicate count : %d' %duplicate_count)
	fauthorid.close()
	fhindex.close()
	fout.close()
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
					
														
def main():
	db = sqlite3.connect('/home/anjan/data/acl_anthology/aan/papers.db')
	cur = db.cursor()
	
	drop_tables(cur)
	create_tables(cur)
	populate_authors(cur)
	readmeta(cur)
	count_rows(cur, 'paper')
	populate_topics(cur)
	populate_citations(cur)	
	populate_citation_year(cur) #also creates the data/feature files
	cur.close()
	db.commit()
	db.close()

	
if __name__ == "__main__":
	main()

import codecs
import sqlite3
import re

def removeUnicode(s): 
	return re.sub(r'[^\x20-\x7e]', 'UNICODE', s)

def drop_tables(cur):
	cur.execute('drop table if exists paper')

def create_tables(cur):
	cur.execute('''create table paper (
	id TEXT,
	title TEXT,
	venue TEXT,
	year TEXT)
	''')
	
def test(cur):
	cur.execute("insert into paper values ('testid', 'testtitle', 'testvenue', 'testyear')")
	cur.execute("select * from paper")
	data = cur.fetchone()
	print(data)
	
def insert(cur, row):
	record = (row['id'], row['title'], row['venue'], row['year'])
	cur.execute("insert into paper values (?, ?, ?, ?)", record)
	
def readmeta(cur):
	'''reads aclmetadata file and populates the records into the table'''
	#fmeta = open('/home/anjan/data/acl_anthology/aan/release/2012/acl-metadata.txt')
	fmeta = codecs.open('/home/anjan/data/acl_anthology/aan/release/2012/acl-metadata.txt', encoding='Latin-1')
	complete = 0
	row = {}
	for line in fmeta:
		#print(line)
		if len(line.strip()) == 0:
			if complete:
				#insert the row into the table
				insert(cur, row)
				row = {}
				complete = 0
			else:
				if len(row) != 0:
					print('error: not complete information')
				#else ignore (multiple white space lines)
		else:
			splitted = line.split('=')
			key = splitted[0].strip()
			if len(splitted) == 2:
				value = splitted[1].strip()
			else:
				value = "NIL"
			if key == 'id':
				row['id'] = value
			elif key == 'title':
				row['title'] = value
			elif key == 'venue':
				row['venue'] = value
			elif key == 'year':
				row['year'] = value
			#todo authors
			if len(row) == 4:
				complete = 1
	#if there is no final empty line, final record maynot have been inserted
	if len(row) != 0 and complete:
		insert(cur, row)


def count_rows(cur, table):
	cur.execute("select count(*) from %s" %table)
	count = cur.fetchone()
	print("count = %d" %count)

def main():
	db = sqlite3.connect('/home/anjan/data/acl_anthology/aan/papers.db')
	cur = db.cursor()
	drop_tables(cur)
	create_tables(cur)
	readmeta(cur)
	count_rows(cur, 'paper')
	#test(cur)
	cur.close()
	db.close()

	
if __name__ == "__main__":
	main()

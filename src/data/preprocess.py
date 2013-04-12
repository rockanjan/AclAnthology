import time
import glob
import logging
import re
from string import punctuation
from settings import DATA_FOLDER
from settings import PAPERS_FOLDER
from settings import VOCAB_FILE
from settings import STOP_WORD_FILE

#logging.basicConfig(filename='example.log',level=logging.DEBUG)
#logging.basicConfig(level=logging.DEBUG)

def get_word_list(fin):
	"""
		return list of words after preprocessing them for the given file
	"""
	word_list = []
	contWord = '';
	for line in fin:
		line = line.lower()
		words = line.split();
		for word in words:
			if word[-1] == '.':
				word = word[0:-1]
			if len(contWord):
				word = contWord + word
				logging.debug('after join ' + word)
				contWord = ''				
			if(len(word)):
				if word[-1] == '-':
					contWord = word[0:-1] #will be processed in next line
					logging.debug('continuing : ' + contWord)
					continue
				else:
					#add to vocab
					for p in punctuation:
						word = word.replace(p, '')
					if len(word):
						#logging.debug(word)
						#time.sleep(1)
						word_list.append(word)
	return word_list


t = time.time()
print('starting processing...')

stop_words = (word.lower() for line in open(STOP_WORD_FILE) for word in line.split())
stop_word_list = []
for word in stop_words:
	stop_word_list.append(word)


filelist = glob.glob(PAPERS_FOLDER + "*.txt")
fvocab = open(VOCAB_FILE, 'w');
vocab_freq = {};
#create vocab file from all files
for filename in filelist:
	fin = open(filename, 'r')
	filename = filename.split('/')[-1]
	if(filename == 'collaboration_network.txt'):
		continue
	logging.info('processing file %s' % filename)
	word_list = get_word_list(fin)
	for word in word_list:
		if word in vocab_freq:
			vocab_freq[word] = vocab_freq[word] + 1
		else:
			vocab_freq[word] = 1
	fin.close()

#save vocab file
vocab_length = 0
for k,v in vocab_freq.items():
	if len(k) and k not in stop_word_list and not re.search('^[0-9]+$', k) and not len(k) == 1 and not (v < 10):
		#fvocab.write('%d %s\n' %(v,k))
		fvocab.write('%s\n' %k)
		vocab_length = vocab_length + 1
fvocab.close()
logging.info('vocab size : %d' %(vocab_length))

#read vocab file
vocablist = open(VOCAB_FILE).read().splitlines()
vocab_index = {}
for index,vocab in enumerate(vocablist):
	vocab_index[vocab] = index

logging.debug('vocab size read : %d' %(len(vocab_index)))
# prepare a file that can be input to LDA topic model
fldaformat = open(DATA_FOLDER + 'lda.dat', 'w');
#order of list of files
flist = open(DATA_FOLDER + 'filelist.dat', 'w');

for filename in filelist:
	fin = open(filename, 'r')
	filename = filename.split('/')[-1]
	if(filename == 'collaboration_network.txt'):
		continue
	flist.write(filename + "\n")
	total = 0
	worddict = {}
	word_list = get_word_list(fin)
	for word in word_list:
		index = -1
		if word in vocab_index:
			total = total + 1
			index = vocab_index[word]
			if index in worddict:
				worddict[index] = worddict[index] + 1
			else:
				worddict[index] = 1
		else:
			logging.debug('word %s not in vocab' %word)
				
	#write
	fldaformat.write('%d ' %total)
	for k,v in worddict.items():
		fldaformat.write("%d:%d " %(k,v))
	fldaformat.write("\n")
	fin.close()
fvocab.close()
fldaformat.close()
flist.close()

elapsed = time.time() - t
print('total time in seconds %d' %elapsed)
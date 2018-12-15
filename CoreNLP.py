# _*_coding:utf-8_*_
import multiprocessing
import sys
import subprocess
from stanfordcorenlp import StanfordCoreNLP

nlp = StanfordCoreNLP(r'/tmp/stanford-corenlp-full-2018-02-27')
def collectResults(ret_cnt):
	total_line = 1000
	print("Processed {}% of Posts".format(str(100.0 * ret_cnt[1] / total_line)))
	outfile = open("/tmp/ner.txt", "a", encoding="utf-8")
	outfile.write(str(ret_cnt[0])+"\n")

def processPosts(post, properties, cnt):
	props = {'annotators': properties}
	ret = nlp.annotate(post, properties=props)
	return [ret, cnt]

if __name__ == "__main__":
	pool = multiprocessing.Pool(multiprocessing.cpu_count() + 4)
	wpfile = open(sys.argv[1], "r", encoding="utf-8")
	properties = sys.argv[2]
	posts = wpfile.readlines()
	cnt = 0
	for post in posts:
		cnt += 1
		pool.apply_async(processPosts, (post, properties, cnt,), callback = collectResults)
	pool.close()
	pool.join()
	nlp.close()        

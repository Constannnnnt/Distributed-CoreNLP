# -*- coding: utf-8 -*-
import multiprocessing
import json
import html.parser
import re

class HTMLTextExtractor(html.parser.HTMLParser):
    def __init__(self):
        super(HTMLTextExtractor, self).__init__()
        self.result = []

    def handle_data(self, d):
        self.result.append(d)

    def get_text(self):
        return ''.join(self.result)

def collectResults(ret):
    outfile = open("washington_post.txt", "a")
    outfile.write(str(ret)+"\n")

def genPosts(post):
    ret = {}
    sentence = HTMLTextExtractor()
    if "contents" in post:
        for block in post["contents"]:
            if ("content" in block):
                if (block["type"] == "sanitized_html"):
                    sentence.feed(str(block["content"]))
    paragraph = sentence.get_text()
    ret[post["id"]] = paragraph
    return ret

if __name__ == "__main__":
    pool = multiprocessing.Pool(4)
    file = open("washington_post_sample.json", "r")
    posts = file.readlines()
    for post in posts:
        pool.map_async(genPosts, (json.loads(post),), callback = collectResults)
    pool.close()
    pool.join()

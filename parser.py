#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

import multiprocessing
import json
import html.parser
import re
import sys

class HTMLTextExtractor(html.parser.HTMLParser):
    def __init__(self):
        super(HTMLTextExtractor, self).__init__()
        self.result = []

    def handle_data(self, d):
        self.result.append(d)

    def get_text(self):
        return ' '.join(self.result)

def collectResults(ret_cnt):
    total_line = 595037
    print("Processed {}% of Posts".format(str(100.0 * ret_cnt[1] / total_line)))
    outfile = open("washington_post.txt", "a", encoding="utf-8")
    outfile.write(str(ret_cnt[0])+"\n")

def genPosts(post, cnt):
    ret = {}
    sentence = HTMLTextExtractor()
    if "contents" in post:
        for block in post["contents"]:
            if ("content" in block):
                if (block["type"] == "sanitized_html"):
                    sentence.feed(str(block["content"]))
    paragraph = sentence.get_text()
    ret[post["id"]] = paragraph
    return [ret, cnt]

if __name__ == "__main__":
    pool = multiprocessing.Pool(multiprocessing.cpu_count() + 2)
    wpfile = open(sys.argv[1], "r", encoding="utf-8")
    posts = wpfile.readlines()
    cnt = 0
    for post in posts:
        cnt += 1
        pool.apply_async(genPosts, (json.loads(post), cnt,), callback = collectResults)
    pool.close()
    pool.join()

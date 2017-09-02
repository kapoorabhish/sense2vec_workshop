from __future__ import print_function, unicode_literals, division
import io
from toolz import partition
from os import path
import os
import re
import sys
import spacy.en
import multiprocessing

try:
    import ujson as json
except ImportError:
    import json

nlp = spacy.en.English()
nlp.matcher = None

LABELS = {
    'ENT': 'ENT',
    'PERSON': 'ENT',
    'NORP': 'ENT',
    'FAC': 'ENT',
    'ORG': 'ENT',
    'GPE': 'ENT',
    'LOC': 'ENT',
    'LAW': 'ENT',
    'PRODUCT': 'ENT',
    'EVENT': 'ENT',
    'WORK_OF_ART': 'ENT',
    'LANGUAGE': 'ENT',
    'DATE': 'DATE',
    'TIME': 'TIME',
    'PERCENT': 'PERCENT',
    'MONEY': 'MONEY',
    'QUANTITY': 'QUANTITY',
    'ORDINAL': 'ORDINAL',
    'CARDINAL': 'CARDINAL'
}


def iter_comments(files_dir, files_list):
    for each in files_list:
        with open(path.join(files_dir, each)) as f:
            s = f.read()
            if s:
                yield each, s


def process_files(files_dir, files_list, out_dir, done_record_file, done_files=[]):
    for each in files_list:
        if each:
            if each not in done_files:
                with open(path.join(files_dir, each)) as f:
                    s = f.read()
                    if s:
                        parse_and_transform(each, [s], out_dir)
            with open(done_record_file, "a") as done_f:
                done_f.write(each+"\n")
            done_f.close()


pre_format_re = re.compile(r'^[\`\*\~]')
post_format_re = re.compile(r'[\`\*\~]$')
url_re = re.compile(r'\[([^]]+)\]\(%%URL\)')
link_re = re.compile(r'\[([^]]+)\]\(https?://[^\)]+\)')


def strip_meta(text):
    text = link_re.sub(r'\1', text)
    text = text.replace('&gt;', '>').replace('&lt;', '<')
    text = pre_format_re.sub('', text)
    text = post_format_re.sub('', text)
    return text


def parse_and_transform(file_name, input_, out_dir):
    out_loc = path.join(out_dir, '%s.txt' % file_name)
    if path.exists(out_loc):
        return None
    with io.open(out_loc, 'w', encoding='utf8') as file_:
        for text in input_:
            try:
                file_.write(transform_doc(nlp(strip_meta(text))))
            except:
                import traceback; traceback.print_exc()
    file_.close()


def transform_doc(doc):
    for ent in doc.ents:
        ent.merge(ent.root.tag_, ent.text, LABELS[ent.label_])
    for np in list(doc.noun_chunks):
        while len(np) > 1 and np[0].dep_ not in ('advmod', 'amod', 'compound'):
            np = np[1:]
        np.merge(np.root.tag_, np.text, np.root.ent_type_)
    strings = []
    for sent in doc.sents:
        if sent.text.strip():
            strings.append(' '.join(represent_word(w) for w in sent if not w.is_space))
    if strings:
        return '\n'.join(strings) + '\n'
    else:
        return ''


def represent_word(word):
    if word.like_url:
        return '%%URL|X'
    text = re.sub(r'\s', '_', word.text)
    tag = LABELS.get(word.ent_type_, word.pos_)
    if not tag:
        tag = '?'
    return text + '|' + tag


if __name__ == "__main__":

    corpus_dir = sys.argv[1]
    out_dir = sys.argv[2]
    partition_size = int(sys.argv[3])
    prcs = []
    jobs = partition(partition_size, os.listdir(corpus_dir))
    for i, each in enumerate(jobs):
        done_files = []
        if os.path.isfile(str(i)):
            with open(str(i)) as f:
                done_files = [each_line.strip() for each_line in f.read().splitlines()]
        p = multiprocessing.Process(target=process_files, args=(corpus_dir, each, out_dir, str(i), done_files))
        prcs.append(p)
        p.start()

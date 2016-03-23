import os
import json
import pymongo
import sys
from datetime import *
from props import *

def process_obj(instance_of, entity_type, entity):
    matches = 0
    st = datetime.utcnow()
    print "Start time " , st
    #ofile.write("start time " + str(st) + "\n")
    for rec in get_records():
        claims = rec['claims']
        if instance_of in claims:
            for it in claims[instance_of]:
                if('datavalue' in it['mainsnak']):
                    if('value' in it['mainsnak']['datavalue']):
                        if('numeric-id' in it['mainsnak']['datavalue']['value']):
                            if (it['mainsnak']['datavalue']['value']['numeric-id'] == entity_type):
                                print "match found "
                                matches += 1
                                if 'enwiki' in rec['sitelinks']:
                                    title = rec['sitelinks']['enwiki']['title']
                                    if(type(title) is unicode):
                                        title = title.encode('utf-8')
                                    op_str = title
                                    if 'url' in rec['sitelinks']['enwiki']:
                                        url = rec['sitelinks']['enwiki']['url']
                                        if(type(url) is unicode):
                                            url = url.encode('utf-8')
                                        op_str = op_str + ',' + url
                                    yield op_str

def get_records():
    # insert all rows
    with open(json_dump_file, 'r') as fp:
        for iline, line in enumerate(fp):
            if line.startswith('[') or line.startswith(']'):
                continue
            obj = json.loads(line.strip(',\n'))
            yield obj

wikidata_props = {
    "instance of" : "P31",
    "public company" : 891723,
    "business enterprise" : 4830453
}

dump_path = '/data/wikidata/wikiworld/'
json_dump_file = os.path.join(dump_path, '20160118.json')


entity = "public company"
instance_of = wikidata_props["instance of"]
entity_type = wikidata_props[entity]
for title in process_obj(instance_of, entity_type, entity):
    print title


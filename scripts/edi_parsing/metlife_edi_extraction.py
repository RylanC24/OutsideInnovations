import json
import re


data_files = [
    '../edi_data/edi_20170401_20170417.txt'
]

data = []
for file in data_files:
    with open(file) as f:
        data += json.load(f)

metlife = []
for d in data:
    if re.search('metlife', d['EdiResponse'], re.IGNORECASE):
        metlife.append(d)

output_file = '../edi_data/metlife_cleaned_edi_20170401_20170417.json'
with open(output_file, 'x') as f:
    for d in metlife:
        if d['EdiResponse']:
            f.write(json.dumps(d, ensure_ascii=False)+'\n')
f.close()

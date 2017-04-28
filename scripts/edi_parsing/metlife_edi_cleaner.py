import json
import re
from pprint import pprint


output_file = './edi_data/metlife_cleaned_edi_HTMLOnly_noErrors_20140516_20170331.txt'

data_files = [
    './edi_data/edi_html_20140516_20141231.txt',
    './edi_data/edi_html_20150101_20150331.txt',
    './edi_data/edi_html_20150401_20150430.txt',
    './edi_data/edi_html_20150501_20150531.txt',
    './edi_data/edi_html_20150601_20150630.txt',
    './edi_data/edi_html_20150701_20150831.txt',
    './edi_data/edi_html_20150901_20151031.txt',
    './edi_data/edi_html_20151101_20151231.txt',
    './edi_data/edi_html_20160101_20160228.txt',
    './edi_data/edi_html_20160301_20160430.txt',
    './edi_data/edi_html_20160501_20160630.txt',
    './edi_data/edi_html_20160701_20160731.txt',
    './edi_data/edi_html_20160801_20160831.txt',
    './edi_data/edi_html_20160901_20161031.txt',
    './edi_data/edi_html_20161101_20161231.txt',
    './edi_data/edi_html_20170101_20170228.txt',
    './edi_data/edi_html_20170301_20170331.txt'
]

data_files = [
    './edi_data/edi_html_20170401_20170417.txt'
]

# Read in json data from OF REST API
data = []
for file in data_files:
    with open(file) as f:
        data += json.load(f)

with open(output_file, 'x') as f:
    for datum in data:
	# Look for MetLife only responses
        if re.search('metlife', datum['HtmlResponse'], re.IGNORECASE):
            # Filter out responses that contained an error	
            if not re.search('An Error Occurred', datum['HtmlResponse']):
                f.write(json.dumps(datum, ensure_ascii=False)+'\n')

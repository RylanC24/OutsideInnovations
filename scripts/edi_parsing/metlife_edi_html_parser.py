import json
import re
import numpy as np
import pandas as pd
import metlife_parsing_utilities as mpu
from bs4 import BeautifulSoup
import time


t1 = time.time()

input_file = '../edi_data/final_data/' \
             'metlife_cleaned_edi_HTMLOnly_noErrors_20170401_20170417.txt'
output_file = '../edi_data/parsed_data/metlife_20170401_20170417.csv'

data = []
with open(input_file) as f:
    for line in f:
        data.append(json.loads(line))

# Create datafram to store parsed data
df = pd.DataFrame()

# Keep track of time
i = 0
n = len(data)

# Loop through html responses and parse out required data to add to dataframe
for datum in data:
    # Print progress and time elapsed
    if i % 1000 == 0:
        print('On record', i, 'out of', n, '\ntime elapsed: {:.02f} minutes'.format((time.time() - t1) / 60))
    i += 1

    # Create dictionary to store parsed values
    values = {}
    if datum['InsurancePolicyPatientEligibilityId']:
        values['InsurancePolicyPatientEligibilityId'] = datum['InsurancePolicyPatientEligibilityId']
    if datum['InsuranceEligibilityAuditId']:
        values['InsuranceEligibilityAuditId'] = datum['InsuranceEligibilityAuditId']

    # Parse the html
    soup = BeautifulSoup(datum['HtmlResponse'], 'lxml')

    # Figure out which carrier this is and send to the html parser
    payer_table = soup.find(id='payerTable')
    if payer_table:
        values['CarrierName_HTML'] = mpu.find_next_sibling(
            payer_table, 'th', 'Payer Name', 'td'
        )
        values['TransactionId'] = mpu.find_next_sibling(
            payer_table, 'th', 'Transaction ID', 'td'
        )

        # Double check to see if carrier is metlife
        if re.search('metlife', values['CarrierName_HTML'], re.IGNORECASE):
            # Find data from provider table if it is there
            parsed_data = mpu.parse_provider_table(soup)
            values.update(parsed_data)

            # Find data from subscriber table if it is there
            parsed_data = mpu.parse_subscriber_table(soup)
            values.update(parsed_data)

            # Find data from coverage type table if it is there
            parsed_data = mpu.parse_coverage_type_table(soup)
            values.update(parsed_data)

            # Find data from coverage dates table if it is there
            parsed_data = mpu.parse_coverage_dates_table(soup)
            values.update(parsed_data)

            # Find data from maximums table if it is there
            parsed_data = mpu.parse_maximums_table(soup)
            values.update(parsed_data)

            # Find data from plan provider table if it is there
            parsed_data = mpu.parse_plan_provisions_table(soup)
            values.update(parsed_data)

            # Find data from coverage table if it is there
            parsed_data = mpu.parse_coverage_table(soup)
            values.update(parsed_data)

            # Create new dataframe to store this row
            try:
                row = pd.DataFrame(values, index=[0])
            except:
                print('Failed to create dataframe row. Iteration:', i, '\nvalues:')
                print(values)

        # Append row to dataframe
        df = df.append(row, ignore_index=True)

    # If a payer table can not be found then skip this edi response
    else:
        print(
            str(int(datum['InsurancePolicyPatientEligibilityId'])),
            " does not have a payer table"
        )
        continue

# Replace blank values from html, represented as spaces (ascii code: '\xa0')
# with NaN values
df.replace(to_replace='\xa0', value=np.NaN, inplace=True)
df.replace(to_replace='', value=np.NaN, inplace=True)

# Write dataframe to csv file
df.to_csv(output_file, index=False)

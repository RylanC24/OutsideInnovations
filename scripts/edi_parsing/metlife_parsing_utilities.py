from bs4 import BeautifulSoup
import re


def find_next_sibling(soup, element_type, text, sibling_type, n_steps=1):
    """ Find the text in the sibling element that comes (n_steps) after the
    element specified by the 'element' and 'text' parameters. By default
    n_steps=1, which means that this will select the sibling element immedietly
    after the element specified. If n_steps=2 it will select the second sibling
    element after the specified element. If there are no matching elements
    or no sibling elements after the matched element then it will return None.

    Args:
        soup (BeautifulSoup object): the html to be parsed
        element_type (str): the specified element type to search for. This is
                            the anchor element that our search is based off of.
        text (str): The text to identify the specified element.
        sibling_type (str): the type of element that we actually want to find.

    Keyword Arguments:
        n_steps (int): the number of elements after the specified element
                       to select.

    Returns:
        The text in desired element if it exists. None otherwise.
    """

    # Find the 'th' element containing the given text
    elem = soup.find(element_type, text=re.compile(r'{}'.format(text)))

    # If the specified element doesn't exist then return None
    if not elem:
        return None

    # Find the sibling element that that is n_steps away
    steps = 1
    for sibling in elem.next_siblings:
        if sibling.name == sibling_type:
            if steps == n_steps:
                return sibling.text
            else:
                steps += 1

    return None


def parse_provider_table(soup):
    data = {}

    # Find the Provider table
    provider_table = soup.find(id='providerTable')
    if not provider_table:
        return data

    data['ProviderName'] = find_next_sibling(
        provider_table, 'th', 'Provider', 'td'
    )
    data['ProviderAddress'] = find_next_sibling(
        provider_table, 'th', 'Address', 'td'
    )
    data['ProviderId'] = find_next_sibling(
        provider_table, 'th', 'Provider ID', 'td'
    )
    data['ProviderTaxId'] = find_next_sibling(
        provider_table, 'th', 'Tax ID', 'td'
    )

    return data


def parse_subscriber_table(soup):
    data = {}

    # Find the Subscriber table
    subscriber_table = soup.find(id='subscriberTable')
    if not subscriber_table:
        return data

    data['SubscriberPatientName'] = find_next_sibling(
        subscriber_table, 'th', 'Patient Name', 'td'
    )
    data['SubscriberMemberId'] = find_next_sibling(
        subscriber_table, 'th', 'Member ID', 'td'
    )
    data['SubscriberSSN'] = find_next_sibling(
        subscriber_table, 'th', 'SSN', 'td'
    )
    data['GroupNumber'] = find_next_sibling(
        subscriber_table, 'th', 'Group Number', 'td'
    )
    data['GroupName'] = find_next_sibling(
        subscriber_table, 'th', 'Group Name', 'td'
    )
    data['SubscriberDOB'] = find_next_sibling(
        subscriber_table, 'th', 'Date of Birth', 'td'
    )
    data['SubscriberSex'] = find_next_sibling(
        subscriber_table, 'th', 'Gender', 'td'
    )
    data['SubscriberAddress'] = find_next_sibling(
        subscriber_table, 'th', 'Address', 'td'
    )

    # Parse the City, State, and zip
    address = subscriber_table.find('th', text=re.compile(r'Address'))
    if not address:
        return data

    address2 = address.parent.next_sibling
    if not address2:
        return data

    address2 = address2.find('td')
    if not address2:
        return data

    address2 = address2.text.split(',')
    data['SubscriberCity'] = address2[0]

    # Filter out any extra spaces
    city_zip = [x for x in address2[1].split(' ') if x != '']
    # Make sure it's in the right format
    if len(city_zip) != 2:
        if city_zip:
            data['SubscriberAddress2'] = city_zip
        return data

    data['SubscriberState'] = city_zip[0] if len(city_zip[0]) == 2 else None
    data['SubscriberZip'] = city_zip[1] if len(city_zip[1]) >= 5 else None

    return data


def parse_coverage_type_table(soup):
    data = {}

    # Find the Coverage Type table
    coverage_table = soup.find(id='coveragesTable')
    if not coverage_table:
        return data

    coverage = coverage_table.find('td').text
    if coverage.find('br'):
        coverage = re.sub('<br/>', ', ', str(coverage))
        data['CoverageType'] = BeautifulSoup(coverage, 'lxml').text

    return data


def parse_coverage_dates_table(soup):
    data = {}

    # Find Coverage Dates table
    coverage_dates_table = soup.find(id='coverageDatesTable')
    if not coverage_dates_table:
        return data

    elem = coverage_dates_table.find(
        'td', text=re.compile(r'Policy Effective')
    )
    if elem:
        data['SubscriberPlanEffectiveDateStart'] = elem.text.split(' ')[-1]

    elem = coverage_dates_table.find(
        'td', text=re.compile(r'Policy Expiration')
    )
    if elem:
        data['SubscriberPlanEffectiveDateEnd'] = elem.text.split(' ')[-1]

    elem = coverage_dates_table.find(
        'td', text=re.compile(r'Plan Begin Date')
    )
    if elem:
        data['PlanBenefitsStart'] = elem.text.split(' ')[-1]

    elem = coverage_dates_table.find(
        'td', text=re.compile(r'Plan End')
    )
    if elem:
        data['PlanBenefitsEnd'] = elem.text.split(' ')[-1]

    return data


def parse_maximums_table(soup):
    data = {}

    # Find Maximums table
    maximums_table = soup.find(id='maximumsTable')
    if not maximums_table:
        return data

    # Find orthodontics row
    elem = maximums_table.find('td', text=re.compile(r'Orthodontics'))
    if not elem:
        return data

    # Find in-network orthodontics lifetime max
    row = elem.parent
    lifetime_max = row.find('td', {'class': 'inNetwork'})
    if lifetime_max:
        data['LifetimeMax_InNetwork'] = lifetime_max.text[1:]
    # Find out-of-network orthodontics lifetime max
    lifetime_max = row.find('td', {'class': 'outNetwork'})
    if lifetime_max:
        data['LifetimeMax_OutNetwork'] = lifetime_max.text[1:]

    # Find in-network orthodontics lifetime used
    row = row.next_sibling
    if not row:
        return data

    lifetime_used = row.find('td', {'class': 'inNetwork'})
    if lifetime_used:
        data['LifetimeUsed_InNetwork'] = lifetime_used.text[1:]
    lifetime_used = row.find('td', {'class': 'outNetwork'})
    if lifetime_used:
        data['LifetimeUsed_OutNetwork'] = lifetime_used.text[1:]

    # Find in-network orthodontics lifetime remaining
    row = row.next_sibling
    if not row:
        return data

    lifetime_remaining = row.find('td', {'class': 'inNetwork'})
    if lifetime_remaining:
        data['LifetimeRemaining_InNetwork'] = lifetime_remaining.text[1:]
    # Find out-of-network orthodontics lifetime remaining
    lifetime_remaining = row.find('td', {'class': 'outNetwork'})
    if lifetime_remaining:
        data['LifetimeRemaining_OutNetwork'] = lifetime_remaining.text[1:]

    return data


def parse_plan_provisions_table(soup):
    data = {}

    # Find Plan Provisions table
    plan_provisions_table = soup.find(id='planProvisionsTable')
    if plan_provisions_table:
        if plan_provisions_table.find('td', text=re.compile(r'Waiting Period does not apply.')):
            data['WaitPeriod'] = False
        else:
            data['WaitPeriod'] = True

    return data


def parse_coverage_table(soup):
    data = {}

    # Find Coverage table
    coverage_table = soup.find(id='coInsuranceTable')
    if not coverage_table:
        return data

    # Find orthodontics row
    elem = coverage_table.find('td', text=re.compile(r'Orthodontics'))
    if not elem:
        return data

    # Find in-network orthodontics co-insurance percentage
    co_in = elem.parent.find('td', {'class': 'inNetwork'})
    if co_in and '%' in co_in.text:
        data['CoIns_InNetwork'] = int(co_in.text[:-1])/100
    # Find out-of-network orthodontics co-insurance percentage
    co_out = elem.parent.find('td', {'class': 'outNetwork'})
    if co_out and '%' in co_out.text:
        data['CoIns_OutNetwork'] = int(co_out.text[:-1])/100

    return data

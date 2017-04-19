import pandas as pd
import numpy as np
from datetime import datetime, date


def drop_columns(df, columns):
    """A function to drop columns from a dataframe

    Args:
        df (Pandas DataFrame object): the dataframe to be modified.
        columns (list of strings): the list of column names to be dropped

    Returns:
        None - the dataframe is modified in place.
    """
    for column in columns:
        # Check to make sure the column is in the dataframe
        if column in df.columns:
            df.drop(column, axis=1, inplace=True)


def build_set(sql_file, html_file):
    """A function to combine various data sources into a single dataframe that
    is used for EDI check classification

    Args:
        sql_file (str): the filename containing the OF SQL data.
        html_file (str): the filename containing the parsed EDI HTML data.

    Returns:
        Pandas DataFrame object - the dataframe containing the joined data
    """

    # Load in the SQL query csv file
    df_query = pd.read_csv(
        sql_file,
        low_memory=False,
        encoding='ISO-8859-1'
    )

    # Load in the parsed HTML csv file
    df_html = pd.read_csv(
        html_file,
        low_memory=False,
        encoding='ISO-8859-1'
    )

    # Convert objects that should be floats to floats
    obj_2_float_col = [
        'LifetimeMax_InNetwork',
        'LifetimeMax_OutNetwork',
        'LifetimeRemaining_InNetwork',
        'LifetimeRemaining_OutNetwork',
        'LifetimeUsed_InNetwork',
        'LifetimeUsed_OutNetwork',
    ]

    for col in obj_2_float_col:
        df_html[col] = df_html[col].str.translate({ord(','): None}).astype('float')

    # Drop some columns that are blank in HTML before determining query columns
    # to merge. Info for these columns is more complete in query
    html_col_drop = [
        'GroupName',
        'SubscriberSSN'
    ]
    drop_columns(df_html, html_col_drop)

    # Extract columns from query that are not in html
    query_columns = list(set(df_query.columns)-set(df_html.columns))
    query_columns.append('InsurancePolicyPatientEligibilityId')

    # Drop entries that with no InsurancePolicyPatientEligibilityId from
    # parsed HTML data
    df_html = df_html[df_html['InsurancePolicyPatientEligibilityId'].notnull()]

    # Drop entries with duplicate IPPEIDs from parsed HTML data
    df_html.drop_duplicates(
        ['InsurancePolicyPatientEligibilityId'],
        keep=False,
        inplace=True
    )

    # Join the datatables
    df_joined = pd.merge(
        df_html,
        df_query[query_columns],
        on='InsurancePolicyPatientEligibilityId',
        how='left'
    )

    # Reduce In/Out of network to appropriate value
    df_joined['LifeTimeRemainingValue'] = [
        row['LifetimeRemaining_OutNetwork']
        if row['IsInNetwork'] == 0
        else row['LifetimeRemaining_InNetwork']
        for index, row in df_joined.iterrows()
    ]

    df_joined['LifeTimeMaxValue'] = [
        row['LifetimeMax_OutNetwork']
        if row['IsInNetwork'] == 0
        else row['LifetimeMax_InNetwork']
        for index, row in df_joined.iterrows()
    ]

    df_joined['OrthoBenefitUsedLifetime'] = [
        row['LifetimeUsed_OutNetwork']
        if row['IsInNetwork'] == 0
        else row['LifetimeUsed_InNetwork']
        for index, row in df_joined.iterrows()
    ]

    df_joined['CoIns'] = [
        row['CoIns_OutNetwork']
        if row['IsInNetwork'] == 0
        else row['CoIns_InNetwork']
        for index, row in df_joined.iterrows()
    ]

    return df_joined


def exclusion_case(dob, student_status, pre_auth, age_max, age_max_student,
                   wait_period, lifetime_max_value, lifetime_remaining_value):
    """Determine whether a given EDI check should be classified by one of the
    exclusion cases.

    Args:
        dob (datetiem): the patients date of birth.
        student_status (): .
        pre_auth (int): whether or not pre authorization is required.
        age_max (int): the maximum age that treatment is covered.
        age_max_student (int): the maximum age that treatment is covered for
                               a student.
        wait_period (boolean): whether or not there is a wait period.

    Returns:
        Boolean - True if the check falls under one of the exclusion cases.
    """

    # Check that all variables have been passed to function - if not error out
    if (pd.isnull(dob) or
            pd.isnull(student_status) or
            pd.isnull(pre_auth) or
            pd.isnull(age_max) or
            pd.isnull(age_max_student) or
            pd.isnull(wait_period)):
        return True

    # Age calculation - dob expected as 'Full_Month_Name Day# Year_w_Century'
    age_days = date.today() - datetime.strptime(dob, '%m/%d/%Y').date()
    age = round(age_days.days/365.25)

    # Student statuses
    student_statuses = ['PartTime', 'FullTime']

    # Check to see if lifetime max value or lifetime remaining value are null
    if pd.isnull(lifetime_max_value) or pd.isnull(lifetime_remaining_value):
        return True

    # Check for the wait period exception
    if wait_period:
        return True

    # Check for the stduent exception
    elif student_status in student_statuses and (age >= age_max_student):
        return True

    # Check for the age max exception
    elif (age >= 18 and age <= 26) or age >= age_max:
        return True

    # Check for the pre authorization exception
    elif pre_auth != 0:
        return True

    return False


def train_feature_impute(df):
    """A function to clean the data and extract features

    Args:
        train (boolean): whether or not a training set is being cleaned
        df (Pandas DataFrame object): the dataframe containing the data to
                                      be cleaned.

    Returns:
        Pandas DataFrame object - the dataframe containing the extracted data
    """

    # Filter out everything but MetLife claims
    df = df[df['CarrierName'] == 'MetLife']

    # Drop the 'CarrierName' column since we're only looking at MetLife
    df.drop('CarrierName', axis=1, inplace=True)

    # Drop columns specific to HTML
    html_col_drop = [
        'LifetimeRemaining_OutNetwork',
        'LifetimeRemaining_InNetwork',
        'LifetimeMax_OutNetwork',
        'LifetimeMax_InNetwork',
        'LifetimeUsed_OutNetwork',
        'LifetimeUsed_InNetwork',
        'CoIns_OutNetwork',
        'CoIns_InNetwork',
        'CarrierName_HTML',
        'CoverageType',
        'ProviderAddress',
        'ProviderId',
        'ProviderName',
        'ProviderTaxId',
        'SubscriberMemberId',
        'SubscriberPatientName',
        'TransactionId'
    ]
    drop_columns(df, html_col_drop)

    # Remove additional columns we know are useless from talks with OF
    OF_talks_unnec = [
        'InitialPaymentPercent',
        'OrthoBenefitUsedLifetime',
        'PlanPriority',
        'IsMinMaxDependentsOnly',
        'IsActive',
        'IsActive.1',
        'IsInNetwork',
        'PracticeOverriddenBenefit',
        'IsTerminated',
        'TotalNumberOfAdjustments',
        'TotalAdjustmentValue',
        'BenefitPaidToDate',
        'CurrentEstimatedAr',
        'OrthoFiCalculatedBenefit',
        'SubscriberAddress2',
        'SubscriberMiddleInitial',
        'SubscriberPhonePrimary',
        'SubscriberPhoneSecondary',
        'SubscriberSex',
        'SubscriberSuffix',
        'DeductibleOrthoLifetimeMax',
        'ClaimStatus',
        'HowManyElecChecks',
        'AgeLimit'
    ]
    drop_columns(df, OF_talks_unnec)

    # Define columns to save based on OF notes
    saved_columns = [
        'InsurancePlanPriorityId',
        'PayerId',
        'PatientDateOfBirth',
        'InsurancePolicyPatientEligibilityId'
    ]

    # Drop ID columns.
    # NOTE: we might want to convert these to binary instead
    id_columns = [
        column
        for column in df.columns
        if column.endswith(('Id', 'Id.1')) and column not in saved_columns
    ]
    drop_columns(df, id_columns)

    # Drop columns that only contain null values
    null_columns = [
        column
        for column in df.columns
        if df[column].count() == 0
    ]
    drop_columns(df, null_columns)

    # Convert PatientDateOfBirth to Patient Age
    df['PatientAge'] = df['PatientDateOfBirth'].apply(
        lambda row: int(
            (date.today() - datetime.strptime(row, '%m/%d/%Y').date()).days / 365.25
        )
    )

    # Check for exclusion cases
    df['Exclusion'] = df.apply(
        lambda row:
        exclusion_case(
            row['PatientDateOfBirth'],
            row['StudentStatus'],
            row['IsPreAuthRequired'],
            row['AgeMax'],
            row['AgeMaxStudent'],
            row['WaitPeriod'],
            row['LifeTimeMaxValue'],
            row['LifeTimeRemainingValue']
        ),
        axis=1
    )

    # All datetime columns
    datetime_columns = [
        'CreatedOn',
        'CreatedOn.1',
        'EligibilityCheckRequestedOn',
        'EligibilityEndCheck',
        'EligibilityStartCheck',
        'PlanBenefitsEnd',
        'PlanBenefitsStart',
        'SubscriberPlanEffectiveDateStart',
        'SubscriberPlanEffectiveDateEnd',
        'SubscriberDOB',
        'UpdatedOn',
        'UpdatedOn.1',
    ]

    # Remove any that are in saved_columns
    datetime_columns = list(set(datetime_columns).difference(saved_columns))
    # Drop datetime columns
    drop_columns(df, datetime_columns)

    # Remove all entries where LifeTimeMaxValue and LifeTimeRemainingValue
    # are null
    df = df[
        (df['LifeTimeMaxValue'].notnull()) &
        (df['LifeTimeRemainingValue'].notnull())
    ]

    # Define columns to be encoded
    encoded_columns = [
        'CoordinationOfBenefits',
        'RelationshipToSubscriber',
        'StudentStatus',
        'SubscriberState'
    ]

    # Encode Coordination of Benefits into three categories:
    # 1: 'one', 2: 'two', and NaN: 'null'
    # We will have to encode these using one-hot-encoding
    df['CoordinationOfBenefits'].replace(
        to_replace=[np.NaN, 1.0, 2.0],
        value=['null', 'one', 'two'],
        inplace=True
    )

    # Replace True/False in WaitPeriod with 1/0
    df['WaitPeriod'].replace(
        to_replace=[True, False],
        value=[1, 0],
        inplace=True
    )

    # Convert remaining object columns, except encoded_columns to binary
    binary_columns = [
        column
        for column in sorted(df.columns)
        if df[column].dtype == 'object' and column
    ]
    binary_columns = list(set(binary_columns).difference(encoded_columns))
    df_binary = df[binary_columns].notnull().astype('uint8')
    drop_columns(df, binary_columns)
    df = pd.concat([df, df_binary], axis=1)

    # Create the target vector
    df['EDI_only'] = [
        1
        if (row['LifeTimeMaxValue'] == row['LifetimeMax']
            and row['LifeTimeRemainingValue'] == row['LifetimeRemaining']
            and not row['Exclusion'])
        else
        0
        for idx, row in df.iterrows()
    ]

    # Drop the OF columns used to define the success criteria
    df.drop('LifetimeMax', axis=1, inplace=True)
    df.drop('LifetimeRemaining', axis=1, inplace=True)

    # Replace null values with the median value of the column
    df.fillna(df.median(), inplace=True)

    # Perform one-hot-encoding on remaining object columns
    df_encoded = pd.get_dummies(df, sparse=False)

    return df_encoded


def test_feature_impute(df, train_df):
    """ A function to clean the test data and impute based on the training data

    Args:
        df (Pandas DataFrame object): the dataframe containing the data to
                                      be cleaned.
        train_df (Pandas DataFrame object): the dataframe containing the data
                                            to impute from.
    Returns:
        test_df (Pandas DataFrame object): dataframe containing the extracted
                                           test data
        sanity_df (Pandas DataFrame object): dataframe containing sanity check
                                             columns (useful for determining
                                             cause of false positives)
    """

    # Filter out everything but MetLife claims
    df = df[df['CarrierName'] == 'MetLife']

    # Drop the 'CarrierName' column since we're only looking at MetLife
    df.drop('CarrierName', axis=1, inplace=True)

    # Convert PatientDateOfBirth to Patient Age
    df['PatientAge'] = df['PatientDateOfBirth'].apply(
        lambda row: int(
            (date.today() - datetime.strptime(row, '%m/%d/%Y').date()).days / 365.25
        )
    )

    # Check for exclusion cases
    df['Exclusion'] = df.apply(
        lambda row:
        exclusion_case(
            row['PatientDateOfBirth'],
            row['StudentStatus'],
            row['IsPreAuthRequired'],
            row['AgeMax'],
            row['AgeMaxStudent'],
            row['WaitPeriod'],
            row['LifeTimeMaxValue'],
            row['LifeTimeRemainingValue']
        ),
        axis=1
    )

    # Create the target vector
    df['EDI_only'] = [
        1
        if (row['LifeTimeMaxValue'] == row['LifetimeMax']
            and row['LifeTimeRemainingValue'] == row['LifetimeRemaining']
            and not row['Exclusion'])
        else
        0
        for idx, row in df.iterrows()
    ]

    # Drop the OF columns used to define the success criteria
    df.drop('LifetimeMax', axis=1, inplace=True)
    df.drop('LifetimeRemaining', axis=1, inplace=True)

    # Define columns to be encoded
    encoded_columns = [
        'CoordinationOfBenefits',
        'RelationshipToSubscriber',
        'StudentStatus',
        'SubscriberState'
    ]

    # Encode Coordination of Benefits into three categories:
    # 1: 'one', 2: 'two', and NaN: 'null'
    # We will have to encode these using one-hot-encoding
    df['CoordinationOfBenefits'].replace(
        to_replace=[np.NaN, 1.0, 2.0],
        value=['null', 'one', 'two'],
        inplace=True
    )

    # Replace True/False in WaitPeriod with 1/0
    df['WaitPeriod'].replace(
        to_replace=[True, False],
        value=[1, 0],
        inplace=True
    )

    # Convert remaining object columns, except encoded_columns to binary
    binary_columns = [
        column
        for column in sorted(df.columns)
        if df[column].dtype == 'object' and column
    ]
    binary_columns = list(set(binary_columns).difference(encoded_columns))
    df_binary = df[binary_columns].notnull().astype('uint8')
    drop_columns(df, binary_columns)
    df = pd.concat([df, df_binary], axis=1)

    # Perform one-hot-encoding on remaining object columns
    df_encoded = pd.get_dummies(df, sparse=False)

    # Setup blank test data dataframe
    test_df = pd.DataFrame(columns=train_df.columns)

    # Assign all existing data from imported dataframe to test dataframe
    for column in train_df.columns:
        if column in df_encoded.columns:
            test_df[column] = df_encoded[column]
        else:
            test_df[column] = 0

    # Replace null values with median of training data
    test_df.fillna(train_df.median(), inplace=True)

    return test_df

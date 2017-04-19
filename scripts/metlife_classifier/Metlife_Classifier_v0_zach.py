import pandas as pd
import numpy as np
import datetime

## Drop columns helper function
#   Input:
#       * dataframe
#       * list of column names to drop
#   Output:
#       * No return - just drops columns from dataframe
def drop_columns(df, columns):
    for column in columns:
        # Check to make sure the column is in the dataframe
        if column in df.columns:
            df.drop(column, axis=1, inplace=True)


## Build dataset
#   Input:
#       * location of SQL query csv file 
#       * parsed HTML csv file
#   Output:
#       * dataset from join of HTML + SQL
def build_set(sql_loc, html_loc):
    # Load in the SQL query csv file
    df_query = pd.read_csv(
        sql_loc,
        low_memory=False,
        encoding='ISO-8859-1'
    )
    
    # Load in the parsed HTML csv file
    df_html = pd.read_csv(
        html_loc,
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
        df_html[col] = df_html[col].str.translate({ord(','): None}).apply(float)

    # Drop some columns that are blank in HTML before determining query columns to merge
    # Info for these columns is more complete in query
    html_col_drop1 = [
        'GroupName',
        'SubscriberSSN'
    ]

    drop_columns(df_html, html_col_drop1)
    
    # Extract columns from query that are not in html
    query_columns = list(set(df_query.columns)-set(df_html.columns))
    query_columns.append('InsurancePolicyPatientEligibilityId')
    
    # Drop entries that with no InsurancePolicyPatientEligibilityId from HTML parsed data
    df_html = df_html[df_html['InsurancePolicyPatientEligibilityId'].notnull()]

    ## Parse out file containing duplicate patient IDs
    patientID = df_html['InsurancePolicyPatientEligibilityId']

    # Count occurance of individual patient IDs on uncleaned data
    from collections import Counter
    patientIDcnt = Counter(patientID)

    # Loop to add up total duplicate checks & save index of duplicate IDs
    patcntlist = list(patientIDcnt.values())
    dupIDindex = [i for i in range(len(patcntlist)) if patcntlist[i] >1]

    # Pull duplicate patient IDs using index from above
    IDlist = list(patientIDcnt.keys())
    dupIDlist = [IDlist[i] for i in dupIDindex]

    # Pull full records for duplicate patient IDs
    duplicate = pd.DataFrame()
    for i in range(len(dupIDlist)):
        loopdata = df_html.loc[lambda df: df.InsurancePolicyPatientEligibilityId == dupIDlist[i]]
        duplicate = duplicate.append(loopdata)

    # Get indexes of duplicate values
    dropind =[duplicate.index[i] for i in range(len(duplicate))]

    # Drop the duplicates based on idexes
    for i in range(len(dropind)):
        df_html.drop(dropind[i],inplace=True)
    ##
        
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


## Exclusion case flagging function
#   Input:
#       * per row values of date of birth, student status, preauthorization, age max, student age max, wait period
#   Output:
#       * True or False to flag row as exclusion case or not
def exclusion_case(dob, student_status, pre_auth, age_max, age_max_student, wait_period):
    # Check that all variables have been passed to function - if not error out
    if (pd.isnull(dob) |
        pd.isnull(student_status) |
        pd.isnull(pre_auth) |
        pd.isnull(age_max) |
        pd.isnull(age_max_student) |
        pd.isnull(wait_period)
    ) == True:
        return True #'MissingValue'
    
    # Age calculation - dob expected as 'Full_Month_Name Day# Year_w_Century'
    age_days = datetime.date.today() - datetime.datetime.strptime(dob, '%m/%d/%Y').date()
    age = round(age_days.days/365.25)

    # Student statuses
    student_statuses = ['PartTime', 'FullTime']
    
    if wait_period == True:
        return True #'WaitPeriodException'
    
    elif student_status in student_statuses  and (age >= age_max_student):
        return True #'StudentException'
        
    elif (age >= 18 and age <= 26) or age >= age_max:
        return True #'AgeMax'
        
    elif pre_auth != 0:
        return True #'PreAuth'
        
    return False


## Feature cleaning
#   Input:
#       * ext_type = str : train or test
#       * df = pd.dataframe : dataframe to perform feature imputation on
#   Output:
#       * cleaned and encoded (imputed) dataset
def feature_impute(ext_type,df):
    # Filter out everything but MetLife claims
    df = df[df['CarrierName'] == 'MetLife']
    
    # Since we're only looking at MetLife claimes we can drop the 'CarrierName' column
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
        'PatientDateOfBirth'
    ]
    
    # Drop ID columns. NOTE: we might want to convert these to binary instead
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
        'PatientDateOfBirth'
    ]
    # Remove any that are in saved_columns
    datetime_columns = list(set(datetime_columns).difference(saved_columns))
    # Drop datetime columns
    drop_columns(df, datetime_columns)

    # Remove bad entries
    if ext_type == 'train':
        # Remove all entries where LifeTimeMaxValue and LifeTimeRemainingValue are null
        df = df[
            (df['LifeTimeMaxValue'].notnull()) &
            (df['LifeTimeRemainingValue'].notnull())
        ]
    
    # Define columns to be encoded
    encoded_columns = [
        'CoordinationOfBenefits',
        'RelationshipToSubscriber',
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
    
    if ext_type == 'train':
        # Create the target vector
        targets = [
            1
            if (row['LifeTimeMaxValue'] == row['LifetimeMax']
                and row['LifeTimeRemainingValue'] == row['LifetimeRemaining']
                and row['Exclusion'] == False)
            else
            0
            for idx, row in df.iterrows()
        ]
        
        # Replace null values with the median value of the column
        df.fillna(df.median(), inplace=True)
        
    # Perform one-hot-encoding on remaining object columns
    df_encoded = pd.get_dummies(df, sparse=False)
    
    if ext_type == 'train':
        # Create a new column containing the 'targets' from earlier
        df_encoded['EDI_only'] = targets
    
    return df_encoded

from sklearn.ensemble import ExtraTreesClassifier
from sklearn.externals import joblib  

if __name__ == '__main__':
    sql_loc = '../sql_data/Flat_MetLife_wEDI_SQLv9_EmptyCol.csv'
    html_loc = '../sql_data/MetLife_HTML_EDI_Parsed_201405_201703.csv'
    set_type = 'train'
    classifier_loc = '../executable_results/Trained_Classifier_v1.pkl'
    
    # Create joined dataset
    df_join = build_set(sql_loc, html_loc)
    
    # Check for exclusion cases
    df_join['Exclusion'] = df_join.apply(
        lambda row:
        exclusion_case(
            row['PatientDateOfBirth'],
            row['StudentStatus'],
            row['IsPreAuthRequired'],
            row['AgeMax'],
            row['AgeMaxStudent'],
            row['WaitPeriod']
        ),
        axis=1
    )
    
    # Save joined dataset for sanity check
    df_join.to_csv('../executable_results/Joined_dataset_v0.1.csv', index=False)
    
    # Clean features
    df_imp = feature_impute(set_type,df_join)
    
    if set_type == 'test':
        df_imp = df_imp[df_imp['Exclusion'] == False]
    
    # Save imputed dataset before dropping columns for use in NtBk
    df_imp.to_csv('../executable_results/Imputed_dataset_v0.1.csv', index=False)
    
    # OF generated columns non longer needed after setting targets
    df_imp.drop('LifetimeMax', axis=1, inplace=True)
    df_imp.drop('LifetimeRemaining', axis=1, inplace=True)
    df_imp.drop('Exclusion', axis=1, inplace=True)
    
    if set_type == 'train':
        # Turn dataframe into X and Y array
        Y = df_imp['EDI_only'].values
        X = df_imp[[column for column in df_imp.columns if column != 'EDI_only']].values

        # Train our Random Forest classifier
        clf = ExtraTreesClassifier(bootstrap=True, n_estimators=1000, max_features=None)
        clf.fit(X, Y)

        # Save the classifier
        joblib.dump(clf, classifier_loc)
    elif set_type == 'test':
        # Throw out Exclusion c
       
        # Load classifier
        clf = joblib.load(classifier_loc)
        
        # Turn imputed dataset into matrix
        X = df_imp.values
        
        # Run classifier
        predictions = clf.predict(X)
        
        # Save results of classifier into dataframe
        df_results = df_imp
        df_results['Predict'] = predictions
        
        # Save results to file
        df_results.to_csv('../executable_results/Results_dataset_v0.csv', index=False)
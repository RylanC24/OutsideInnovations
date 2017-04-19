import pandas as pd
import numpy as np
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.externals import joblib
from feature_extraction_utilities import build_set, test_feature_impute


if __name__ == '__main__':
    train_date_range = '20140516_20170331'
    test_date_range = '20170401_20170417'

    # Input data files
    sql_file = '../sql_data/Flat_MetLife_wEDI_SQLv9_EmptyCol.csv'
    test_html_file = '../edi_data/parsed_data/metlife_' + test_date_range + '.csv'
    train_file = '../training_data/input_cleaned_ediHTML_ofSQL_' + train_date_range + '.csv'

    # Output data files
    raw_test_data_file = '../test_data/input_raw_ediHTML_ofSQL_' + test_date_range + '.csv'
    cleaned_test_data_file = '../test_data/input_cleaned_ediHTML_ofSQL_' + test_date_range + '.csv'
    output_file = '../test_data/output_ExtraTrees_nf1000_' + test_date_range + '.csv'

    # Serialized classifier output file
    classifier_file = '../trained_classifiers/ExtraTrees_nf1000_' + train_date_range + '.pkl'

    # Create joined dataset
    test_df = build_set(sql_file, test_html_file)
    train_df = pd.read_csv(train_file, low_memory=False)

    # Save joined dataset for sanity check
    test_df.to_csv(raw_test_data_file, index=False)

    # Clean features
    test_df = test_feature_impute(test_df, train_df)

    # Save imputed dataset before dropping columns for use in NtBk
    test_df.to_csv(cleaned_test_data_file, index=False)

    # Transform the targets into a numpy array
    Y = test_df['EDI_only'].values
    # Transform input data into numpy ndarray
    X = test_df[
        [
            column
            for column in train_df.columns
            if column not in ['EDI_only', 'Exclusion']
        ]
    ].values

    # Load the classifier
    clf = joblib.load(classifier_file)

    # Test the classifier
    predictions = clf.predict(X)

    # Save results of classifier into dataframe
    df_results = test_df
    df_results['Predict'] = predictions

    # Save results to file
    df_results.to_csv(output_file, index=False)

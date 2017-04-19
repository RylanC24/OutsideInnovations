import pandas as pd
import numpy as np
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.externals import joblib
from feature_extraction_utilities import build_set, train_feature_impute


if __name__ == '__main__':
    train_date_range = '20140516_20170331'

    # Input data files
    sql_file = '../sql_data/Flat_MetLife_wEDI_SQLv9_EmptyCol.csv'
    train_html_file = '../edi_data/parsed_data/metlife_' + train_date_range + '.csv'

    # Output data files
    raw_training_data_file = '../training_data/input_raw_ediHTML_ofSQL_' + train_date_range + '.csv'
    cleaned_training_data_file = '../training_data/input_cleaned_ediHTML_ofSQL_' + train_date_range + '.csv'

    # Serialized classifier output file
    classifier_file = '../trained_classifiers/ExtraTrees_nf1000_' + train_date_range + '.pkl'

    # Create joined dataset
    train_df = build_set(sql_file, train_html_file)

    # Save joined dataset for sanity check
    train_df.to_csv(raw_training_data_file, index=False)

    # Clean features
    train_df = train_feature_impute(train_df)

    # Save imputed dataset before dropping columns for use in NtBk
    train_df.to_csv(cleaned_training_data_file, index=False)

    # Transform the targets into a numpy array
    Y = train_df['EDI_only'].values
    # Transform input data into numpy ndarray
    X = train_df[
        [
            column
            for column in train_df.columns
            if column not in [
                                'EDI_only',
                                'Exclusion',
                                'InsurancePolicyPatientEligibilityId'
                            ]
        ]
    ].values

    # Train our Random Forest classifier
    clf = ExtraTreesClassifier(
        bootstrap=True,
        n_estimators=1000,
        max_features=None
    )
    clf.fit(X, Y)

    # Save the classifier
    joblib.dump(clf, classifier_file)

    # Test the classifier
#    predictions = clf.predict(X)

    # Save results of classifier into dataframe
#    df_results = df_imp
#    df_results['Predict'] = predictions
#    df_results['Target'] = Y
#    df_results['Exclusion'] = Exclusions

    # Save results to file
#    df_results.to_csv(test_data_file, index=False)

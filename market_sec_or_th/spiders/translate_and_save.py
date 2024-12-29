from doctor_trans import trans
from market_sec_or_th.spiders.market_sec_thailand import df_cleaner
import pandas as pd
import sys

if __name__ == '__main__':
    # Get the filenames from command-line arguments
    if len(sys.argv) < 3:
        print("Usage: python translate_and_save.py <native_excel_file> <translated_excel_file>")
        sys.exit(1)

    native_filename = sys.argv[1]  # The first argument is the native file path
    translated_filename = sys.argv[2]  # The second argument is the translated file path
    source_lang = sys.argv[3]  # The third argument is the native language code

    # Read Native Excel file
    native_data_df = pd.read_excel(io=native_filename, engine='calamine')
    native_data_df.drop(columns='id', axis=1, inplace=True)  # Drop 'id' column from native_df

    # Translate the DataFrame to English and return translated DataFrame
    tranlated_df = trans(native_data_df, input_lang=source_lang, output_lang='en')  # Change the input-lang when required

    # Clean the df
    cleaned_tranlated_df = df_cleaner(data_frame=tranlated_df)  # Apply the function to all columns for Cleaning

    # Write translated_df in Excel file
    print("Creating Translated sheet...")
    with pd.ExcelWriter(path=translated_filename, engine='xlsxwriter', engine_kwargs={"options": {'strings_to_urls': False}}) as writer:
        cleaned_tranlated_df.insert(loc=0, column='id', value=range(1, len(tranlated_df) + 1))  # Add 'id' column at position 1
        cleaned_tranlated_df.to_excel(excel_writer=writer, index=False)
    print("Translated Excel file Successfully created.")

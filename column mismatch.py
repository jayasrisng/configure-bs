import logging
import os
import pandas as pd
import glob
from xror import XROR

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()])

# Path to the root folder containing subdirectories with XROR files
root_folder_path = 'D:\\Beat Saber\\saber'

# Recursively find all .xror files
xror_files = glob.glob(os.path.join(root_folder_path, '**/*.xror'), recursive=True)

print(f"Found {len(xror_files)} XROR files to process.")

# Process each XROR file
for index, file_path in enumerate(xror_files):
    try:
        print(f"Processing file {index + 1}/{len(xror_files)}: {file_path}")

        with open(file_path, 'rb') as f:
            binary_data = f.read()

        # Unpack the XROR file
        xror_data = XROR.unpack(binary_data)

        # Dynamically set columns based on unpacked data
        frame_columns = ['Column' + str(i) for i in range(len(xror_data.data['frames'][0]))]

        # Create DataFrame
        df_frames = pd.DataFrame(xror_data.data['frames'], columns=frame_columns)

        # Define CSV file path
        base_name = os.path.splitext(file_path)[0]
        csv_file_path = base_name + '.csv'

        # Save to CSV
        df_frames.to_csv(csv_file_path, index=False)
        print(f"Successfully saved: {csv_file_path}")

    except Exception as e:
        print(f"Error processing {file_path}: {e}")

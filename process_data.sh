#!/bin/bash

# Define variables
S3_BUCKET="ptde2403-mbd-predict-jackson-mothapo-s3-source"          
MOUNT_POINT="/home/ubuntu/s3-mount"                      # Mount point for the S3 bucket
SCRIPT_DIR="/home/ubuntu/"              
OUTPUT_DIR="$MOUNT_POINT/Output" 

# Create the directories if they don't already exist
sudo mkdir -p $OUTPUT_DIR
sudo mkdir -p $SCRIPT_DIR


# Mount the S3 bucket
echo "Mounting S3 bucket..."
s3fs $S3_BUCKET $MOUNT_POINT -o allow_other -o iam_role=auto -o nonempty

# Check if the bucket was mounted successfully
if mount | grep $MOUNT_POINT > /dev/null; then
    echo "S3 bucket mounted successfully."
else
    echo "Failed to mount S3 bucket."
    exit 1
fi

# Run the Python data processing script
echo "Running Python data processing script..."
python3 $SCRIPT_DIR/data_loading.py

# Check if the Python script executed successfully
if [ $? -eq 0 ]; then
    echo "Data processing completed successfully."
else
    echo "Data processing failed."
    exit 1
fi

# Move the output CSV to the Output folder on S3
echo "Moving the output CSV to S3..."
mv historical_stock_data.csv $OUTPUT_DIR/

# Unmount the S3 bucket
echo "Unmounting S3 bucket..."
fusermount -u $MOUNT_POINT

echo "Processing complete!"
#!/bin/bash

# Source Blob Storage Details
SOURCE_ACCOUNT_NAME=""
SOURCE_ACCOUNT_KEY=""
SOURCE_CONTAINER_NAME=""

# Destination Blob Storage Details
DEST_ACCOUNT_NAME=""
DEST_ACCOUNT_KEY=""
DEST_CONTAINER_NAME=""


# List all the .parquet files in the source blob storage with the specific path pattern
FILES=$(az storage blob list --account-name $SOURCE_ACCOUNT_NAME --container-name $SOURCE_CONTAINER_NAME \
                            --account-key $SOURCE_ACCOUNT_KEY --prefix "hotel-weather/year=2017/month=09/" \
                            --query "[?contains(name, '.parquet')].name" -o tsv)

for FILE in $FILES; do
    expiry_date=$(date -v+1d '+%Y-%m-%dT%H:%MZ')

    # Generate SAS tokens
    SOURCE_SAS=$(az storage blob generate-sas --account-name $SOURCE_ACCOUNT_NAME --account-key $SOURCE_ACCOUNT_KEY \
                                              --container-name $SOURCE_CONTAINER_NAME --name $FILE --permissions r \
                                              --expiry $expiry_date --output tsv)
    DEST_SAS=$(az storage blob generate-sas --account-name $DEST_ACCOUNT_NAME --account-key $DEST_ACCOUNT_KEY \
                                            --container-name $DEST_CONTAINER_NAME --name $FILE --permissions w \
                                            --expiry $expiry_date --output tsv)

    # Copy each file using the SAS tokens
    azcopy copy "https://${SOURCE_ACCOUNT_NAME}.blob.core.windows.net/${SOURCE_CONTAINER_NAME}/${FILE}?${SOURCE_SAS}"\
                "https://${DEST_ACCOUNT_NAME}.blob.core.windows.net/${DEST_CONTAINER_NAME}/${FILE}?${DEST_SAS}"

    sleep 10 # 10 seconds delay
done



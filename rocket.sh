DATA_DIR="/home/dev/28_03_rocket/business_case_rocket_25/data"
RES_DIR="/home/dev/28_03_rocket/res_files"

echo "Starting copirovanie files..."
cp -r "$DATA_DIR"/* "$RES_DIR"

echo "The end of the process. Check $RES_DIR"

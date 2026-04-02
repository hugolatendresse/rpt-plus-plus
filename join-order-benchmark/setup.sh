#!/bin/bash

# Exit immediately if a pipeline returns a non-zero status
set -e

cd /mnt/local_ssd/spy/join-order-benchmark

echo "Downloading IMDB dataset..."
wget https://bonsai.cedardb.com/job/imdb.tgz

echo "Extracting IMDB dataset..."
tar -xvzf imdb.tgz

echo "Initializing schema and loading data into job.db..."
../build/release/duckdb job.db <<EOF
.read schema.sql

COPY aka_name FROM 'aka_name.csv' (DELIMITER ',', ESCAPE '\');
COPY aka_title FROM 'aka_title.csv' (DELIMITER ',', ESCAPE '\');
COPY cast_info FROM 'cast_info.csv' (DELIMITER ',', ESCAPE '\');
COPY char_name FROM 'char_name.csv' (DELIMITER ',', ESCAPE '\');
COPY comp_cast_type FROM 'comp_cast_type.csv' (DELIMITER ',', ESCAPE '\');
COPY company_name FROM 'company_name.csv' (DELIMITER ',', ESCAPE '\');
COPY company_type FROM 'company_type.csv' (DELIMITER ',', ESCAPE '\');
COPY complete_cast FROM 'complete_cast.csv' (DELIMITER ',', ESCAPE '\');
COPY info_type FROM 'info_type.csv' (DELIMITER ',', ESCAPE '\');
COPY keyword FROM 'keyword.csv' (DELIMITER ',', ESCAPE '\');
COPY kind_type FROM 'kind_type.csv' (DELIMITER ',', ESCAPE '\');
COPY link_type FROM 'link_type.csv' (DELIMITER ',', ESCAPE '\');
COPY movie_companies FROM 'movie_companies.csv' (DELIMITER ',', ESCAPE '\');
COPY movie_info FROM 'movie_info.csv' (DELIMITER ',', ESCAPE '\');
COPY movie_info_idx FROM 'movie_info_idx.csv' (DELIMITER ',', ESCAPE '\');
COPY movie_keyword FROM 'movie_keyword.csv' (DELIMITER ',', ESCAPE '\');
COPY movie_link FROM 'movie_link.csv' (DELIMITER ',', ESCAPE '\');
COPY name FROM 'name.csv' (DELIMITER ',', ESCAPE '\');
COPY person_info FROM 'person_info.csv' (DELIMITER ',', ESCAPE '\');
COPY role_type FROM 'role_type.csv' (DELIMITER ',', ESCAPE '\');
COPY title FROM 'title.csv' (DELIMITER ',', ESCAPE '\');
EOF

echo "Database initialization and data loading complete."
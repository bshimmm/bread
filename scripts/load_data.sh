#!/bin/bash

## example: ./setup_db.sh bolt://localhost:7687 neo4j ftphl

host="${1}" ## example: bolt://localhost:7687
username="${2}"
password="${3}"
setup_schema="${4}"
home_dir="${5}"
db_home_dir="${6}"


if [ "${home_dir}" == "" ]; then
  home_dir="."
fi

if [ "${db_home_dir}" == "" ]; then
  db_home_dir="/apps/neo4j"
fi
db_home_import_dir="${db_home_dir}/import"

is_schema_setup_success="true"
#home_dir="/Users/hthai00/Google\ Drive/projects/bread/"
eval cd $home_dir

if [ "${setup_schema}" == "true" ]; then
  echo "Loading db schema..."
  cat scripts/cypher/schema.cyp | ${db_home_dir}/bin/cypher-shell --debug --format verbose -a $host -u $username -p $password
  if [ $? -ne 0 ]; then
    echo "Error loading schema!"
    is_schema_setup_success="false"
  fi
fi

if [ "${is_schema_setup_success}" == "true" ]; then
  echo "Removing previous main data in ${db_home_import_dir}..."
  eval rm -f "${db_home_import_dir}/*.csv "
  echo "Copying main data to ${db_home_import_dir} to prepare for data load..."
  eval cp -f data/*.csv "${db_home_import_dir}"
  echo "Loading main data..."
  cat scripts/cypher/main_data.cyp | ${db_home_dir}/bin/cypher-shell --debug --format verbose -a $host -u $username -p $password
  # echo "Loading role and permission data..."
  # cat scripts/cypher/load_role.cyp | /apps/neo4j/bin/cypher-shell --debug --format verbose -a $host -u $username -p $password
  # echo "Loading user data..."
  # cat scripts/cypher/load_user.cyp | /apps/neo4j/bin/cypher-shell --debug --format verbose -a $host -u $username -p $password
  # echo "Loading auth data..."
  # cat scripts/cypher/load_auth.cyp | /apps/neo4j/bin/cypher-shell --debug --format verbose -a $host -u $username -p $password
else
  echo "Data loading skipped!"
fi

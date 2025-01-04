#!/bin/bash

rm result.sql

cp template.sql result.sql

file=$1

if [ -z "$file" ]; then
  echo "Please provide a file name in ./data/ as the first argument."
  exit 1
fi

current_file=$(pwd)
current_file+="/data/"
current_file+=$file
echo "data file : $current_file"

escaped_file=$(printf "%s" "$current_file" | sed 's#/#\\/#g')
sed -i 's/WORK_FILE/'"$escaped_file"'/g' result.sql


num=$2

if [ -z "$num" ]; then
  echo "Please provide a number as the second argument."
  exit 1
fi

focus=""

for (( i=1; i<=num; i++ )); do
  focus+="c$(printf "%02d" $i),"
done

focus=${focus%,}

echo "focus columns: $focus"

focusCols=$(printf "%s" "$focus" | sed 's#/#\\/#g')
sed -i 's/focusCols/'"$focusCols"'/g' result.sql


basename=""
output="output.txt"
rm ${output}
for i in {0..10} ; do
    inputfile="/home/zx/workspace/macrobase/bin/order/TrednDiffDemo${i}.sql"
    echo "==========================    i = ${i}       ============================" >> $output
    ./macrobase-sql -f ${basename}${inputfile} -q true >> ${output}
done
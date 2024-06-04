for i in {3..8}
do 
	./build/run ../../data/mini_sample_query_workload/1.txt $i 
	./build/run ../../data/mini_sample_query_workload/2.txt $i 
	./build/run ../../data/mini_sample_query_workload/3.txt $i 
	./build/run ../../data/mini_sample_query_workload/4.txt $i 
	./build/run ../../data/mini_sample_query_workload/5.txt $i 
done  

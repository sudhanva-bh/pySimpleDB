python3 benchmark.py --query all --mode opt > opt_results.txt
python3 benchmark.py --query Q2 --mode index > q2_index_results.txt
python3 benchmark.py --query Q3 --mode index > q3_index_results.txt
python3 benchmark.py --query Q2 --mode baseline > q2_baseline_results.txt
python3 benchmark.py --query Q3 --mode baseline > q3_baseline_results.txt
cat opt_results.txt
echo "--- Q2 Index ---"
cat q2_index_results.txt
echo "--- Q3 Index ---"
cat q3_index_results.txt
echo "--- Q2 Baseline ---"
cat q2_baseline_results.txt
echo "--- Q3 Baseline ---"
cat q3_baseline_results.txt

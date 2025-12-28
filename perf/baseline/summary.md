# Performance Summary

## Ion vs JSON (text)
Query | Ion CPU | JSON CPU | CPU Ratio | Ion Lat | JSON Lat | Lat Ratio
--- | --- | --- | --- | --- | --- | ---
COUNT(*) | 0.018s | 0.001s | 15.34x | 0.047s | 0.018s | 2.59x
Project 3 cols | 0.025s | 0.002s | 14.35x | 0.049s | 0.018s | 2.71x
Filter + group | 0.025s | 0.002s | 13.32x | 0.050s | 0.018s | 2.81x
Wide project (4 cols) | 0.090s | 0.008s | 10.69x | 0.183s | 0.082s | 2.22x

## Ion vs JSON (parallel newline-delimited)
Query | Ion CPU | JSON CPU | CPU Ratio | Ion Lat | JSON Lat | Lat Ratio
--- | --- | --- | --- | --- | --- | ---
COUNT(*) | 0.022s | 0.001s | 19.31x | 0.050s | 0.017s | 2.88x
Project min | 0.019s | 0.001s | 12.79x | 0.047s | 0.018s | 2.66x

## Ion binary vs JSON (text)
Query | Ion CPU | JSON CPU | CPU Ratio | Ion Lat | JSON Lat | Lat Ratio
--- | --- | --- | --- | --- | --- | ---
COUNT(*) | 0.008s | 0.001s | 6.58x | 0.020s | 0.018s | 1.11x
Project 3 cols | 0.013s | 0.002s | 7.50x | 0.025s | 0.018s | 1.37x
Wide project (4 cols) | 0.025s | 0.008s | 3.01x | 0.051s | 0.082s | 0.62x

## Ion text vs binary
Query | Text CPU | Binary CPU | Speedup | Text Lat | Binary Lat | Speedup
--- | --- | --- | --- | --- | --- | ---
COUNT(*) | 0.018s | 0.008s | 2.33x | 0.047s | 0.020s | 2.33x
Project 3 cols | 0.025s | 0.013s | 1.91x | 0.049s | 0.025s | 1.98x
Wide COUNT(*) | 0.070s | 0.008s | 8.70x | 0.163s | 0.034s | 4.77x
Wide project (4 cols) | 0.090s | 0.025s | 3.56x | 0.183s | 0.051s | 3.56x

## Ion vs JSON (write)
Query | Ion CPU | JSON CPU | CPU Ratio | Ion Lat | JSON Lat | Lat Ratio
--- | --- | --- | --- | --- | --- | ---
Write | 0.033s | 0.003s | 9.93x | 0.037s | 0.007s | 5.08x
Write wide | 0.165s | 0.025s | 6.55x | 0.184s | 0.044s | 4.21x

## Ion write text vs binary
Query | Text CPU | Binary CPU | Speedup | Text Lat | Binary Lat | Speedup
--- | --- | --- | --- | --- | --- | ---
Write | 0.033s | 0.022s | 1.53x | 0.037s | 0.024s | 1.56x
Write wide | 0.165s | 0.139s | 1.19x | 0.184s | 0.142s | 1.29x

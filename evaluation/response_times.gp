set terminal png
set output "evaluation/response_times.png"
set title "Data Visualization"
set xlabel "Response Time (ms)"
set ylabel "Frequency"
# Set style of the histogram
set style data histogram
set style histogram cluster gap 1
set style fill solid border -1
# Set bin size
bin_width = 5.0
bin_number(x) = floor(x/bin_width)
rounded(x) = bin_width * ( bin_number(x) + 0.5 )

# Set up multiplot
set multiplot layout 1,2

# Plot histogram for Put Requests
set title "Put Requests"
plot "evaluation/data/put_performance_test.csv" using (rounded($1)):(1.0) smooth freq with boxes

# Plot histogram for Get Requests
set title "Get Requests"
plot "evaluation/data/get_performance_test.csv" using (rounded($1)):(1.0) smooth freq with boxes

unset multiplot
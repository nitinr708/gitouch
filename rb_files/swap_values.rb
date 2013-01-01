STDIN.each_line do |line|
  row = line.chomp.split('\t')
  key = row[0]
  values = row.shift
  swapped_values = []
  values.length.times do 
    swapped_values.push(values.pop)
  end
  puts "#{key}:#{swapped_values.join(':')}"
end

 


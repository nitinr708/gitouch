#!/usr/local/bin/ruby
DELIMITER = "\001"
DELIMITER2 = "\t"
load_ts = ENV['LOAD_TS'].to_s
$load_ts = load_ts.gsub(/__/, ' ')
$output_row = nil
$records = {}
$records['left'] = []
$records['right'] = []

current_key = nil
prev_key = nil

def output_with_loadts(row=nil)
  row.insert(0,$load_ts)
  puts "#{row.join(DELIMITER)}"
  $output_row=nil
end

def output(row=nil)
  puts "#{row.join(DELIMITER)}"
  $output_row=nil
end

def combine
  # Getting both side values for the same key and replacing left row with the right one if it exists else not replacing.
  return if($records['left'].empty? and $records['right'].empty?)

  #If left table doesn't have rows that are newly added in right table
  if $records['left'].empty? and !$records['right'].empty?
    $output_row=$records['right'].first.split(DELIMITER)
    output_with_loadts($output_row)
  else  #If left table value has key common with right table value
    $records['left'].each do |left_row|
    if $records['right'].empty?
      $output_row=left_row.split(DELIMITER) 
      output($output_row)
    else
      $output_row=$records['right'].first.split(DELIMITER)  #Replacing old values of left table with ones from right table
      output_with_loadts($output_row)
    end
  end

    $records['left'] = []
    $records['right'] = []
  end
end

STDIN.each_line do |line|
  next if line.nil? || line.chomp.empty?
  begin
    key, file, val = line.chomp.split(DELIMITER2, 3)
    current_key = key
    #Ignore null keys
    if file.eql?('left') || file.eql?('right')
      if prev_key && (prev_key != current_key.gsub(/\s/,''))
        combine()
        #combine() unless $records['left'].empty?
      end
      prev_key = current_key.gsub(/\s/,'')
      $records[file.to_s] << val.to_s
    else
      STDERR.puts 'Input data format is not valid'
    end		
  rescue Exception => e
    STDERR.puts e.backtrace
    STDERR.puts e.message
    break
  end
end

#Handling last key
#combine() unless ($records['left'].empty?)

combine()


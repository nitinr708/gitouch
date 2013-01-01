# Ruby code to evaluate a number for primality testing, i.e. if it is prime or not.
# output is either "composite" or "prime"

# Inputs: 
# 1. The number n
# 2. Accuracy / No of tests to be performed k


# Functions:
# 1. IsEven? x : Returns true / false according to a number 'x' is divisible by 2

def isEven?(x=nil)
 ((x % 2)==0 ? true : false) unless x.nil?
end 

n = ARGV[0].to_i
k = ARGV[1].to_i
target = n-1

# Loop1 : finding value for d, traversing between s = 1 to target/2
# according to the formulae d = target / 2^s .

to_print=nil
isEven?(n) ? to_print="n(#{n}) is EVEN!" : to_print="n(#{n}) is ODD.."
puts "#{to_print}"
isEven?(k) ? to_print="k(#{k}) is EVEN!" : to_print="k(#{k}) is ODD.."
puts "#{to_print}"
isEven?(target) ? to_print="target(#{target}) is EVEN!" : to_print="target(#{target}) is ODD.."
puts "#{to_print}"



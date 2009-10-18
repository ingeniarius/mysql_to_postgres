# Run me with:
#   watchr specs.watchr
#
# Install watchr:
#  gem install watchr --source http://gemcutter.org

def run_tests
  system 'clear && rake test'
end

# Watchr Rules
watch('.*\.rdoc')    { |m| system "rdoc #{m[0]}" }
watch('test/.*\.rb') { |m| run_tests }

# Signal Handling
Signal.trap('QUIT') { run_tests }    # ctrl-\
Signal.trap('INT')  { abort("\n") }  # ctrl-c

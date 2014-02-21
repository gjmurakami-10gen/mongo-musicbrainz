#!/usr/bin/env ruby
gem "bson", "~> 2.2.1"
require 'bson'
require 'json'

USAGE = "usage: {$0} [json_file] > bson_file"

abort("usage error - output must be redirected\n#{USAGE}") if $stdout.isatty

# assumes exactly one JSON document per line

ARGF.each_line do |line|
  $stdout.write JSON.parse(line).to_bson
end

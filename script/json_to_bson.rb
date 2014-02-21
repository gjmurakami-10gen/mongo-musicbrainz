#!/usr/bin/env ruby
gem "bson", "~> 2.2.1"
require 'bson'
require 'json'

USAGE = "usage: {$0} [json_file] > bson_file"

abort("Usage error - output must be redirected\n#{USAGE}") if $stdout.isatty

ARGF.each_line do |line|
  $stdout.write JSON.parse(line).to_bson
end

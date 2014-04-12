#!/usr/bin/env ruby
require 'mongo'
require 'benchmark'

puts '40 most-recorded titles'

collection_name = 'recording'

pipeline = [
  {'$match' => {'track' => {'$type' => 3}}},
  {'$project' => {'name' => 1, 'count' => {'$size' => '$track'}}},
  {'$group' => {'_id' => '$name', 'count' => {'$sum' => '$count'}}},
  {'$sort' => {'count' => -1}},
  {'$limit' => 40}
]

collection = Mongo::MongoClient.from_uri.db[collection_name]
result = []
tms = Benchmark.measure do
  result = collection.aggregate(pipeline, :cursor => {}, :allowDiskUse => true).to_a
end
result.each{|doc| puts "    #{'%-18s' % doc['_id']}#{'%7d' % doc['count']}"}
puts "real: #{tms.real.round}"
# 179 seconds (all storage), 102 seconds (part storage?) - 2.6 GHz Intel Core i7, MacBookPro11,3

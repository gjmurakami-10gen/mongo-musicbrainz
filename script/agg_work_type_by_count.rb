#!/usr/bin/env ruby
require 'mongo'
require 'benchmark'

puts 'work type by count'

collection_name = 'work'

pipeline = [
  {'$project' => {'type' => '$type.name'}}, # {'$project' => {'name' => 1, 'type' => '$type.name'}},
  {'$group' => {'_id' => '$type', 'count' => {'$sum' => 1}}},
  {'$sort' => {'count' => -1}},
]

collection = Mongo::MongoClient.from_uri.db[collection_name]
result = []
tms = Benchmark.measure do
  result = collection.aggregate(pipeline, :cursor => {}, :allowDiskUse => true).to_a
end
result.each{|doc| puts "    #{'%-18s' % doc['_id']}#{'%7d' % doc['count']}"}
puts "real: #{tms.real.round}"
# 4 seconds (storage), 1 second (collection in memory) - 2.6 GHz Intel Core i7, MacBookPro11,3

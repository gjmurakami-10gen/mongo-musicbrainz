#!/usr/bin/env ruby
require 'mongo'
require 'benchmark'

puts '40 countries with the most artists'

collection_name = 'artist'

pipeline = [
  {'$match' => {'area.type.name' => 'Country'}},
  {'$project' => {'country' => '$area.sort_name'}},
  {'$group' => {'_id' => '$country', 'count' => {'$sum' => 1}}},
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
# 30 seconds (storage), 1 second (collection in memory) - 2.6 GHz Intel Core i7, MacBookPro11,3

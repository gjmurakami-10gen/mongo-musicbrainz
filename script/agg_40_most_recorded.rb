#!/usr/bin/env ruby
require 'mongo'
require 'pp'
require 'benchmark'

mongo_client = Mongo::MongoClient.from_uri
mongo_uri = Mongo::URIParser.new(ENV['MONGODB_URI'])
db = mongo_client[mongo_uri.db_name]

collection_name = 'recording'
collection = db[collection_name]

title = '40 most-recorded recording names'

max_id = 1_000_000
limit = 40

pipeline = [
  #{'$match' => {'_id' => {'$lte' => max_id}}},
  {'$project' => {'name' => 1, 'track' => 1 } },
  {'$unwind' => '$track'},
  {'$group' => {'_id' => '$name', 'count' => {'$sum' => 1}}},
  {'$sort' => {'count' => -1}},
  {'$limit' => limit}
]

#puts "collection count: #{collection.count}" # 13_312_436
#puts "max_id: #{max_id}"
pp db.command({collStats: collection_name})

result = []
tms = Benchmark.measure do
  collection.aggregate(pipeline, :cursor => {}, :allowDiskUse => true).each_slice(limit) do |slice|
    result = slice
    break
  end
end

puts title
pp result
puts "real: #{tms.real.round}" # 110 - MacBook Pro Retina, 15-inch, Late 2013, Processor  2.6 GHz Intel Core i7, MacBookPro11,3

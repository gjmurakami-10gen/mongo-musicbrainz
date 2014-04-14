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

db = Mongo::MongoClient.from_uri.db
collection = db[collection_name]
result = []
tms = Benchmark.measure do
  result = collection.aggregate(pipeline, :cursor => {}, :allowDiskUse => true).to_a
end
result.each{|doc| puts "    #{'%7d' % doc['count']} #{doc['_id']}"}
coll_stats = db.command({collStats: collection_name})
puts "real: #{'%.1f' % tms.real} seconds"
puts "collection size: #{'%.1f' % (coll_stats['size'].to_f/1_000_000_000.0)} GB, count:#{coll_stats['count']}, avgObjSize:#{coll_stats['avgObjSize']}"
# real: 193.1 seconds
# real: 146.7 seconds
# collection size: 8.1 GB
# 2.6 GHz Intel Core i7, MacBookPro11,3

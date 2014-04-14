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
# real: 3.5 seconds
# real: 0.9 seconds
# collection size: 0.2 GB
# 2.6 GHz Intel Core i7, MacBookPro11,3

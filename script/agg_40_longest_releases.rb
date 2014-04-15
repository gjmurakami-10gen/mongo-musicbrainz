#!/usr/bin/env ruby
require 'mongo'
require 'benchmark'

puts '40 longest releases'

collection_name = 'release_group'

pipeline = [
  {'$unwind' => '$release'},
  {'$unwind' => '$release.medium'},
  {'$unwind' => '$release.medium.track'},
  {'$group' => {'_id' => '$release.medium._id',
                'release_group' => {'$first' => '$_id'},
                'name' => {'$first' => '$name'},
                'length' => {'$sum' => '$release.medium.track.length'},
                'count' => {'$sum' => 1}}},
  {'$sort' => {'length' => -1}},
  {'$group' => {'_id' => '$release_group',
                'name' => {'$first' => '$name'},
                'length' => {'$first' => '$length'},
                'count' => {'$first' => '$count'}}},
  {'$sort' => {'length' => -1}},
  {'$limit' => 40}
]

db = Mongo::MongoClient.from_uri.db
collection = db[collection_name]
result = []
tms = Benchmark.measure do
  result = collection.aggregate(pipeline, :cursor => {}, :allowDiskUse => true).to_a
end
result.each{|doc| puts "    #{'%8d' % (doc['length'].to_f/1000.0).round} #{'%3d' % doc['count']} #{doc['name']}"}
coll_stats = db.command({collStats: collection_name})
puts "real: #{'%.1f' % tms.real} seconds"
puts "collection size: #{'%.1f' % (coll_stats['size'].to_f/1_000_000_000.0)} GB, count: #{coll_stats['count']}, avgObjSize: #{coll_stats['avgObjSize']}"
# real: 541.9 second
# real: 515.5 seconds
# collection size: 17.6 GB, count: 1039090, avgObjSize: 16962
# 2.6 GHz Intel Core i7, MacBookPro11,3

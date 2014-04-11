#!/usr/bin/env ruby
require 'mongo'
require 'pp'
require 'benchmark'

mongo_client = Mongo::MongoClient.from_uri
mongo_uri = Mongo::URIParser.new(ENV['MONGODB_URI'])
db = mongo_client[mongo_uri.db_name]

title = 'work type by count'

collection_name = 'work'
collection = db[collection_name]

pipeline = [
  {'$project' => {'type' => '$type.name'}}, # {'$project' => {'name' => 1, 'type' => '$type.name'}},
  {'$group' => {'_id' => '$type', 'count' => {'$sum' => 1}}},
  {'$sort' => {'count' => -1}},
]

puts title
result = []
tms = Benchmark.measure do
  result = collection.aggregate(pipeline, :cursor => {}, :allowDiskUse => true).to_a
end
puts result.collect{|doc| [doc['_id'], doc['count']].join("\t")}.join("\n")
puts "real: #{tms.real.round}" # 99 - MacBook Pro Retina, 15-inch, Late 2013, Processor  2.6 GHz Intel Core i7, MacBookPro11,3

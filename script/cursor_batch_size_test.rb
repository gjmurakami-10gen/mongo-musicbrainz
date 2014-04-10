#!/usr/bin/env ruby
require 'mongo'

mongo_client = Mongo::MongoClient.from_uri
mongo_uri = Mongo::URIParser.new(ENV['MONGODB_URI'])
db = mongo_client[mongo_uri.db_name]

coll = db['release_group']
query = {}
fields = {'_id' => 1}
BATCH_SIZE = 100000 #0
SLICE_SIZE = 20000

module Mongo
  class Cursor
    alias_method :orig_send_initial_query, :send_initial_query
    def send_initial_query
      orig_send_initial_query
      puts "[send_initial_query @n_received:#{@n_received.inspect}]"
    end
    alias_method :orig_send_get_more, :send_get_more
    def send_get_more
      orig_send_get_more
      puts "[send_get_more @n_received:#{@n_received.inspect}]"
    end
  end
end

coll.find(query, :fields => fields, :batch_size => BATCH_SIZE).each_slice(SLICE_SIZE) do |parent_docs|
  print "."
  STDOUT.flush
end

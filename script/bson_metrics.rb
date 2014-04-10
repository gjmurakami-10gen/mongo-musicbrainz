#!/usr/bin/env ruby
gem "bson", "~> 2.2.1"
require 'bson'
require 'benchmark'
require 'json'
include BSON

module BSON
  module Registry
    @hist = ::Array.new(256, 0)
    class << self; attr_accessor :hist; end
    def get(byte)
      @hist[byte.ord] += 1
      MAPPINGS.fetch(byte)
    end
  end
end

class Hash
  @tally = 0
  class << self; attr_accessor :tally; end
  def self.from_bson(bson)
    hash = new
    bson.read(4) # Swallow the first four bytes.
    while (type = bson.readbyte.chr) != NULL_BYTE
      field = bson.gets(NULL_BYTE).from_bson_string.chop!
      @tally += field.size
      hash[field] = BSON::Registry.get(type).from_bson(bson)
    end
    hash
  end
end

class String
  @tally = 0
  class << self; attr_accessor :tally; end
  def self.from_bson(bson)
    s = bson.read(Int32.from_bson(bson)).from_bson_string.chop!
    @tally += s.size
    s
  end
end

abort "usage: #{$0} FILE.bson ...\n" if ARGV.size < 1

puts "["
ARGV.each_with_index do |filename, i|
  puts "," if i > 0
  STDOUT.flush
  File.open(filename) do |file|
    BSON::Registry.hist.fill(0)
    Hash.tally = 0
    String.tally = 0
    doc_count = 0
    tms = Benchmark.measure do
      while !file.eof
        Hash.from_bson(file)
        doc_count += 1
      end
    end
    doc_count = 1 if doc_count < 1

    hist = BSON::Registry.hist
    element_count = hist.inject{|sum,x| sum + x}
    hash_count = hist[BSON::Hash::BSON_TYPE.ord]
    array_count = hist[BSON::Array::BSON_TYPE.ord]
    aggregate_count = hash_count + array_count
    type_counts = BSON::Registry::MAPPINGS.collect{|byte, klass| [klass, hist[byte.ord]]}
    type_counts = type_counts.select{|elem| elem[1] > 0}
    type_counts = type_counts.sort{|a,b| b[1] <=> a[1]}

    print JSON.pretty_generate({
      file: filename,
      seconds: tms.real.round,
      docs_per_sec: (doc_count.to_f/tms.real).round,
      docs: doc_count,
      elements: element_count,
      elements_per_doc: (element_count.to_f / doc_count.to_f).round,
      aggregates: aggregate_count,
      aggregates_per_doc: (aggregate_count.to_f / doc_count.to_f).round,
      degree: (element_count.to_f / (doc_count + aggregate_count).to_f).round,
      key_size_average: (Hash.tally.to_f/[element_count,1].max.to_f).round,
      string_size_average: (String.tally.to_f/[hist[BSON::String::BSON_TYPE.ord],1].max.to_f).round,
      percent_by_type: Hash[*type_counts.collect{|klass, count| [klass, (100.0*count.to_f/element_count.to_f).round]}.flatten(1)]
    }).gsub(/^/, '  ')
  end
end
puts "\n]"

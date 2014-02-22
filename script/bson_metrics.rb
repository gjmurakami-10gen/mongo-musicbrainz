#!/usr/bin/env ruby
# Copyright (C) 2009-2014 MongoDB Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

gem "bson", "~> 2.2.1"
require 'bson'
require 'benchmark'

# usage: bson_metrics.rb [file.bson]

module BSON
  module Registry
    @@hist = ::Array.new(256, 0)
    def get(byte)
      @@hist[byte.ord] += 1
      MAPPINGS.fetch(byte)
    end
  end
end

doc_count = 0
bm = Benchmark.measure do
  while !ARGF.eof
    Hash.from_bson(ARGF)
    doc_count += 1
  end
end
doc_count = 1 if doc_count < 1

hist = BSON::Registry.class_variable_get(:@@hist)
element_count = hist.inject{|sum,x| sum + x}
hash_count = hist[BSON::Hash::BSON_TYPE.ord]
array_count = hist[BSON::Array::BSON_TYPE.ord]
embed_count = hash_count + array_count
type_counts = BSON::Registry::MAPPINGS.collect{|byte, klass| [klass, hist[byte.ord]]}
type_counts = type_counts.select{|elem| elem[1] > 0}
type_counts = type_counts.sort{|a,b| b[1] <=> a[1]}

puts <<-EOT.gsub(/^\s+/, '')
  seconds: #{'%.1f' % bm.real}
  docs/sec: #{(doc_count.to_f/bm.real).round}
  docs: #{doc_count}
  elements: #{element_count}
  embeds: #{embed_count} (sub-docs+sub-arrays)
  elements/doc: #{'%.1f' % (element_count.to_f / doc_count.to_f)}
  denorm: #{'%.1f' % (embed_count.to_f / doc_count.to_f)} (embeds/doc)
  degree: #{'%.1f' % (element_count.to_f / (doc_count + embed_count).to_f)}
EOT
puts type_counts.collect{|klass, count| "#{klass}: #{(100.0*count.to_f/element_count.to_f).round}%"}.join("\n")

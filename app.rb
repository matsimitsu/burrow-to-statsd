$:.unshift File.dirname(__FILE__)

require 'lib/burrow'

Burrow::Collector.run!

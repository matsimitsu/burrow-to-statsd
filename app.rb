require 'em/pure_ruby'
require 'statsd-ruby'
require 'json'
require 'rest-client'

module Burrow
  class Collector
    attr_reader :config, :statsd, :base_url

    def initialize(config)
      @config   = config
      @statsd   = Statsd.new config[:statsd_host], config[:statsd_port]
      @base_url = "http://#{config[:host]}:#{config[:port]}/v2/kafka"
    end

    def collect
      get('')["clusters"].map do |cluster|
        get("/#{cluster}/consumer")['consumers'].map do |consumer|
          status = get("/#{cluster}/consumer/#{consumer}/lag")

          statsd.gauge("kafka_#{cluster}_consumer_#{consumer}_partitions", status['partition_count'])
          statsd.gauge("kafka_#{cluster}_consumer_#{consumer}_maxlag",     status['maxlag'] || 0)
          statsd.gauge("kafka_#{cluster}_consumer_#{consumer}_totallag",   status['totallag'])
        end
      end
    rescue => e
      $stderr.puts "Error collecting metrics: #{e.inspect}"
    end

    def get(path)
      res = RestClient.get("#{base_url}#{path}")
      JSON.parse(res)
    rescue => e
      $stderr.puts "Error fetching #{path}: #{e.inspect}"
    end

    def self.default_config
      {
        :host           => "0.0.0.0",
        :port           => 8000,
        :statsd_host    => "0.0.0.0",
        :statsd_port    => 8125
      }
    end

    def self.run!(opts = {})
      config = self.default_config.merge(opts)
      collector = new(config)
      EM.run do
        EventMachine::PeriodicTimer.new(60) do
          collector.collect
        end
      end
    end
  end
end

Burrow::Collector.run!

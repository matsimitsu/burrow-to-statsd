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
      get_clusters.map do |cluster|
        get_consumers(cluster).map do |consumer|
          status   = get_lag(cluster, consumer)
          key_base = "kafka_#{cluster}_consumer_#{consumer}"

          statsd.gauge("#{key_base}_partitions", status['partition_count'])
          statsd.gauge("#{key_base}_maxlag",     status['maxlag'] || 0)
          statsd.gauge("#{key_base}_totallag",   status['totallag'])
        end
      end
    rescue => e
      $stderr.puts "Error collecting metrics: #{e.inspect}"
    end

    def get_clusters
      get('')["clusters"]
    end

    def get_consumers(cluster)
      get("/#{cluster}/consumer")['consumers']
    end

    def get_lag(cluster, consumer)
      get("/#{cluster}/consumer/#{consumer}/lag")['status']
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
      config    = self.default_config.merge(opts)
      collector = new(config)
      $stdout.puts "Running Burrow checker with config: #{config.inspect}"

      EM.run do
        EM::PeriodicTimer.new(60) do
          collector.collect
        end
      end
    end
  end
end

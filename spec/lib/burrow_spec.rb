require 'spec_helper'

describe "Burrow::Collector" do
  let(:config)    { Burrow::Collector.default_config}
  let(:collector) { Burrow::Collector.new(config) }

  describe "#initialize" do
    it "should store the config" do
      collector = Burrow::Collector.new({:foo => :bar})

      expect( collector.config ).to eql({:foo => :bar})
    end

    it "should start a StatsD client" do
      statsd = double
      expect( Statsd ).to receive(:new)
        .with('0.0.0.0', 8125)
        .and_return(statsd)

      collector = Burrow::Collector.new(config)

      expect( collector.statsd ).to eql(statsd)
    end

    it "should set a base url" do
      collector = Burrow::Collector.new(config)

      expect( collector.base_url ).to eql("http://0.0.0.0:8000/v2/kafka")
    end
  end

  describe "#collect" do
    let(:statsd) { double(:gague => true) }
    let(:result) { {'partition_count' => 6, 'maxlag' => nil, 'totallag' => 0} }

    before do
      allow(collector).to receive(:get_clusters).and_return(['staging'])
      allow(collector).to receive(:get_consumers).and_return(['testconsumer'])
      allow(collector).to receive(:get_lag).and_return(result)
      allow(collector).to receive(:statsd).and_return(statsd)
    end

    context "when there's no lag" do
      it "should call statsD with the correct params" do
        expect( statsd ).to receive(:gauge)
          .with("kafka_staging_consumer_testconsumer_partitions", 6)

        expect( statsd ).to receive(:gauge)
          .with("kafka_staging_consumer_testconsumer_maxlag", 0)

        expect( statsd ).to receive(:gauge)
          .with("kafka_staging_consumer_testconsumer_totallag", 0)
      end
    end

    context "when there's lag" do
      let(:result) { {'partition_count' => 6, 'maxlag' => 5, 'totallag' => 9} }

      it "should call statsD with the correct params" do
        expect( statsd ).to receive(:gauge)
          .with("kafka_staging_consumer_testconsumer_partitions", 6)

        expect( statsd ).to receive(:gauge)
          .with("kafka_staging_consumer_testconsumer_maxlag", 5)

        expect( statsd ).to receive(:gauge)
          .with("kafka_staging_consumer_testconsumer_totallag", 9)
      end
    end

    context "with an error" do
      before do
        allow(collector).to receive(:get_clusters)
          .and_raise(StandardError.new('foo'))
      end

      it "should log the error" do
        expect( $stderr ).to receive(:puts).with(
          "Error collecting metrics: #<StandardError: foo>"
        )
      end
    end
    after { collector.collect }
  end

  describe "#get_clusters" do
    it "should return a list of clusters" do
      expect( collector ).to receive(:get)
        .with('')
        .and_return({'clusters' => ['staging']})

      expect( collector.get_clusters ).to eql(['staging'])
    end
  end

  describe "#get_consumers" do
    it "should return a list of consumer" do
      expect( collector ).to receive(:get)
        .with('/staging/consumer')
        .and_return({'consumers' => ['testconsumer']})

      expect( collector.get_consumers('staging') ).to eql(['testconsumer'])
    end
  end

  describe "#get_lag" do
    let(:result) { {'partition_count' => 1, 'maxlag' => nil, 'totallag' => 0} }
    it "should return the lag" do
      expect( collector ).to receive(:get)
        .with('/staging/consumer/testconsumer/lag')
        .and_return({'status' => result})

      expect( collector.get_lag('staging', 'testconsumer') ).to eql(result)
    end
  end

  describe "#get" do
    it "should call RestClient" do
      expect( RestClient ).to receive(:get)
        .with('http://0.0.0.0:8000/v2/kafka/path')
        .and_return("{}")
    end

    it "parse the JSON" do
      json = %Q({"foo":"bar"})
      allow( RestClient ).to receive(:get).and_return(json)

      expect( JSON ).to receive(:parse).with(json)
    end

    context "with an error" do
      before do
        allow(RestClient).to receive(:get).and_raise(StandardError.new('foo'))
      end

      it "should log the error" do
        expect( $stderr ).to receive(:puts).with(
          "Error fetching /path: #<StandardError: foo>"
        )
      end
    end

    after { collector.get('/path') }
  end

  describe ".default_config" do
    it "should return a config hash" do
      expect( Burrow::Collector.default_config ).to eql({
        :host           => "0.0.0.0",
        :port           => 8000,
        :statsd_host    => "0.0.0.0",
        :statsd_port    => 8125
      })
    end
  end

  describe ".run!" do
    before do
      # Prevent Em from running
      allow(EM).to receive(:run)
      allow(EM::PeriodicTimer).to receive(:new)

      # Silence stdout
      allow($stdout).to receive(:puts)

      allow(Burrow::Collector).to receive(:new).and_return(collector)
    end

    it "should merge config with given options" do
      expect( Burrow::Collector ).to receive(:new)
        .with({
          :host           => "127.0.0.1",
          :port           => 8000,
          :statsd_host    => "0.0.0.0",
          :statsd_port    => 8125
        })
        .and_return(collector)
    end

    it "should log a start line" do
      expect( $stdout ).to receive(:puts)
    end

    after { Burrow::Collector.run!(:host => '127.0.0.1') }
  end
end

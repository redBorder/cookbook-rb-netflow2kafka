module Netflow2kafka
  module Helper
    require 'ipaddr'

    DOMAIN_FIELDS = %w(sensor_uuid deployment deployment_uuid namespace namespace_uuid market market_uuid
                       organization organization_uuid service_provider service_provider_uuid campus
                       campus_uuid building building_uuid floor floor_uuid).freeze

    # Builds the x2kafka config that replaces the legacy f2k sensors_networks config.
    # x2kafka enriches each flow with the whole enrichment_map entry matching the
    # exporter IP, so every sensor IP gets one flat entry. home_nets and span_port
    # are per listener in x2kafka, so they are merged across all the sensors.
    # Ports, brokers, topic and template TTL come from the node['netflow2kafka'] attributes.
    # memory (KB) sizes the in-memory queue of x2kafka.
    def x2kafka_config(sensors, memory = nil)
      enrichment_map = {}
      observations = {}
      home_nets = []
      span_ports = []

      sensors.keys.sort.each do |sensor|
        begin
          node_info = Chef::Node.load(sensor)
        rescue => e
          Chef::Log.warn("netflow2kafka: skipping sensor #{sensor}: #{e.message}")
          next
        end

        ip = node_info['ipaddress'].to_s
        next if ip.empty?

        rb = node_info['redborder'] || {}

        # only one entry per exporter IP: the default observation wins over the rest
        observation_id = rb['observation_id'].to_s.empty? ? 'default' : rb['observation_id'].to_s
        if observations.key?(ip) && (observations[ip] == 'default' || observation_id != 'default')
          Chef::Log.warn("netflow2kafka: exporter #{ip} already mapped, ignoring observation #{observation_id} of #{sensor}")
          next
        end
        observations[ip] = observation_id

        enrichment = {
          'sensor_name' => node_info['rbname'] || node_info.name,
          'sensor_ip' => ip,
          'index_partitions' => [1, (rb['index_partitions'] || 5).to_i].max,
          'index_replicas' => [1, (rb['index_replicas'] || 1).to_i].max,
        }
        DOMAIN_FIELDS.each do |field|
          enrichment[field] = rb[field] unless rb[field].to_s.empty?
        end
        enrichment_map[ip] = enrichment

        span_ports << (rb['spanport'].to_i == 1)

        Array(rb['homenets']).each do |homenet|
          network = homenet['value'].to_s.strip
          begin
            IPAddr.new(network)
            home_nets << network
          rescue IPAddr::Error
            # a single invalid CIDR makes x2kafka drop all the home_nets
            Chef::Log.warn("netflow2kafka: ignoring invalid homenet '#{network}' of #{sensor}")
          end
        end
      end

      listener = {
        'name' => 'netflow2kafka',
        'proto' => 'netflow',
        'bind' => '0.0.0.0',
        'port' => node['netflow2kafka']['port'],
        'topic' => node['netflow2kafka']['kafka_topic'],
        'span_port' => !span_ports.empty? && span_ports.all?,
        'enrichment_key' => 'sensor_ip',
        'enrichment_map' => enrichment_map,
        'template_ttl_seconds' => node['netflow2kafka']['template_ttl'],
      }
      # without home_nets x2kafka falls back to the private ranges
      listener['home_nets'] = home_nets.uniq unless home_nets.empty?

      config = {
        'brokers' => node['netflow2kafka']['kafka_brokers'].to_a,
        'topic' => node['netflow2kafka']['kafka_topic'],
        'client_id' => 'netflow2kafka',
        'async' => true,
        'stats_socket' => '/run/redborder-netflow2kafka/redborder-netflow2kafka.sock',
        # keepalived health checks /health on this port from the other managers
        'stats_bind' => '0.0.0.0',
        'stats_port' => node['netflow2kafka']['stats_port'],
        'hosts_path' => '/etc/objects',
        'applications_file' => '/etc/objects/applications',
        'listeners' => [listener],
      }
      # same sizing as the kafka queue of http2k; without memory (nil or 0) x2kafka keeps its default (50000)
      config['channel_buffer_size'] = [memory / 10, 1000].max if memory.to_i.positive?
      config
    end

    def get_known_ips
      known_ips = {}
      # Only the needed attributes of each node are requested to the chef server
      query_filter = { 'name' => ['name'], 'rbname' => ['rbname'], 'ipaddress' => ['ipaddress'] }
      search(:node, '*:*', filter_result: query_filter).sort_by { |n| n['name'].to_s }.each do |n|
        next unless n['ipaddress'] && (n['name'] || n['rbname'])

        node_name = n['rbname'] || n['name']
        known_ips[node_name] = n['ipaddress']
      end
      known_ips
    end

    def get_objects(object_type)
      objects = {}
      if node['redborder'] &&
         node['redborder']['objects'] &&
         node['redborder']['objects'][object_type] &&
         node['redborder']['objects'][object_type].class != Chef::Node::ImmutableArray
        objects = (object_type == 'hosts') ? get_known_ips.merge(node['redborder']['objects'][object_type]) : node['redborder']['objects'][object_type]
      elsif object_type == 'hosts'
        objects = get_known_ips
      end
      objects
    end
  end
end

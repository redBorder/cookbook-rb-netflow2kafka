# Flags
default['netflow2kafka']['registered'] = false

# x2kafka netflow listener
default['netflow2kafka']['port'] = 2055
default['netflow2kafka']['kafka_brokers'] = ['kafka.service:9092']
default['netflow2kafka']['kafka_topic'] = 'rb_flow'
# keepalived checks /health on this port: keep it in sync with the health_port of redborder-netflow2kafka
# in cookbook-rb-manager and with the ports opened by cookbook-rb-firewall
default['netflow2kafka']['stats_port'] = 7983
# x2kafka only refreshes a template when the exporter resends it, so keep them longer
# than the exporters' template refresh (cisco resends every 30 minutes)
default['netflow2kafka']['template_ttl'] = 3600

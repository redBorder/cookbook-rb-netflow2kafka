# Cookbook:: rb-netflow2kafka
# Resource:: config

actions :add, :remove, :register, :deregister
default_action :add

attribute :config_dir, kind_of: String, default: '/etc/redborder-netflow2kafka'
attribute :sensors, kind_of: Hash, default: {}
# memory assigned to the service in KB (node['redborder']['memory_services'])
attribute :memory, kind_of: Integer

# Cookbook:: rb-netflow2kafka
# Provider:: config

include Netflow2kafka::Helper

action :add do
  begin
    config_dir = new_resource.config_dir
    sensors = new_resource.sensors
    memory = new_resource.memory

    # redborder-netflow2kafka ships the redborder-netflow2kafka unit running x2kafka
    dnf_package 'redborder-netflow2kafka' do
      action :upgrade
    end

    # restart redborder-netflow2kafka when a new x2kafka is installed
    dnf_package 'x2kafka' do
      action :upgrade
      notifies :restart, 'service[redborder-netflow2kafka]', :delayed unless node['redborder']['leader_configuring']
    end

    # Directory creation
    directory config_dir do
      owner 'root'
      group 'root'
      mode '0755'
    end

    directory '/etc/objects' do
      owner 'root'
      group 'root'
      mode '0777'
      action :create
    end

    service 'redborder-netflow2kafka' do
      supports status: true, start: true, restart: true, reload: true, stop: true
      if node['redborder']['leader_configuring']
        action [:enable, :stop]
      else
        action [:enable, :start]
      end
    end

    # Templates
    template "#{config_dir}/config.json" do
      source 'netflow2kafka_config.erb'
      cookbook 'rb-netflow2kafka'
      owner 'root'
      group 'root'
      mode '0644'
      retries 2
      variables(config: x2kafka_config(sensors, memory))
      notifies :reload, 'service[redborder-netflow2kafka]', :delayed unless node['redborder']['leader_configuring']
    end

    # Objects templates
    # x2kafka only reads /etc/objects/applications, so the rest of the objects must not
    # reload redborder-netflow2kafka: every reload drops the netflow templates received so far
    %w(icmps interfaces hosts).each do |ob|
      objects = get_objects(ob)

      template "/etc/objects/#{ob}" do
        source 'objects_hosts.erb'
        owner 'root'
        group 'root'
        mode '0644'
        retries 2
        cookbook 'rb-netflow2kafka'
        variables(objects: objects)
      end
    end

    %w(macs protocols services networks vlans).each do |ob|
      begin
        ob_db = data_bag_item('rBglobal', ob)
      rescue
        ob_db = nil
      end

      objects = {}

      if ob_db && ob_db['list'] && !ob_db['list'].empty?
        objects = ob_db['list']
      end

      template "/etc/objects/#{ob}" do
        source 'objects.erb'
        owner 'root'
        group 'root'
        mode '0644'
        retries 2
        cookbook 'rb-netflow2kafka'
        variables(objects: objects)
      end
    end

    %w(http_domains http_host_l1_alias os engines).each do |ob|
      template "/etc/objects/#{ob}" do
        source "objects_#{ob}.erb"
        owner 'root'
        group 'root'
        mode '0644'
        retries 2
        cookbook 'rb-netflow2kafka'
      end
    end

    begin
      app_db = data_bag_item('rBglobal', 'applications')
    rescue
      app_db = nil
    end

    if app_db.nil? || app_db['list'].nil? || app_db['list'].empty?
      template '/etc/objects/applications' do
        source 'objects_default_applications.erb'
        owner 'root'
        group 'root'
        mode '0644'
        retries 2
        cookbook 'rb-netflow2kafka'
        notifies :reload, 'service[redborder-netflow2kafka]', :delayed unless node['redborder']['leader_configuring']
      end
    else
      template '/etc/objects/applications' do
        source 'objects_applications.erb'
        owner 'root'
        group 'root'
        mode '0644'
        retries 2
        cookbook 'rb-netflow2kafka'
        variables(applications: app_db['list'])
        notifies :reload, 'service[redborder-netflow2kafka]', :delayed unless node['redborder']['leader_configuring']
      end
    end
    # End objects templates

    Chef::Log.info('rb-netflow2kafka cookbook has been processed')
  rescue => e
    Chef::Log.error(e.message)
  end
end

action :remove do
  begin
    service 'redborder-netflow2kafka' do
      supports stop: true, disable: true
      action [:stop, :disable]
    end

    Chef::Log.info('rb-netflow2kafka cookbook has been processed')
  rescue => e
    Chef::Log.error(e.message)
  end
end

action :register do
  begin
    unless node['netflow2kafka']['registered']
      query = {}
      query['ID'] = "netflow2kafka-#{node['hostname']}"
      query['Name'] = 'netflow2kafka'
      query['Address'] = "#{node['ipaddress']}"
      query['Port'] = node['netflow2kafka']['port']
      json_query = Chef::JSONCompat.to_json(query)

      execute 'Register service in consul' do
        command "curl -X PUT http://localhost:8500/v1/agent/service/register -d '#{json_query}' &>/dev/null"
        action :nothing
      end.run_action(:run)

      node.normal['netflow2kafka']['registered'] = true
    end
    Chef::Log.info('netflow2kafka service has been registered in consul')
  rescue => e
    Chef::Log.error(e.message)
  end
end

action :deregister do
  begin
    if node['netflow2kafka']['registered']
      execute 'Deregister service in consul' do
        command "curl -X PUT http://localhost:8500/v1/agent/service/deregister/netflow2kafka-#{node['hostname']} &>/dev/null"
        action :nothing
      end.run_action(:run)

      node.normal['netflow2kafka']['registered'] = false
    end
    Chef::Log.info('netflow2kafka service has been deregistered from consul')
  rescue => e
    Chef::Log.error(e.message)
  end
end

#!/usr/bin/ruby
#
# Create a platform with 3 networks (asia, europe, us) connected together
# via a 'global' network.
# Inspired from the tutorial's example
# To use with: platform_setup_globalinternet.rb <SUBNET>
# e.g.: platform_setup_globalinternet.rb 10.144.0.0/22

require 'distem'
require 'ipaddress'

$cl = Distem::NetAPI::Client.new

# The path to the compressed filesystem image
# We can point to local file since our homedir is available from NFS
FSIMG="file:///home/dmalikireddy/public/node-1-fsimage.tar.gz"
# Getting the physical nodes list which is set in the
# environment variable 'DISTEM_NODES' by distem-bootstrap
#pnodes=ENV['DISTEM_NODES'].split("\n")

=begin
def create_vnodes
  # Read SSH keys
  private_key = IO.readlines('/root/.ssh/id_rsa').join
  public_key = IO.readlines('/root/.ssh/id_rsa.pub').join
  sshkeys = {
    'private' => private_key,
    'public' => public_key
  }
  pnodes=ENV['DISTEM_NODES'].split("\n")
  #FSIMG="file:///home/ejeanvoine/public/distem/distem-fs-wheezy.tar.gz"
  $cl.vnode_create('r-us', { 'host' => pnodes[0] }, sshkeys)
  $cl.vfilesystem_create('r-us', { 'image' => FSIMG })
  $cl.vnode_create('r-europe', { 'host' => pnodes[1] }, sshkeys)
  $cl.vfilesystem_create('r-europe', { 'image' => FSIMG })
  $cl.vnode_create('r-asia', { 'host' => pnodes[2] }, sshkeys)
  $cl.vfilesystem_create('r-asia', { 'image' => FSIMG })
  $cl.vnode_create('us1', { 'host' => pnodes[0] }, sshkeys)
  $cl.vfilesystem_create('us1', { 'image' => FSIMG })
  $cl.vnode_create('us2', { 'host' => pnodes[0] }, sshkeys)
  $cl.vfilesystem_create('us2', { 'image' => FSIMG })
  $cl.vnode_create('europe1', { 'host' => pnodes[1] }, sshkeys)
  $cl.vfilesystem_create('europe1', { 'image' => FSIMG })
  $cl.vnode_create('europe2', { 'host' => pnodes[1] }, sshkeys)
  $cl.vfilesystem_create('europe2', { 'image' => FSIMG })
  $cl.vnode_create('asia1', { 'host' => pnodes[2] }, sshkeys)
  $cl.vfilesystem_create('asia1', { 'image' => FSIMG })
  $cl.vnode_create('asia2', { 'host' => pnodes[2] }, sshkeys)
  $cl.vfilesystem_create('asia2', { 'image' => FSIMG })
end
def create_vifaces
  # routers on global network
  $cl.viface_create('r-us', 'if0', { 'vnetwork' => 'global', "output"=>{ "latency"=> {"delay" => "20ms"} }, "input"=>{ "latency"=> { "delay" => "20ms" } }})
  $cl.viface_create('r-europe', 'if0', { 'vnetwork' => 'global', "output"=>{ "latency"=> {"delay" => "30ms" } }, "input"=>{ "latency"=>{ "delay" =>  "30ms" } } })
  $cl.viface_create('r-asia', 'if0', { 'vnetwork' => 'global', "output"=>{ "latency"=> { "delay" => "40ms" } }, "input"=>{ "latency"=> { "delay" => "40ms" } } })

  # routers on their respective local networks
  $cl.viface_create('r-us', 'if1', { 'vnetwork' => 'us' })
  $cl.viface_create('r-europe', 'if1', { 'vnetwork' => 'europe' })
  $cl.viface_create('r-asia', 'if1', { 'vnetwork' => 'asia' })

  # nodes on local networks
  $cl.viface_create('us1', 'if1', { 'vnetwork' => 'us' })
  $cl.viface_create('us2', 'if1', { 'vnetwork' => 'us' })
  $cl.viface_create('europe1', 'if1', { 'vnetwork' => 'europe' })
  $cl.viface_create('europe2', 'if1', { 'vnetwork' => 'europe' })
  $cl.viface_create('asia1', 'if1', { 'vnetwork' => 'asia' })
  $cl.viface_create('asia2', 'if1', { 'vnetwork' => 'asia' })
end
=end

def create_subnets
  res_subnet = IPAddress(ARGV[-1])
  $no = (ARGV.length - 1)/4
  subnets = res_subnet.split($no+1)
  $cl.vnetwork_create('global', subnets[0].to_string)
  $no.times do |index|
  	$cl.vnetwork_create(ARGV[4*index], subnets[index+1].to_string)
  end
  pp $cl.vnetworks_info
end

def create_vnodes
  # Read SSH keys
  private_key = IO.readlines('/root/.ssh/id_rsa').join
  public_key = IO.readlines('/root/.ssh/id_rsa.pub').join
  sshkeys = {
    'private' => private_key,
    'public' => public_key
  }
  pnodes=ENV['DISTEM_NODES'].split("\n")
 
  #FSIMG="file:///home/ejeanvoine/public/distem/distem-fs-wheezy.tar.gz"
  
  if pnodes.length < $nodelist.length
  	$flag = -1
  	$nodelist.length.times do |index|
		if $nodelist[index].start_with?('r_') == true
			$flag += 1
		end
		$cl.vnode_create($nodelist[index], { 'host' => pnodes[$flag] }, sshkeys)
		$cl.vfilesystem_create($nodelist[index], { 'image' => FSIMG })
  	end
  else
        $nodelist.length.times do |index|
                $cl.vnode_create($nodelist[index], { 'host' => pnodes[index] }, sshkeys)
                $cl.vfilesystem_create($nodelist[index], { 'image' => FSIMG })
        end
  end
end

def create_vifaces
  # routers on global network
  $no = (ARGV.length - 1)/4
  $no.times do |index|
	$cl.viface_create("r_"+ARGV[4*index], 'if0', { 'vnetwork' => 'global', "output"=>{ "latency"=> {"delay"=> ARGV[4*index+1].to_s+"ms"} }, "input"=>{ "latency"=> {"delay"=> ARGV[4*index+1].to_s+"ms"} }})
  end

  # routers on their respective local networks
  $no.times do |index|
	$cl.viface_create("r_"+ARGV[4*index], 'if1', { 'vnetwork' => ARGV[4*index] })
  end

  # nodes on local networks
  $nodelist.length.times do |index|
	if $nodelist[index].start_with?('r_') == false
		$node = $nodelist[index].gsub(/[SC0-9_]/,'')
		$cl.viface_create($nodelist[index], 'if1', { 'vnetwork' => $node })
		puts $node
	end
  end
end

def create_vroutes
  puts 'Completing virtual routes'
  $cl.vroute_complete
  puts 'Virtual route creation completed'
end

#puts ARGV.length

$nodelist = Array.new
$i=0
while $i < ARGV.length - 1
	$nodelist.push("r_"+ARGV[$i])
	ARGV[$i+2].to_i.times do |index|
		$nodelist.push(ARGV[$i]+"_S"+index.to_s)
	end
	ARGV[$i+2].to_i.times do |index|
		$nodelist.push(ARGV[$i]+"_C"+index.to_s)
	end
	$i += 4
end

$nodelist.each do |element|
	puts element
end


puts 'Creating sub-networks'
create_subnets
puts 'Sub-network creation done'
puts 'Creating virtual nodes'
create_vnodes
puts 'Virtual nodes creation done'

puts 'Creating virtual interfaces'
create_vifaces
puts 'Virtual interfaces creation done'

create_vroutes

puts 'Starting virtual nodes'
# start all vnodes
$cl.vnodes_start($cl.vnodes_info.map { |vn| vn['name'] })

puts "Setting global /etc/hosts"
$cl.set_global_etchosts()
puts "Setting global ARP tables"
$cl.set_global_arptable()
puts 'Network setup complete'

pp $cl.vnodes_info

exit(0)

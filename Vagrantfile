Vagrant.configure(2) do |config|
  config.vm.box = "debian/jessie64"
  config.vm.provision :shell, path: "vagrant_bootstrap.sh"
  config.ssh.forward_agent = true

  config.vm.synced_folder ENV['VAGRANT_DATA_HOME'], "/vagrant_data_home"

  config.vm.provider "virtualbox" do |vb|
    vb.gui = false
  
    vb.memory = "2048"
  end

  config.vm.network "forwarded_port", guest: 8888, host: 10000

  config.vm.provider "virtualbox" do |vb|
    vb.gui = false
    vb.memory = "2048"
  end

  config.vm.provider :virtualbox do |vb|
    vb.customize ["modifyvm", :id, "--natdnshostresolver1", "on"]
  end
  
end

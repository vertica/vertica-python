# -*- mode: ruby -*-
# vi: set ft=ruby :

VAGRANTFILE_API_VERSION = "2"

#######################################################################
# This will set up a box with Vertica Community Edition 7.1.1
# running inside the box in a Docker container.
#
# The purpose is to have a Vertica instance that can be used by tests.
#
# Vertica's port 5433 is exposed to host machine.
# Database 'docker' is available.
# User is 'dbadmin' with no password.
#
# >>>
# !   As is, any data stored inside Vertica will not live through
# !   container or VM restart.
# >>>
#######################################################################


# Globally install docker - bypass default Vagrant docker installation
# procedure because it does not work reliably (curl does not follow
# redirect = Docker won't be installed)

$install_docker = <<SCRIPT
curl -sSL https://get.docker.io | sh
usermod -aG docker vagrant
SCRIPT

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.box = "phusion/ubuntu-14.04-amd64"

  # Make Vertica available on host machine on port 5433
  config.vm.network "forwarded_port", guest: 5433, host: 5433

  config.vm.provider "virtualbox" do |v|
     v.memory = 2048
     v.cpus = 2
  end

  config.vm.provision "shell", inline: $install_docker

  # Pull Vertica image when the box is first provisioned
  config.vm.provision "docker" do |d|
    d.pull_images 'sumitchawla/vertica:latest'
  end

  # Start Vertica inside container every time box is started
  config.vm.provision "docker", run: "always" do |d|
    d.run 'sumitchawla/vertica',
    cmd: "",
    args: "-d -p 5433:5433 --name vertica"
  end

end

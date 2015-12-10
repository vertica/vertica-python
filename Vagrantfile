# -*- mode: ruby -*-
# vi: set ft=ruby :

ENV["VAGRANT_DEFAULT_PROVIDER"] ||= "docker"

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

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|

  config.vm.provider "docker" do |d|
     d.image = "sumitchawla/vertica:latest"
     d.ports = ["5433:5433"]
  end

  config.vm.synced_folder ".", "/vagrant", disabled: true

end

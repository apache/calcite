## Creating a development virtual machine

This is a set of scripts to create development virtual machine and populate it
with test data.

The following databases are created: MongoDB, MySQL, PostgreSQL.

Requirements: Vagrant, Virtual Box.

To create a VM, use the following command: `vagrant up && cd mondrian-loader && mvn verify`
Fully-populated VM requires ~5GiB.

For more information see Creating a development virtual machine in HOWTO

awslogs-sd
==========

.. image:: https://travis-ci.org/mbachry/awslogs-sd.svg?branch=master
    :alt: Build status
    :target: https://travis-ci.org/mbachry/awslogs-sd

Forward systemd journal logs to CloudWatch.

A log forwarder daemon similar to Amazon's awslogs agent, but using
per systemd unit journal output instead of text log files.

Installing
----------

Use your system package manager to install Python 3 and pip. The
required package is named ``python3-pip`` both in Ubuntu and Fedora::

    dnf install python3-pip

Create dedicated virtualenv for awslogs-sd::

    python3 -m venv /opt/awslogs-sd
    /opt/awslogs-sd/bin/pip install wheel

And finally::

    /opt/awslogs-sd/bin/pip install awslogs-sd

Daemon binary will be available under
``/opt/awslogs-sd/bin/awslogs-sd``.

AWS setup
---------

Similarly to Amazon's awslogs, ``awslogs-sd`` requires an IAM policy
attached to EC2 instance. Configuration is the same as in `Amazon
awslogs documentation`_.

.. _Amazon awslogs documentation: https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/QuickStartEC2Instance.html

Usage
-----

``awslogs-sd`` requires path to configuration file as a positional
argument.

You can use ``--logging-conf`` to pass optional YAML logging
configuration in ``dictConfig`` format. See `Python logging
documentation`_. Following loggers can be configured:

* ``awslogs``: main daemon logger

* ``metrics``: daemon statistics printed at ``INFO`` level every 10
  seconds

.. _Python logging documentation: https://docs.python.org/2/library/logging.config.html#configuration-dictionary-schema

Configuration
-------------

Configuration file uses ini format and is designed to be similar to
Amazon `awslogs configuration`_.

There are two main ini sections ``general`` and ``include``. Every
other section has arbitrary name and contains a single systemd unit
configuration.

.. _awslogs configuration: https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/AgentReference.html

``general``
~~~~~~~~~~~

Following option is required:

* ``state_file``: path to daemon database. Must be in persistent storage.

Example::

    [general]
    state_file = /var/lib/awslogs-sd/state

``include``
~~~~~~~~~~~

Optional section with a single option:

* ``path``: an Unix glob pattern to specify locations of additional
  configuration files to load and merge with main one. Used mostly to
  support popular ``conf.d`` drop-in pattern.

Example::

    [include]
    path = /etc/awslogs-sd.conf.d/*.conf

Unit sections
~~~~~~~~~~~~~

Example::

    [httpd_error]
    unit = httpd.service
    priority = ERR
    syslog_ident = httpd_error
    syslog_facility = local1
    log_group_name = httpd-access
    log_stream_name = {instance_id}

Following options are supported:

* ``unit`` (required): systemd unit name

* ``log_group_name`` (required): destination CloudWatch log group

* ``log_stream_name`` (required): CloudWatch stream name inside group;
  support basic variable interpolation (see below)

* ``priority``: minimum journal priority to match (default:
  ``INFO``). See `Arch wiki`_.

* ``format``: output log format, one of ``text`` or ``json`` (default:
  ``text``)

* ``datetime_format``: datetime format in strftime format if text
  output is used (default: ``%b %d %H:%M:%S``)

* ``syslog_ident``: match by syslog ident (aka "tag") if syslog
  transport is used

* ``syslog_facility``: match by syslog facility if syslog transport is
  used

.. _Arch wiki: https://wiki.archlinux.org/index.php/systemd#Journal

Stream name variables
~~~~~~~~~~~~~~~~~~~~~

Stream names support variable interpolation with ``{var}``
syntax. Following variables are available:

* ``instance_id``: local EC2 instance id

* ``hostname``: machine hostname

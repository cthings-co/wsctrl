# Wirepas Sink Controller

Wirepas Sink Controller is a wrapper for [BusClient](https://github.com/wirepas/gateway/blob/master/python_transport/wirepas_gateway/dbus/dbus_client.py) provided by Wirepas for simplified and universal way of communication to connected DualMCU application (Serial Radio), enabling easier integrations of [Sink Service](https://github.com/wirepas/gateway/tree/master/sink_service) to any other Python-based software.

## Usage

This repository provides following interfaces:

* `SinkController` - used for data transmission and managing connected Sink over Dbus.
* `WirepasResponse` - data class for received data from Wirepas network.
* `Nbor` - Information about Neighbors of a Sink (currently unusable due to blocked PR: https://github.com/wirepas/gateway/pull/296)

The reference example how to use above interfaces can be found in WMB-Controller repository: https://github.com/cthings-co/wmb-controller

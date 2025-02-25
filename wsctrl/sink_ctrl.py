# Copyright (c) 2025 CTHINGS.CO
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import asyncio
import logging
import multiprocessing as mp
import threading
from dataclasses import dataclass, field

from wsctrl.exceptions import SinkCtrlNoComms
from wirepas_gateway.dbus.dbus_client import BusClient

logger = logging.getLogger(__name__)

poll_cb_sem = threading.Semaphore(value=0)
response_queue = mp.Queue()


class Nbor():
    def __init__(self, nbor_tuple, **kwargs):
        self._address = nbor_tuple[0]
        self._link_rel = nbor_tuple[1]
        self._norm_rssi = nbor_tuple[2]
        self._cost = nbor_tuple[3]
        self._channel = nbor_tuple[4]
        self._nbor_type = nbor_tuple[5]
        self._tx_power = nbor_tuple[6]
        self._rx_power = nbor_tuple[7]
        self._last_update = nbor_tuple[8]

    @property
    def address(self):
        return self._address

    @property
    def link_rel(self):
        return self._link_rel

    @property
    def norm_rssi(self):
        return self._norm_rssi

    @property
    def cost(self):
        return self._cost

    @property
    def channel(self):
        return self._channel

    @property
    def nbor_type(self):
        return self._nbor_type

    @property
    def tx_power(self):
        return self._tx_power

    @property
    def rx_power(self):
        return self._rx_power

    @property
    def last_update(self):
        return self._last_update


    def dump_info(self):
        logging.info(f"Address: {self.address} | Link_rel: {self.link_rel} | RSSI: {self.norm_rssi} | "
                     f"Cost: {self.cost} | Channel: {self.channel} | Type: {self.nbor_type} | "
                     f"TX_power: {self.tx_power} | RX_power: {self.rx_power} | Last_update: {self.last_update}")


@dataclass
class WirepasResponse:
    dst: int
    src: int
    src_ep: int
    dst_ep: int
    travel_time: int
    qos: int
    hop_count: int
    payload: bytes


class SinkController(BusClient):
    MAX_HOP_LIMIT = 15
    DEFAULT_QOS = 0
    DEFAULT_DELAY_MS = 0

    def __init__(self, dst_addr, ep_src=1, ep_dst=1, qos=DEFAULT_QOS, initial_delay_ms=DEFAULT_DELAY_MS,
                 is_unack_csma_ca=False, hop_limit=MAX_HOP_LIMIT, pm=False, sink_ids=["sink0"],
                 **kwargs):
        super().__init__(c_extension=True, **kwargs)
        self._dst_addr = dst_addr
        self._qos = qos
        self._initial_delay_ms = initial_delay_ms
        self._is_unack_csma_ca = is_unack_csma_ca
        self._hop_limit = hop_limit
        self._src_ep = ep_src
        self._dst_ep = ep_dst
        self._nbors = []
        self._pm = pm   # Promiscous Mode
        self._sink_id_set = set(sink_ids)

        self.c_extension_thread.start()
        self._recv_event = asyncio.Event()
        self._loop = asyncio.get_running_loop()
        self._poll_thread = threading.Thread(target=self._poll_data_ready, args=(self._loop, self._recv_event,))
        self._poll_thread.daemon = True
        self._poll_thread.start()

    def _poll_data_ready(self, loop, event):
        while True:
            poll_cb_sem.acquire()
            loop.call_soon_threadsafe(event.set)

    def _get_sink(self, sink_id):
        """Helper function to retrieve a sink by ID efficiently."""
        for sink in self.sink_manager.get_sinks():
            if sink.sink_id == sink_id:
                return sink
        return None

    def _stop_sinks(self):
        for sink_id in self._sink_id_set:
            sink = self._get_sink(sink_id)
            if sink:
                sink.write_config({"started": False})
                logger.debug(f"Sink {sink} stopped")

    def _start_sinks(self):
        for sink_id in self._sink_id_set:
            sink = self._get_sink(sink_id)
            if sink:
                sink.write_config({"started": True})
                logger.debug(f"Sink {sink} started")

    def _send(self, payload_coded):
        for sink_id in self._sink_id_set:
            sink = self._get_sink(sink_id)
            if sink:
                sink.send_data(
                    self._dst_addr, self._src_ep, self._dst_ep, self._qos,
                    self._initial_delay_ms, payload_coded, self._is_unack_csma_ca,
                    self._hop_limit
                )
                logger.debug(f"Data sent over {sink}: {payload_coded}")

    def initialize_sink(self):
        try:
            self._start_sinks()
        except Exception as e:
            raise SinkCtrlNoComms(f"Bus error: {e}") from e

    def deinitialize_sink(self):
        try:
            self._stop_sinks()
        except Exception as e:
            raise SinkCtrlNoComms(f"Bus error: {e}") from e

    def send(self, payload_coded):
        try:
            self._send(payload_coded)
        except Exception as e:
            raise SinkCtrlNoComms(f"Bus error: {e}") from e

    def on_data_received(
        self,
        sink_id,
        timestamp,
        src,
        dst,
        src_ep,
        dst_ep,
        travel_time,
        qos,
        hop_count,
        data,
    ):
        response = WirepasResponse(dst, src, src_ep, dst_ep, travel_time, qos, hop_count, data)
        # Source and Destination EPs filtering if applied
        if (not self._pm and (src_ep != self.dst_ep or dst_ep != self.src_ep)):
            return
        response_queue.put(response)
        poll_cb_sem.release()

    def receive(self):
        return response_queue.get()

    async def async_receive(self):
        await self._recv_event.wait()
        self._recv_event.clear()
        return response_queue.get()

    @property
    def nbors(self) -> list:
        return self._nbors

    @property
    def src_ep(self):
        return self._src_ep

    @src_ep.setter
    def src_ep(self, value):
        self._src_ep = value

    @property
    def dst_ep(self):
        return self._dst_ep

    @dst_ep.setter
    def dst_ep(self, value):
        self._dst_ep = value

    @property
    def mtu(self) -> int:
        for sink_id in self._sink_id_set:
            sink = self._get_sink(sink_id)
            if sink:
                config, _ = sink.read_config()
                return config["max_mtu"]
        return None

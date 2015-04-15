import os
import time
import logging
import tornado
import optparse
import binascii
import cStringIO
import functools
import collections
import tornado.gen
import ConfigParser
import tornado.ioloop


import uavcan
import uavcan.node


class NodeStatusHandler(uavcan.node.MessageHandler):
    NODE_INFO = {}
    NODE_STATUS = {}
    NODE_TIMESTAMP = collections.defaultdict(float)
    TIMEOUT = 30.0

    def __init__(self, *args, **kwargs):
        super(NodeStatusHandler, self).__init__(*args, **kwargs)
        self.new_node_callback = kwargs.get("new_node_callback", None)

    @tornado.gen.coroutine
    def on_message(self, message):
        node_id = self.transfer.source_node_id
        last_timestamp = NodeStatusHandler.NODE_TIMESTAMP[node_id]
        last_node_uptime = NodeStatusHandler.NODE_STATUS[node_id].uptime_sec \
                           if node_id in NodeStatusHandler.NODE_STATUS else 0

        # Update the node status registry
        NodeStatusHandler.NODE_STATUS[node_id] = message
        NodeStatusHandler.NODE_TIMESTAMP[node_id] = time.time()

        if time.time() - last_timestamp > NodeStatusHandler.TIMEOUT or \
                message.uptime_sec < last_node_uptime:
            # The node has timed out, hasn't been seen before, or has
            # restarted, so get the node's hardware and software info
            request = uavcan.protocol.GetNodeInfo(mode="request")
            response, response_transfer = \
                yield self.node.send_request(request, node_id)
            NodeStatusHandler.NODE_INFO[node_id] = response

            hw_unique_id = "".join(format(c, "02X") for c in
                                   response.hardware_version.unique_id)
            msg = (
                "[#{0:03d}:uavcan.protocol.GetNodeInfo] " +
                "software_version.major={1:d} " +
                "software_version.minor={2:d} " +
                "software_version.vcs_commit={3:08x} " +
                "software_version.image_crc={4:016X} " +
                "hardware_version.major={5:d} " +
                "hardware_version.minor={6:d} " +
                "hardware_version.unique_id={7!s} " +
                "name={8!r}"
            ).format(
                self.transfer.source_node_id,
                response.software_version.major,
                response.software_version.minor,
                response.software_version.vcs_commit,
                response.software_version.image_crc,
                response.hardware_version.major,
                response.hardware_version.minor,
                hw_unique_id,
                response.name.decode()
            )
            logging.info(msg)

            # If a new-node callback is defined, call it now
            if self.new_node_callback:
                self.new_node_callback(node_id, response)

        raise tornado.gen.Return()


class DynamicNodeIDAllocationHandler(uavcan.node.MessageHandler):
    ALLOCATION = {}
    ALLOCATION_QUERY = ""

    def __init__(self, *args, **kwargs):
        super(DynamicNodeIDAllocationHandler, self).__init__(*args, **kwargs)
        self.dynamic_id_range = kwargs.get("dynamic_id_range", (1, 127))

    def on_message(self, message):
        if message.first_part_of_unique_id:
            # First-phase messages trigger a second-phase query
            DynamicNodeIDAllocationHandler.ALLOCATION_QUERY = \
                message.unique_id.decode()

            response = uavcan.protocol.dynamic_node_id.Allocation()
            response.first_part_of_unique_id = 0
            response.node_id = 0
            response.unique_id.encode(NODE_ALLOCATION_QUERY)
            self.node.send_broadcast(response)

            logging.debug(("[MASTER] Got first-stage dynamic ID request " +
                           "for {0!r}").format(
                           DynamicNodeIDAllocationHandler.ALLOCATION_QUERY))
        elif len(message.unique_id) == 7 and \
                len(DynamicNodeIDAllocationHandler.ALLOCATION_QUERY) == 7:
            # Second-phase messages trigger a third-phase query
            DynamicNodeIDAllocationHandler.ALLOCATION_QUERY = \
                DynamicNodeIDAllocationHandler.ALLOCATION_QUERY + \
                message.unique_id.decode()

            response = uavcan.protocol.dynamic_node_id.Allocation()
            response.first_part_of_unique_id = 0
            response.node_id = 0
            response.unique_id.encode(
                DynamicNodeIDAllocationHandler.ALLOCATION_QUERY)
            self.node.send_broadcast(response)
            logging.debug(("[MASTER] Got second-stage dynamic ID request " +
                           "for {0!r}").format(
                           DynamicNodeIDAllocationHandler.ALLOCATION_QUERY))
        elif len(message.unique_id) == 2 and \
                len(DynamicNodeIDAllocationHandler.ALLOCATION_QUERY) == 14:
            # Third-phase messages trigger an allocation
            DynamicNodeIDAllocationHandler.ALLOCATION_QUERY = \
                DynamicNodeIDAllocationHandler.ALLOCATION_QUERY + \
                message.unique_id.decode()

            logging.debug(("[MASTER] Got third-stage dynamic ID request " +
                           "for {0!r}").format(
                           DynamicNodeIDAllocationHandler.ALLOCATION_QUERY))

            node_requested_id = message.node_id
            node_allocated_id = None

            allocated_node_ids = \
                set(DynamicNodeIDAllocationHandler.ALLOCATION.itervalues()) | \
                set(NodeStatusHandler.NODE_STATUS.iterkeys())
            allocated_node_ids.add(self.node.node_id)

            # If we've already allocated a node ID to this device, return the
            # same one
            if DynamicNodeIDAllocationHandler.ALLOCATION_QUERY in \
                    DynamicNodeIDAllocationHandler.ALLOCATION:
                node_allocated_id = DynamicNodeIDAllocationHandler.ALLOCATION[
                    DynamicNodeIDAllocationHandler.ALLOCATION_QUERY]

            # If an ID was requested but not allocated yet, allocate the first
            # ID equal to or higher than the one that was requested
            if node_requested_id and not node_allocated_id:
                for node_id in xrange(node_requested_id,
                                      self.dynamic_id_range[1]):
                    if node_id not in allocated_node_ids:
                        node_allocated_id = node_id
                        break

            # If no ID was allocated in the above step (also if the requested
            # ID was zero), allocate the highest unallocated node ID
            if not node_allocated_id:
                for node_id in xrange(self.dynamic_id_range[1],
                                      self.dynamic_id_range[0], -1):
                    if node_id not in allocated_node_ids:
                        node_allocated_id = node_id
                        break

            DynamicNodeIDAllocationHandler.ALLOCATION[
                DynamicNodeIDAllocationHandler.ALLOCATION_QUERY] = \
                node_allocated_id

            if node_allocated_id:
                response = uavcan.protocol.dynamic_node_id.Allocation()
                response.first_part_of_unique_id = 0
                response.node_id = node_allocated_id
                response.unique_id.encode(
                    DynamicNodeIDAllocationHandler.ALLOCATION_QUERY)
                self.node.send_broadcast(response)
                logging.info(("[MASTER] Allocated node ID #{0:03d} to node " +
                              "with unique ID {1!r}").format(
                              node_allocated_id,
                              DynamicNodeIDAllocationHandler.ALLOCATION_QUERY)
                              )
            else:
                logging.error("[MASTER] Couldn't allocate dynamic node ID")
        else:
            # Received mis-sequenced reply, clear out the state
            DynamicNodeIDAllocationHandler.ALLOCATION_QUERY = ""
            logging.error("[MASTER] Got mis-sequenced reply, resetting query")


class FileGetInfoHandler(uavcan.node.ServiceHandler):
    def __init__(self, *args, **kwargs):
        super(FileGetInfoHandler, self).__init__(*args, **kwargs)
        self.base_path = kwargs.get("path")

    def on_request(self):
        logging.debug("[#{0:03d}:uavcan.protocol.file.GetInfo] {1!r}".format(
                      self.transfer.source_node_id,
                      self.request.path.path.decode()))
        try:
            vpath = self.request.path.path.decode()
            with open(os.path.join(self.base_path, vpath), "rb") as fw:
                data = fw.read()
                self.response.error.value = self.response.error.OK
                self.response.size = len(data)
                self.response.crc64 = \
                    uavcan.dsdl.signature.compute_signature(data)
                self.response.entry_type.flags = \
                    (self.response.entry_type.FLAG_FILE |
                     self.response.entry_type.FLAG_READABLE)
        except Exception:
            logging.exception("[#{0:03d}:uavcan.protocol.file.GetInfo] error")
            self.response.error.value = self.response.error.UNKNOWN_ERROR
            self.response.size = 0
            self.response.crc64 = 0
            self.response.entry_type.flags = 0


class FileReadHandler(uavcan.node.ServiceHandler):
    def __init__(self, *args, **kwargs):
        super(FileReadHandler, self).__init__(*args, **kwargs)
        self.base_path = kwargs.get("path")

    def on_request(self):
        logging.debug(("[#{0:03d}:uavcan.protocol.file.Read] {1!r} @ " +
                       "offset {2:d}").format(self.transfer.source_node_id,
                                              self.request.path.path.decode(),
                                              self.request.offset))
        try:
            vpath = self.request.path.path.decode()
            with open(os.path.join(self.base_path, vpath), "rb") as fw:
                fw.seek(self.request.offset)
                for byte in fw.read(250):
                    self.response.data.append(ord(byte))
                self.response.error.value = self.response.error.OK
        except Exception:
            logging.exception("[#{0:03d}:uavcan.protocol.file.Read] error")
            self.response.error.value = self.response.error.UNKNOWN_ERROR


class DebugLogMessageHandler(uavcan.node.MessageHandler):
    def on_message(self, message):
        logmsg = "[#{0:03d}:{1}] {2}".format(self.transfer.source_node_id,
                                             message.source.decode(),
                                             message.text.decode())
        (logging.debug, logging.info,
            logging.warning, logging.error)[message.level.value](logmsg)

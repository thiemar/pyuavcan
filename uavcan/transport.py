#encoding=utf-8

import time
import math
import ctypes
import struct
import logging
import binascii
import functools
import collections


import uavcan.dsdl as dsdl
import uavcan.dsdl.common as common


def bits_from_bytes(s):
    return "".join(format(c, "08b") for c in s)


def bytes_from_bits(s):
    return bytearray(int(s[i:i+8], 2) for i in xrange(0, len(s), 8))


def be_from_le_bits(s, bitlen):
    if len(s) < bitlen:
        raise ValueError("Not enough bits; need {0} but got {1}".format(
                         bitlen, len(s)))
    elif len(s) > bitlen:
        s = s[0:bitlen]

    return "".join([s[i:i + 8] for i in xrange(0, len(s), 8)][::-1])


def le_from_be_bits(s, bitlen):
    if len(s) < bitlen:
        raise ValueError("Not enough bits; need {0} but got {1}".format(
                         bitlen, len(s)))
    elif len(s) > bitlen:
        s = s[len(s) - bitlen:]

    return "".join([s[max(0, i - 8):i] for i in xrange(len(s), 0, -8)])


def format_bits(s):
    return " ".join(s[i:i+8] for i in xrange(0, len(s), 8))


# http://davidejones.com/blog/1413-python-precision-floating-point/
def f16_from_f32(float32):
    F16_EXPONENT_BITS = 0x1F
    F16_EXPONENT_SHIFT = 10
    F16_EXPONENT_BIAS = 15
    F16_MANTISSA_BITS = 0x3ff
    F16_MANTISSA_SHIFT =  (23 - F16_EXPONENT_SHIFT)
    F16_MAX_EXPONENT =  (F16_EXPONENT_BITS << F16_EXPONENT_SHIFT)

    a = struct.pack('>f', float32)
    b = binascii.hexlify(a)

    f32 = int(b, 16)
    f16 = 0
    sign = (f32 >> 16) & 0x8000
    exponent = ((f32 >> 23) & 0xff) - 127
    mantissa = f32 & 0x007fffff

    if exponent == 128:
        f16 = sign | F16_MAX_EXPONENT
        if mantissa:
            f16 |= (mantissa & F16_MANTISSA_BITS)
    elif exponent > 15:
        f16 = sign | F16_MAX_EXPONENT
    elif exponent > -15:
        exponent += F16_EXPONENT_BIAS
        mantissa >>= F16_MANTISSA_SHIFT
        f16 = sign | exponent << F16_EXPONENT_SHIFT | mantissa
    else:
        f16 = sign
    return f16


# http://davidejones.com/blog/1413-python-precision-floating-point/
def f32_from_f16(float16):
    t1 = float16 & 0x7FFF
    t2 = float16 & 0x8000
    t3 = float16 & 0x7C00

    t1 <<= 13
    t2 <<= 16

    t1 += 0x38000000
    t1 = 0 if t3 == 0 else t1
    t1 |= t2

    return struct.unpack("<f", struct.pack("<L", t1))[0]


def cast(value, dtype):
    if dtype.cast_mode == dsdl.parser.PrimitiveType.CAST_MODE_SATURATED:
        if value > dtype.value_range[1]:
            value = dtype.value_range[1]
        elif value < dtype.value_range[0]:
            value = dtype.value_range[0]
        return value
    elif (dtype.cast_mode == dsdl.parser.PrimitiveType.CAST_MODE_TRUNCATED and
            dtype.kind == dsdl.parser.PrimitiveType.KIND_FLOAT):
        if not isnan(value) and value > dtype.value_range[1]:
            value = float("+inf")
        elif not isnan(value) and value < dtype.value_range[0]:
            value = float("-inf")
        return value
    elif dtype.cast_mode == dsdl.parser.PrimitiveType.CAST_MODE_TRUNCATED:
        return value & ((1 << dtype.bitlen) - 1)
    else:
        raise ValueError("Invalid cast_mode: " + repr(dtype))


def _mask(length):
    return (1 << length) - 1


def _get_field(value, offset, length):
    return (value >> offset) & _mask(length)


def _set_field(base_value, new_value, offset, length):
    return (base_value & ~(_mask(length) << offset)) | \
           ((new_value & _mask(length)) << offset)


class TransferPriority(object):
    HIGH = 0
    NORMAL = 1
    SERVICE = 2
    LOW = 3

class BaseValue(object):
    def __init__(self, uavcan_type, *args, **kwargs):
        self.type = uavcan_type
        self._bits = None

    def unpack(self, stream):
        if self.type.bitlen:
            self._bits = be_from_le_bits(stream, self.type.bitlen)
            return stream[self.type.bitlen:]
        else:
            return stream

    def pack(self):
        if self._bits:
            return le_from_be_bits(self._bits, self.type.bitlen)
        else:
            return "0" * self.type.bitlen


class PrimitiveValue(BaseValue):
    @property
    def value(self):
        if not self._bits:
            raise ValueError("Undefined value")

        int_value = int(self._bits, 2)
        if self.type.kind == dsdl.parser.PrimitiveType.KIND_BOOLEAN:
            return int_value
        elif self.type.kind == dsdl.parser.PrimitiveType.KIND_UNSIGNED_INT:
            return int_value
        elif self.type.kind == dsdl.parser.PrimitiveType.KIND_SIGNED_INT:
            if int_value >= (1 << (self.type.bitlen - 1)):
                int_value = -((1 << self.type.bitlen) - int_value)
            return int_value
        elif self.type.kind == dsdl.parser.PrimitiveType.KIND_FLOAT:
            if self.type.bitlen == 16:
                return f32_from_f16(int_value)
            elif self.type.bitlen == 32:
                return struct.unpack("<f", struct.pack("<L", int_value))[0]
            else:
                raise ValueError("Only 16- or 32-bit floats are supported")

    @value.setter
    def value(self, new_value):
        if new_value is None:
            raise ValueError("Can't serialize a None value")
        elif self.type.kind == dsdl.parser.PrimitiveType.KIND_BOOLEAN:
            self._bits = "1" if new_value else "0"
        elif self.type.kind == dsdl.parser.PrimitiveType.KIND_UNSIGNED_INT:
            new_value = cast(new_value, self.type)
            self._bits = format(new_value, "0" + str(self.type.bitlen) + "b")
        elif self.type.kind == dsdl.parser.PrimitiveType.KIND_SIGNED_INT:
            new_value = cast(new_value, self.type)
            self._bits=  format(new_value, "0" + str(self.type.bitlen) + "b")
        elif self.type.kind == dsdl.parser.PrimitiveType.KIND_FLOAT:
            new_value = cast(new_value, self.type)
            if self.type.bitlen == 16:
                int_value = f16_from_f32(new_value)
            elif self.type.bitlen == 32:
                int_value = \
                    struct.unpack("<L", struct.pack("<f", new_value))[0]
            else:
                raise ValueError("Only 16- or 32-bit floats are supported")
            self._bits = format(int_value, "0" + str(self.type.bitlen) + "b")


class ArrayValue(BaseValue, collections.MutableSequence):
    def __init__(self, uavcan_type, tao=False, *args, **kwargs):
        super(ArrayValue, self).__init__(uavcan_type, *args, **kwargs)
        value_bitlen = getattr(self.type.value_type, "bitlen", None)
        self._tao = tao if value_bitlen >= 8 else False
        if isinstance(self.type.value_type, dsdl.parser.PrimitiveType):
            self.__item_ctor = functools.partial(PrimitiveValue,
                                                 self.type.value_type)
        elif isinstance(self.type.value_type, dsdl.parser.ArrayType):
            self.__item_ctor = functools.partial(ArrayValue,
                                                 self.type.value_type)
        elif isinstance(self.type.value_type, dsdl.parser.CompoundType):
            self.__item_ctor = functools.partial(CompoundValue,
                                                 self.type.value_type)
        if self.type.mode == dsdl.parser.ArrayType.MODE_STATIC:
            self.__items = list(self.__item_ctor()
                                for i in xrange(self.type.max_size))
        else:
            self.__items = []

    def __repr__(self):
        return "ArrayValue(type={0!r}, tao={1!r}, items={2!r})".format(
                self.type, self._tao, self.__items)

    def __str__(self):
        return self.__repr__()

    def __getitem__(self, idx):
        if isinstance(self.__items[idx], PrimitiveValue):
            return self.__items[idx].value if self.__items[idx]._bits else 0
        else:
            return self.__items[idx]

    def __setitem__(self, idx, value):
        if idx >= self.type.max_size:
            raise IndexError(("Index {0} too large (max size " +
                              "{1})").format(idx, self.type.max_size))
        if isinstance(self.type.value_type, dsdl.parser.PrimitiveType):
            self.__items[idx].value = value
        else:
            self.__items[idx] = value

    def __delitem__(self, idx):
        del self.__items[idx]

    def __len__(self):
        return len(self.__items)

    def insert(self, idx, value):
        if idx >= self.type.max_size:
            raise IndexError(("Index {0} too large (max size " +
                              "{1})").format(idx, self.type.max_size))
        elif len(self) == self.type.max_size:
            raise IndexError(("Array already full (max size "
                              "{0})").format(self.type.max_size))
        if isinstance(self.type.value_type, dsdl.parser.PrimitiveType):
            new_item = self.__item_ctor()
            new_item.value = value
            self.__items.insert(idx, new_item)
        else:
            self.__items.insert(idx, value)

    def unpack(self, stream):
        if self.type.mode == dsdl.parser.ArrayType.MODE_STATIC:
            for i in xrange(self.type.max_size):
                stream = self.__items[i].unpack(stream)
        elif self._tao:
            del self[:]
            while len(stream) >= 8:
                new_item = self.__item_ctor()
                stream = new_item.unpack(stream)
                self.__items.append(new_item)
            stream = ""
        else:
            del self[:]
            count_width = int(math.ceil(math.log(self.type.max_size, 2))) or 1
            count = int(stream[0:count_width], 2)
            stream = stream[count_width:]
            for i in xrange(count):
                new_item = self.__item_ctor()
                stream = new_item.unpack(stream)
                self.__items.append(new_item)

        return stream

    def pack(self):
        if self.type.mode == dsdl.parser.ArrayType.MODE_STATIC:
            items = "".join(i.pack() for i in self.__items)
            if len(self) < self.type.max_size:
                empty_item = self.__item_ctor()
                items += "".join(empty_item.pack() for i in
                                 xrange(self.type.max_size - len(self)))
            return items
        elif self._tao:
            return "".join(i.pack() for i in self.__items)
        else:
            count_width = int(math.ceil(math.log(self.type.max_size, 2))) or 1
            count = format(len(self), "0{0:1d}b".format(count_width))
            return count + "".join(i.pack() for i in self.__items)

    def from_bytes(self, value):
        del self[:]
        for byte in bytearray(value):
            self.append(byte)

    def to_bytes(self):
        return bytes(bytearray(item.value for item in self.__items
                               if item._bits))

    def encode(self, value):
        del self[:]
        value = bytearray(value, encoding="utf-8")
        for byte in value:
            self.append(byte)

    def decode(self, encoding="utf-8"):
        return bytearray(item.value for item in self.__items
                         if item._bits).decode(encoding)


class CompoundValue(BaseValue):
    def __init__(self, uavcan_type, mode=None, tao=False, *args, **kwargs):
        self.__dict__["fields"] = collections.OrderedDict()
        self.__dict__["constants"] = {}
        super(CompoundValue, self).__init__(uavcan_type, *args, **kwargs)
        self.mode = mode
        self.data_type_id = self.type.default_dtid
        self.crc_base = ""

        source_fields = None
        source_constants = None
        if self.type.kind == dsdl.parser.CompoundType.KIND_SERVICE:
            if self.mode == "request":
                source_fields = self.type.request_fields
                source_constants = self.type.request_constants
            elif self.mode == "response":
                source_fields = self.type.response_fields
                source_constants = self.type.response_constants
            else:
                raise ValueError("mode must be either 'request' or " +
                                 "'response' for service types")
        else:
            source_fields = self.type.fields
            source_constants = self.type.constants

        for constant in source_constants:
            self.constants[constant.name] = constant.value

        for field in source_fields:
            atao = field is source_fields[-1] and tao
            if isinstance(field.type, dsdl.parser.PrimitiveType):
                self.fields[field.name] = PrimitiveValue(field.type)
            elif isinstance(field.type, dsdl.parser.ArrayType):
                self.fields[field.name] = ArrayValue(field.type, tao=atao)
            elif isinstance(field.type, dsdl.parser.CompoundType):
                self.fields[field.name] = CompoundValue(field.type, tao=atao)

    def __getattr__(self, attr):
        if attr in self.constants:
            return self.constants[attr]
        elif attr in self.fields:
            if isinstance(self.fields[attr], PrimitiveValue):
                return self.fields[attr].value
            else:
                return self.fields[attr]
        else:
            raise AttributeError(attr)

    def __setattr__(self, attr, value):
        if attr in self.constants:
            raise AttributeError(attr + " is read-only")
        elif attr in self.fields:
            if isinstance(self.fields[attr].type, dsdl.parser.PrimitiveType):
                self.fields[attr].value = value
            else:
                raise AttributeError(attr + " cannot be set directly")
        else:
            super(CompoundValue, self).__setattr__(attr, value)

    def unpack(self, stream):
        for field in self.fields.itervalues():
            stream = field.unpack(stream)
        return stream

    def pack(self):
        return "".join(field.pack() for field in self.fields.itervalues())


class Frame(object):
    COMMON_ID_FIELDS = {
        "transfer_id": (0, 3),
        "last_frame": (3, 1),
        "transfer_priority": (27, 2)
    }

    MESSAGE_ID_FIELDS = {
        "frame_index": (4, 4),
        "broadcast_not_unicast": (8, 1),
        "source_node_id": (9, 7),
        "data_type_id": (16, 11)
    }

    SERVICE_ID_FIELDS = {
        "frame_index": (4, 6),
        "source_node_id": (10, 7),
        "data_type_id": (17, 9),
        "request_not_response": (26, 1)
    }

    def __init__(self, message_id=None, raw_payload=None):
        self.message_id = message_id
        self._payload = bytearray(raw_payload) if raw_payload else None

    def __str__(self):
        return ("Frame(message_id={0!r}, raw_payload={1!r}): " +
                "transfer_id={2}, last_frame={3}, frame_index={4}, " +
                "source_node_id={5}, transfer_priority={6}, data_type_id={7}, " +
                "dest_node_id={8}, payload={9}").format(
                self.message_id, self._payload, self.transfer_id,
                self.last_frame, self.frame_index, self.source_node_id,
                self.transfer_priority, self.data_type_id, self.dest_node_id,
                format_bits(bits_from_bytes(self.payload)))

    def __repr__(self):
        return "Frame(message_id={0!r}, raw_payload={1!r})".format(
                self.message_id, self._payload)

    def _get_field(self, field_name):
        if field_name in Frame.COMMON_ID_FIELDS:
            return _get_field(self.message_id,
                              *Frame.COMMON_ID_FIELDS[field_name])

        priority = _get_field(self.message_id,
                              *Frame.COMMON_ID_FIELDS["transfer_priority"])
        if priority == TransferPriority.SERVICE:
            return _get_field(self.message_id,
                              *Frame.SERVICE_ID_FIELDS[field_name])
        else:
            return _get_field(self.message_id,
                              *Frame.MESSAGE_ID_FIELDS[field_name])

    def _set_field(self, field_name, value):
        if field_name in Frame.COMMON_ID_FIELDS:
            self.message_id = _set_field(self.message_id, value,
                                         *Frame.COMMON_ID_FIELDS[field_name])
        else:
            priority = _get_field(
                self.message_id, *Frame.COMMON_ID_FIELDS["transfer_priority"])
            if priority == TransferPriority.SERVICE:
                self.message_id = _set_field(
                    self.message_id, value,
                    *Frame.SERVICE_ID_FIELDS[field_name])
            else:
                self.message_id = _set_field(
                    self.message_id, value,
                    *Frame.MESSAGE_ID_FIELDS[field_name])

    @property
    def transfer_priority(self):
        return self._get_field("transfer_priority")

    @transfer_priority.setter
    def transfer_priority(self, value):
        self._set_field("transfer_priority", value)

    @property
    def transfer_id(self):
        if not self.source_node_id:  # Always 0 for anonymous frames
            return 0
        else:
            return self._get_field("transfer_id")

    @transfer_id.setter
    def transfer_id(self, value):
        self._set_field("transfer_id", value)

    @property
    def last_frame(self):
        # Always true for anonymous frames
        if not self.source_node_id or self._get_field("last_frame"):
            return True
        else:
            return False

    @last_frame.setter
    def last_frame(self, value):
        self._set_field("last_frame", value)

    @property
    def frame_index(self):
        if not self.source_node_id:  # Always 0 for anonymous frames
            return 0
        else:
            return self._get_field("frame_index")

    @frame_index.setter
    def frame_index(self, value):
        self._set_field("frame_index", value)

    @property
    def source_node_id(self):
        return self._get_field("source_node_id")

    @source_node_id.setter
    def source_node_id(self, value):
        self._set_field("source_node_id", value)

    @property
    def data_type_id(self):
        return self._get_field("data_type_id")

    @data_type_id.setter
    def data_type_id(self, value):
        self._set_field("data_type_id", value)

    @property
    def broadcast_not_unicast(self):
        try:
            return self._get_field("broadcast_not_unicast")
        except Exception:
            return False

    @broadcast_not_unicast.setter
    def broadcast_not_unicast(self, value):
        try:
            self._set_field("broadcast_not_unicast", value)
        except Exception:
            pass

    @property
    def request_not_response(self):
        try:
            return self._get_field("request_not_response")
        except Exception:
            return False

    @request_not_response.setter
    def request_not_response(self, value):
        try:
            self._set_field("request_not_response", value)
        except Exception:
            pass

    @property
    def dest_node_id(self):
        if not self.broadcast_not_unicast:
            return self._payload[0] & 0x7F
        else:
            return None

    @dest_node_id.setter
    def dest_node_id(self, value):
        if value:
            self.broadcast_not_unicast = False
            if not self.payload:
                self._payload = bytearray([value & 0x7F])
            else:
                self._payload[0] = value & 0x7F
        else:
            self.broadcast_not_unicast = True

    @property
    def payload(self):
        if self.broadcast_not_unicast:
            return self._payload
        elif self._payload:
            return self._payload[1:]
        else:
            return None

    @payload.setter
    def payload(self, value):
        value = bytearray(value)
        if self.broadcast_not_unicast:
            if len(value) > 8:
                raise IndexError("Maximum broadcast frame payload size is 8")
            else:
                self._payload = value
        else:
            if len(value) > 7:
                raise IndexError("Maximum unicast frame payload size is 7")
            elif self._payload:
                self._payload[1:] = value
            else:
                self._payload = bytearray([0]) + value

    @property
    def transfer_key(self):
        if self.broadcast_not_unicast:
            return (self.source_node_id, self.data_type_id, self.transfer_id,
                    self.transfer_priority)
        else:
            return (self.source_node_id, self.dest_node_id, self.data_type_id,
                    self.transfer_id, self.transfer_priority)

    def to_bytes(self):
        return self._payload


class Transfer(object):
    def __init__(self, transfer_id=0, source_node_id=0, data_type_id=0,
                 dest_node_id=None, payload=0,
                 transfer_priority=TransferPriority.NORMAL,
                 request_not_response=False, broadcast_not_unicast=False):
        self.transfer_priority = transfer_priority
        self.transfer_id = transfer_id
        self.source_node_id = source_node_id
        self.data_type_id = data_type_id
        self.dest_node_id = dest_node_id
        self.data_type_signature = 0
        self.request_not_response = request_not_response
        self.broadcast_not_unicast = broadcast_not_unicast

        if isinstance(payload, CompoundValue):
            payload_bits = payload.pack()
            if len(payload_bits) & 7:
                payload_bits += "0" * (8 - (len(payload_bits) & 7))
            self.payload = bytes_from_bits(payload_bits)
            self.data_type_id = payload.type.default_dtid
            self.data_type_signature = payload.type.get_data_type_signature()
        else:
            self.payload = payload

        self.is_complete = True if self.payload else False

    def to_frames(self, datatype_crc=None):
        # Broadcast frames support up to 8 bytes, other frames have a
        # destination node ID which consumes the first byte of the message
        if self.dest_node_id is not None:
            bytes_per_frame = 8
        else:
            bytes_per_frame = 7

        out_frames = []
        remaining_payload = self.payload

        # Prepend the transfer CRC to the payload if the transfer requires
        # multiple frames
        if len(remaining_payload) > bytes_per_frame:
            crc = common.crc16_from_bytes(self.payload, initial=datatype_crc)
            remaining_payload = bytearray([crc & 0xFF, crc >> 8]) + \
                                remaining_payload

        # Generate the frame sequence
        while True:
            frame = Frame(message_id=0)
            frame.transfer_priority = self.transfer_priority
            frame.transfer_id = self.transfer_id
            frame.frame_index = len(out_frames)
            frame.last_frame = len(remaining_payload) <= bytes_per_frame
            frame.source_node_id = self.source_node_id
            frame.data_type_id = self.data_type_id
            frame.dest_node_id = self.dest_node_id
            frame.payload = remaining_payload[0:bytes_per_frame]
            frame.request_not_response = self.request_not_response
            frame.broadcast_not_unicast = False if self.dest_node_id else True

            out_frames.append(frame)
            remaining_payload = remaining_payload[bytes_per_frame:]
            if not remaining_payload:
                break

        return out_frames

    def from_frames(self, frames, datatype_crc=None):
        # Ignore frame index for anonymous transfers since they can't be
        # multi-frame
        if frames[0].source_node_id:
            for i, f in enumerate(frames):
                if i != f.frame_index:
                    raise IndexError(("Frame index mismatch: expected {0}, " +
                                      "got {1}").format(i, f.frame_index))

        self.transfer_id = frames[0].transfer_id
        self.transfer_priority = frames[0].transfer_priority
        self.source_node_id = frames[0].source_node_id
        self.data_type_id = frames[0].data_type_id
        self.dest_node_id = frames[0].dest_node_id
        self.payload = sum((f.payload for f in frames), bytearray())
        self.request_not_response = frames[0].request_not_response
        self.broadcast_not_unicast = frames[0].broadcast_not_unicast

        # For a multi-frame transfer, validate the CRC and frame indexes
        if len(frames) > 1:
            transfer_crc = self.payload[0] + (self.payload[1] << 8)
            self.payload = self.payload[2:]
            crc = common.crc16_from_bytes(self.payload, initial=datatype_crc)
            if crc != transfer_crc:
                raise ValueError(("CRC mismatch: expected {0:x}, got {1:x} " +
                                  "for payload {2!r} (DTID {3:d})").format(
                                  crc, transfer_crc, self.payload,
                                  self.data_type_id))

    def is_message(self):
        return self.transfer_priority != TransferPriority.SERVICE

    def is_service(self):
        return self.transfer_priority == TransferPriority.SERVICE

    def is_request(self):
        return self.is_service() and self.request_not_response

    def is_response(self):
        return self.is_service() and not self.request_not_response

    def is_broadcast(self):
        return self.is_message() and self.broadcast_not_unicast

    def is_unicast(self):
        return self.is_message() and not self.broadcast_not_unicast

    @property
    def key(self):
        if self.dest_node_id is None:
            return (self.source_node_id, self.data_type_id, self.transfer_id,
                    self.transfer_priority)
        else:
            return (self.source_node_id, self.dest_node_id, self.data_type_id,
                    self.transfer_id, self.transfer_priority)

    def is_response_to(self, transfer):
        if self.transfer_priority == TransferPriority.SERVICE and \
                self.source_node_id == transfer.dest_node_id and \
                self.dest_node_id == transfer.source_node_id and \
                self.data_type_id == transfer.data_type_id and \
                not self.request_not_response:
            return True
        else:
            return False


class TransferManager(object):
    def __init__(self):
        self.active_transfers = collections.defaultdict(list)
        self.active_transfer_timestamps = {}

    def receive_frame(self, frame):
        key = frame.transfer_key
        self.active_transfers[key].append(frame)
        self.active_transfer_timestamps[key] = time.time()

        # If the last frame of a transfer was received, return its frames
        result = None
        if frame.last_frame:
            result = self.active_transfers[key]
            del self.active_transfers[key]
            del self.active_transfer_timestamps[key]

        return result

    def remove_inactive_transfers(self, timeout=1.0):
        t = time.time()
        transfer_keys = self.active_transfers.keys()
        for key in transfer_keys:
            if t - self.active_transfer_timestamps[key] > timeout:
                del self.active_transfers[key]
                del self.active_transfer_timestamps[key]

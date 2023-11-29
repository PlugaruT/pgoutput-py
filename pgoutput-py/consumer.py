import psycopg2
import psycopg2.extras
from psycopg2.extras import LogicalReplicationConnection, StopReplication, ReplicationMessage
import struct
import io
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from abc import ABC


@dataclass(frozen=True)
class TupleData:
    nr_columns: int
    columns: list


@dataclass(frozen=True)
class ColumnData:
    type: str
    length: int = 0
    value: any = None


class ChangeEvent:
    def __repr__(self) -> str:
        return f"""
        {self.__class__.__name__}(self.relation_id={self.relation_id}, self.before={self.before}, self.after={self.after})
        """


class WALMessage(ABC):
    def _process_timestamp(self, ts_microsec: int) -> datetime:
        ts = datetime(2000, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
        return ts + timedelta(microseconds=ts_microsec)

    def read_tuple_data(self, buffer: io.BytesIO) -> TupleData:
        columns = []
        nr_columns = struct.unpack(">h", buffer.read(2))[0]
        for _ in range(nr_columns):
            col_type = struct.unpack(">c", buffer.read(1))[0].decode()
            match col_type:
                case "n":
                    columns.append(ColumnData(type="null"))
                case "u":
                    columns.append(ColumnData(type="toast"))
                case "t":
                    column_data_length = struct.unpack(">i", buffer.read(4))[0]
                    column_value = struct.unpack(f">{column_data_length}s", buffer.read(column_data_length))[0].decode()
                    columns.append(ColumnData(type="text", length=column_data_length, value=column_value))
        return TupleData(nr_columns=nr_columns, columns=columns)


class Begin(WALMessage):
    def __init__(self, payload: bytes) -> None:
        if payload[:1].decode("utf-8") != "B":
            raise ValueError("Invalid message type. Expected 'B', got '%s'" % payload[:1].decode("utf-8"))

        self.lsn, self.commit_ts, self.tx_id = struct.unpack(">qqi", payload[1:])
        self.commit_ts = self._process_timestamp(self.commit_ts)

    def __repr__(self) -> str:
        return f"Begin(lsn={self.lsn}, commit_ts={self.commit_ts}, tx_id={self.tx_id})"

class Commit(WALMessage):
    def __init__(self, payload: bytes) -> None:
        if payload[:1].decode("utf-8") != "C":
            raise ValueError("Invalid message type. Expected 'C', got '%s'" % payload[:1].decode("utf-8"))

        self.flags, self.lsn, self.commit_lsn, self.commit_ts = struct.unpack(">bqqq", payload[1:])
        self.commit_ts = self._process_timestamp(self.commit_ts)

    def __repr__(self) -> str:
        return f"Commit(flags={self.flags}, lsn={self.lsn}, commit_lsn={self.commit_lsn}, commit_ts={self.commit_ts})"

class Relation(WALMessage):
    def __init__(self, payload: bytes) -> None:
        if payload[:1].decode("utf-8") != "R":
            raise ValueError("Invalid message type. Expected 'R', got '%s'" % payload[:1].decode("utf-8"))
        buffer = io.BytesIO(payload[1:])
        self.relation_id = struct.unpack(">i", buffer.read(4))[0]
        self.namespace = struct.unpack(">s", buffer.read(1))[0].decode()


class Update(WALMessage, ChangeEvent):
    def __init__(self, payload: bytes) -> None:
        if payload[:1].decode("utf-8") != "U":
            raise ValueError("Invalid message type. Expected 'U', got '%s'" % payload[:1].decode("utf-8"))
        buffer = io.BytesIO(payload[1:])
        self.relation_id = struct.unpack(">i", buffer.read(4))[0]
        self.before = None

        identifier = struct.unpack(">s", buffer.read(1))[0].decode()

        if identifier in ("K", "O"):
            self.before = self.read_tuple_data(buffer)
            identifier = struct.unpack(">s", buffer.read(1))[0].decode()
            if identifier != "N":
                raise ValueError("Invalid message type. Expected 'N', got '%s'" % identifier)

        self.after = self.read_tuple_data(buffer)


class Insert(WALMessage, ChangeEvent):
    def __init__(self, payload: bytes) -> None:
        if payload[:1].decode("utf-8") != "I":
            raise ValueError("Invalid message type. Expected 'I', got '%s'" % payload[:1].decode("utf-8"))
        buffer = io.BytesIO(payload[1:])
        self.relation_id = struct.unpack(">i", buffer.read(4))[0]
        identifier = struct.unpack(">s", buffer.read(1))[0].decode()

        if identifier != "N":
            raise ValueError("Invalid message type. Expected 'N', got '%s'" % identifier)

        self.before = None
        self.after = self.read_tuple_data(buffer)


class Delete(WALMessage, ChangeEvent):
    def __init__(self, payload: bytes) -> None:
        if payload[:1].decode("utf-8") != "D":
            raise ValueError("Invalid message type. Expected 'D', got '%s'" % payload[:1].decode("utf-8"))
        buffer = io.BytesIO(payload[1:])
        self.relation_id = struct.unpack(">i", buffer.read(4))[0]
        identifier = struct.unpack(">s", buffer.read(1))[0].decode()

        if identifier not in ("K", "O"):
            raise ValueError("Invalid message type. Expected 'K' or 'O', got '%s'" % identifier)

        self.before = self.read_tuple_data(buffer)
        self.after = None

class FiniteConsumer:
    def __init__(self, n: int, ack: bool = False) -> None:
        self.n = n
        self.ack = ack

    def __call__(self, message: ReplicationMessage):
        self.process_message(message)

    def process_message(self, message: ReplicationMessage):
        match message.payload[:1].decode("utf-8"):
            case "B":
                print(f"Begin Raw: {message.wal_end} {message.data_start}")
                msg = Begin(message.payload)
                print(msg)
            case "U":
                print(f"Update Raw: {message.wal_end} {message.data_start}")
                msg = Update(message.payload)
                print(msg)
                self.n -= 1
            case "I":
                print(f"Insert Raw: {message.wal_end} {message.data_start}")
                msg = Insert(message.payload)
                print(msg)
                self.n -= 1
            case "D":
                print(f"Delete Raw: {message.wal_end} {message.data_start}")
                msg = Delete(message.payload)
                print(msg)
                self.n -= 1
            case "C":
                print(f"Commit Raw: {message.wal_end} {message.data_start}")
                msg = Commit(message.payload)
                print(msg)
            case _:
                print(f"Skipping message lsn{message.wal_end} {message.payload[:1].decode('utf-8')}")
        if self.n < 0:
            raise StopReplication()
        
        if self.ack:
            message.cursor.send_feedback(flush_lsn=message.data_start)


# if __name__ == "__main__":
#     connection = psycopg2.connect(
#         "host=localhost port=6432 user=test password=test",
#         connection_factory=psycopg2.extras.LogicalReplicationConnection,
#     )
#     cursor = connection.cursor()
#     cursor.start_replication(
#         slot_name="test_slot", decode=False, options={"proto_version": 1, "publication_names": "test_pub"}
#     )

#     cursor.consume_stream(LogicalConsumer())


# b = b"B\x00\x00\x00\x00\x08\x07\x9c\xf8\x00\x02\x91d\xe0\xfc\xc6\xfe\x00\x00\x08-"  # unpack(">qqi", b[1:]) 8 bytes int, 8 bytes int, 4 bytes int
# u = b"U\x00\x00M\x00N\x00\x1ct\x00\x00\x00\x0292t\x00\x00\x00\x011nnt\x00\x00\x00\x05open1t\x00\x00\x00\x06normalt\x00\x00\x00\x08facebookt\x00\x00\x00\x05emailt\x00\x00\x00\x01ft\x00\x00\x00\x01ft\x00\x00\x00\x015t\x00\x00\x00\x012nt\x00\x00\x00\x17Update credit card infonnt\x00\x00\x00\x02ent\x00\x00\x00\x1a2022-10-31 12:03:06.803033nnnt\x00\x00\x00\x1a2022-10-31 12:03:06.803033t\x00\x00\x00\x1a2022-10-31 12:03:06.803033t\x00\x00\x00\x1a2022-10-31 12:03:06.803033nnt\x00\x00\x01\x86'2':21 'a':19 'account':36 'acme':11B 'ago':23 'but':24 'can':40 'card':3A,7A,31 'credit':2A,6A,30 'curie':10B 'days':22 'expired':38 'has':37 'hi':13 'how':39 'i':14,25,41 'info':4A,8A 'it':44 'marie':9B 'my':35 'on':34 'please':42 'realized':27 'receive':18 'refund':20 'registered':33 'support':12B,16 'thanks':45 'that':28 'thatis':32 'the':29 'to':17 'update':1A,5A,43 've':26 'was':15n"
# c = b"C\x00\x00\x00\x00\x00\x08\x07\x9c\xf8\x00\x00\x00\x00\x08\x07\x9d(\x00\x02\x91d\xe0\xfc\xc6\xfe"

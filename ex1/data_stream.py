from abc import ABC, abstractmethod
from typing import Any, List, Dict, Optional, Union


class DataStream(ABC):
    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {"stream_id": self.stream_id,
                "format": f"Stream ID: {self.stream_id}"}


class SensorStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.type = "Environmental Data"

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            data_len = len(data_batch)
            qnt_temp = len([value for data in data_batch
                            for key, value in data.items()
                            if key == "temp"])
            temp_sum = sum([value for data in data_batch
                            for key, value in data.items()
                            if key == "temp"])
            avg = temp_sum / qnt_temp
        except ZeroDivisionError:
            return f"{data_len} readings processed"
        return f"{data_len} readings processed, avg temp: {avg:.1f}°C"

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria == "high-priority" or criteria == "critical":
            return [{key: value}
                    for data in data_batch
                    if isinstance(data, dict)
                    for key, value in data.items()
                    if isinstance(key, str)
                    and isinstance(value, (int, float))
                    if (key == "temp" and (value >= 20 or value <= 5))
                    or (key == "humidity" and (value >= 60 or value <= 30))
                    or (key == "pressure" and (value >= 1000 or value <= 900))]
        return [{key: value}
                for data in data_batch
                if isinstance(data, dict)
                for key, value in data.items()]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stat = super().get_stats()
        stat.update({"type": self.type})
        stat['format'] = f"{stat['format']}, Type: {stat['type']}"
        return stat


class TransactionStream(DataStream):
    def __init__(self, stream_id):
        super().__init__(stream_id)
        self.type = "Financial Data"

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            data_len = len(data_batch)
            net_flow = sum([-value if key == "sell"
                            else value
                            for data in data_batch
                            for key, value in data.items()
                            if key in ("buy", "sell")])
        except Exception:
            return "Error"
        msg = f"{data_len} operations"
        if net_flow > 0:
            msg += f", net flow: {'+' if net_flow > 0 else ''}{net_flow} units"
        return msg

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria == "high-priority" or criteria == "large":
            return [{key: value}
                    for data in data_batch
                    if isinstance(data, dict)
                    for key, value in data.items()
                    if isinstance(key, str)
                    and isinstance(value, (int, float))
                    if (key == "buy" and value >= 70)
                    or (key == "sell" and value >= 100)]
        return [{key: value}
                for data in data_batch
                if isinstance(data, dict)
                for key, value in data.items()]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stat = super().get_stats()
        stat.update({"type": self.type})
        stat['format'] = f"{stat['format']}, Type: {stat['type']}"
        return stat


class EventStream(DataStream):
    def __init__(self, stream_id):
        super().__init__(stream_id)
        self.type = "System Events"

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            data_len = len(data_batch)
            errors = len([data for data in data_batch
                          if data == "error"])
        except Exception:
            return "error"
        msg = f"{data_len} events"
        msg += f"{f', {errors} error detected' if errors > 0 else ''}"
        return msg

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria == "high-priority" or criteria == "log":
            return [data for data in data_batch
                    if isinstance(data, str)
                    if data in ("login", "error", "logout")]
        return [data for data in data_batch
                if isinstance(data, str)]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stat = super().get_stats()
        stat.update({"type": "System Events"})
        stat['format'] = f"{stat['format']}, Type: {stat['type']}"
        return stat


class StreamProcessor():
    def __init__(self,
                 streams_batch: Optional[List[DataStream]] = None) -> None:
        self.streams = []
        self.__add_stream(streams_batch)

    def __add_stream(self, streams: List[DataStream]) -> None:
        for stream in streams:
            if isinstance(stream, DataStream):
                self.streams.append(stream)

    def __organize_batch(self, data_batch: List[Any]) -> List[Any]:
        return [self.streams[i].filter_data(data_batch, "high-priority")
                for i in range(0, 3)]

    def process_stream(self, data_batch: List[Any]) -> str:
        stream_data_batch = self.__organize_batch(data_batch)
        result = "Error"
        if len(self.streams) == len(stream_data_batch):
            result = ""
            for i in range(0, len(self.streams)):
                if isinstance(self.streams[i], SensorStream):
                    result += "- Sensor data: "
                elif isinstance(self.streams[i], TransactionStream):
                    result += "- Transaction data: "
                elif isinstance(self.streams[i], EventStream):
                    result += "- Event data: "
                tmp_str = self.streams[i].process_batch(stream_data_batch[i])
                result += tmp_str + ('\n' if ("processed" in tmp_str)
                                     else ' processed\n')
        return result

    def filter_stream(self, data_batch: List[Any],
                      criteria: Optional[str] = None) -> List[Any]:
        if criteria == "high-priority":
            result = [0, 0, 0]
            for data in data_batch:
                if isinstance(data, dict):
                    for key, value in data.items():
                        if ((key == "temp" and (value >= 20 or value <= 5))
                            or (key == "humidity" and (value >= 60
                                                       or value <= 30))
                            or (key == "pressure" and (value >= 1000
                                                       or value <= 900))):
                            result[0] += 1
                        elif ((key == "buy" and value >= 70)
                              or (key == "sell" and value >= 100)):
                            result[1] += 1
                        elif isinstance(data, str) and data == "error":
                            result[2] += 1
            return result
        return [0, 0, 0]


if __name__ == "__main__":
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")

    print("\nInitializing Sensor Stream...")
    sensor_batch = [{"temp": 22.5},
                    {"humidity": 65},
                    {"pressure": 1013}]
    sensor = SensorStream("SENSOR_001")
    filter_sensor = sensor.filter_data(sensor_batch, "high-priority")

    print(sensor.get_stats()['format'])
    print("Processing sensor batch:", sensor_batch)
    print("Sensor analysis:", sensor.process_batch(filter_sensor))

    print("\nInitializing Transaction Stream...")
    transaction_batch = [{"buy": 100},
                         {"sell": 150},
                         {"buy": 75}]
    transaction = TransactionStream("TRANS_001")
    filter_trans = transaction.filter_data(transaction_batch)

    print(transaction.get_stats()['format'])
    print("Processing transaction batch:", transaction_batch)
    print("Transaction anlysis:", transaction.process_batch(filter_trans))

    print("\nInitializing Event Stream...")
    event_batch = ["login",
                   "error",
                   "logout"]
    event = EventStream("EVENT_001")
    filter_event = event.filter_data(event_batch)

    print(event.get_stats()['format'])
    print("Processing event batch:", event_batch)
    print("Event analysis:", event.process_batch(filter_event))

    print("=== Polymorphic Stream Processing ===")
    stream = StreamProcessor([sensor, transaction, event])
    data_batch = ["login",
                  "logout",
                  {"buy": 100},
                  {"sell": 150},
                  {"buy": 50},
                  {"humidity": 65},
                  {"pressure": 1013}]
    result = stream.filter_stream(data_batch, "high-priority")
    print("Processing mixed stream types through unified interface...")

    print("\nBatch 1 Results:")
    print(stream.process_stream(data_batch))
    print("Stream filtering active: High-priority data only")
    print(f"Filtered results: {len([i for i in result if i > 0])} critical"
          + f" sensor alerts, {result[1]} large transaction")
    print("\nAll streams processed successfully. Nexus throughput optimal.")

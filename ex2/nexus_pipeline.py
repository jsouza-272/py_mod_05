from abc import ABC, abstractmethod
from typing import Any, Union, Protocol, List, Dict
import collections


class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        pass


class InputStage():
    def process(self, data: Any) -> Dict:
        processed = {}
        if isinstance(data, dict):
            for key, value in data.items():
                if key in ("sensor", "value", "unit"):
                    processed.update({key: value})
        elif isinstance(data, str) and "," in data:
            split = data.split(",")
            if len(split) % 3 == 0:
                total_actions = 0
                for _ in split:
                    if _ == "action":
                        total_actions += 1
                processed.update({"actions": total_actions})
        elif isinstance(data, list):
            if len(data) > 0:
                processed.update({"len": len(data)})
                processed.update({"sum": sum(data)})
        return processed


class TransformStage():
    def process(self, data: Any) -> Dict:
        processed = {}
        if "sensor" in data:
            processed.update({"type": "sensor"})
            processed.update({"temp": data['value']})
            processed.update({"unit": data['unit']})
            processed.update({"range": "Normal"})
            if processed["temp"] <= 5 or processed["temp"] >= 35:
                processed.update({"range": "Critical"})
        elif "actions" in data:
            processed.update(data)
        elif "len" in data:
            processed.update({"reading": data['len']})
            processed.update({"avg": data['sum'] / data['len']})
        return processed


class OutputStage():
    def process(self, data: Any) -> str:
        processed = ""
        if "type" in data:
            processed += f"Processed temperature reading: \
{data['temp']:.1f}°{data['unit']} ({data['range']} range)"
        elif "actions" in data:
            processed += f"User activity logged: \
{data['actions']} actions processed"
        elif "avg" in data:
            processed += f"Stream summary: {data['reading']} \
readings, avg: {data['avg']}°C"
        return processed


class ProcessingPipeline(ABC):
    def __init__(self, pipeline_id: str):
        self.stages = []
        self.id = pipeline_id

    def add_stage(self, stage: ProcessingStage) -> None:
        if stage is not None:
            self.stages.append(stage)

    @abstractmethod
    def process(data) -> Any:
        pass


class JSONAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str):
        super().__init__(pipeline_id)

    def process(self, data: Any) -> Union[str, Any]:
        print("\nProcessing JSON data through pipeline...")
        print(f"Input: {data}")
        processed = data
        if len(self.stages) > 0:
            print("Transform: Enriched with metadata and validation")
            for stage in self.stages:
                processed = stage.process(processed)
        return processed


class CSVAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id):
        super().__init__(pipeline_id)

    def process(self, data: Any) -> Union[str, Any]:
        print("\nProcessing CSV data through same pipeline...")
        print(f'Input: "{data}"')
        processed = data
        if len(self.stages) > 0:
            print("Transform: Parsed and structured data")
            for stage in self.stages:
                processed = stage.process(processed)
        return processed


class StreamAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id):
        super().__init__(pipeline_id)

    def process(self, data: Any) -> Union[str, Any]:
        processed = data
        if len(self.stages) > 0:
            for stage in self.stages:
                processed = stage.process(processed)
        return processed


if __name__ == "__main__":
    print("=== Multi-Format Data Processing ===")
    stages = [InputStage(), TransformStage(), OutputStage()]
    j_adapter = JSONAdapter("JSON_001")
    j_data = {"sensor": "temp", "value": 23.5, "unit": "C"}
    for stage in stages:
        j_adapter.add_stage(stage)
    print("Output:", j_adapter.process(j_data))

    c_adapter = CSVAdapter("CSV_001")
    c_data = "user,action,timestamp"
    for stage in stages:
        c_adapter.add_stage(stage)
    print("Output:", c_adapter.process(c_data))

    print("\nProcessing Stream data through same pipeline...")
    s_adapter = StreamAdapter("STREAM_001")
    s_data = [22.1, 22.1, 22.1, 22.1, 22.1]
    for stage in stages:
        s_adapter.add_stage(stage)
    print(f"Input: {s_data}")
    print("Transform: Enriched with metadata and validation")
    print("Output:", s_adapter.process(s_data))

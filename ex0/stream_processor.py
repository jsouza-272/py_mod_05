from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


class NumericProcessor(DataProcessor):
    def process(self, data: Any):
        try:
            sm = sum(data)
            nbr = len(data)
            avg = sm / nbr
            return f"Processed {nbr} numeric values, sum={sm}, avg={avg:.1f}"
        except Exception:
            return "Process fail"

    def validate(self, data: Any):
        if type(data) is list:
            for d in data:
                if type(d) is not int:
                    print("Validation: Data is not numeric")
                    return False
            print("Validation: Numeric data verified")
            return True
        print("Validation: Data is not numeric")
        return False

    def format_output(self, result: str) -> str:
        return super().format_output(result)


class TextProcessor(DataProcessor):
    def process(self, data: Any) -> str:
        try:
            if "ERROR" in data or "INFO" in data:
                raise Exception
            char_nbr = len(data)
            word_nbr = len(data.split())
            return f"Processed text: {char_nbr} characters, {word_nbr} words"
        except Exception:
            return "Process fail"

    def validate(self, data: Any) -> bool:
        if type(data) is str and "ERROR" not in data and "INFO" not in data:
            print("Validation: Text data verified")
            return True
        print("Validation: Data is not text")
        return False

    def format_output(self, result: str) -> str:
        return super().format_output(result)


class LogProcessor(DataProcessor):
    def process(self, data: Any) -> str:
        try:
            message = data.split(":")
            if message[0] == "ERROR":
                return f"[ALERT] ERROR level detected:{message[1]}"
            if message[0] == "INFO":
                return f"[INFO] INFO level detected:{message[1]}"
            raise Exception
        except Exception:
            return "Process fail"

    def validate(self, data: Any) -> bool:
        if type(data) is str and "ERROR" in data or "INFO" in data:
            print("Validation: Log entry verified")
            return True
        print("Validation: Log not entry")
        return False

    def format_output(self, result: str) -> str:
        return super().format_output(result)


def main() -> None:
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")
    data = [[1, 2, 3, 4, 5],
            "Hello Nexus World",
            "ERROR: Connection timeout"]

    print("\nInitializing Numeric Processor...")
    num_processor = NumericProcessor()
    print(f"Processing data: {data[0]}")
    num_processor.validate(data[0])
    print(num_processor.format_output(num_processor.process(data[0])))

    print("\nInitializing Text Processor...")
    txt_processor = TextProcessor()
    print(f'Processing data: "{data[1]}"')
    txt_processor.validate(data[1])
    print(txt_processor.format_output(txt_processor.process(data[1])))

    print("\nInitializing Log Processor...")
    log_processor = LogProcessor()
    print(f'Processing data: "{data[2]}"')
    log_processor.validate(data[2])
    print(log_processor.format_output(log_processor.process(data[2])))

    print("\n=== Polymorphic Processing Demo ===")
    print("\nProcessing multiple data types through same interface...")
    data = [[1, 2, 3],
            "Hello  World",
            "INFO: System ready"]
    processor = [num_processor, txt_processor, log_processor]
    i = 0
    while i < 3:
        print(f"Result {i + 1}: {processor[i].process(data[i])}")
        i += 1

    print("Foundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    main()

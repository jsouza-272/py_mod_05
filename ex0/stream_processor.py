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
        if self.validate(data):
            sm = sum(data)
            nbr = len(data)
            avg = sm / nbr
            return f"Processed {nbr} numeric values, sum={sm}, avg={avg:.1f}"
        return "Process fail"

    def validate(self, data: Any):
        if type(data) is list:
            for d in data:
                if type(d) is not int:
                    return False
            return True
        return False

    def format_output(self, result):
        return super().format_output(result)


class TextProcessor(DataProcessor):
    def process(self, data):
        if self.validate(data):
            char_nbr = len(data)
            word_nbr = len(data.split())
            return f"Processed text: {char_nbr} characters, {word_nbr} words"
        return "Process fail"

    def validate(self, data):
        return type(data) is str \
            and "ERROR" not in data and "INFO" not in data

    def format_output(self, result):
        return super().format_output(result)


class LogProcessor(DataProcessor):
    def process(self, data):
        if self.validate(data):
            if "ERROR" in data:
                message = data.split("ERROR:")
                return f"[ALERT] ERROR level detected: {message[1]}"
            elif "INFO" in data:
                message = data.split("INFO:")
                return f"[INFO] INFO level detected: {message[1]}"
        return "Process fail"

    def validate(self, data):
        return type(data) is str and "ERROR" in data or "INFO" in data

    def format_output(self, result):
        return super().format_output(result)


def main():
    nbr_processor = NumericProcessor()
    str_processor = TextProcessor()
    log_processor = LogProcessor()
    data = [[1, 2, 3, 4, 5],
            "Hello Nexus World",
            "ERROR: Connection timeout"]

    try:
        print("Initializing Numeric Processor...")
        print(f"Processing data: {data[0]}")
        if nbr_processor.validate(data[0]):
            print("Validation: Numeric data verified")
            print(nbr_processor.format_output(
                nbr_processor.process(data[0])))
        else:
            print("Validation: Data is not numeric")
        print()
        print("Initializing Text Processor...")
        print(f'Processing data: "{data[1]}"')
        if str_processor.validate(data[1]):
            print("Validation: Text data verified")
            print(str_processor.format_output(
                str_processor.process(data[1])))
        else:
            print("Validation: Data is not text")
        print()
        print("Initializing Log Processor...")
        print(f'Processing data: "{data[2]}"')
        if log_processor.validate(data[2]):
            print("Validation: Log entry verified")
            print(log_processor.format_output(
                log_processor.process(data[2])))
        else:
            print("Validation: Log not entry")
    except Exception as error:
        print(f"type={type(error).__name__} args={error.args}")

main()
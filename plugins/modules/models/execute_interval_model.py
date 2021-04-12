from typing import Union

class Daily:
    def get_type(self) -> str:
        return "daily"

class Weekly:
    def get_type(self) -> str:
        return "weekly"

class Monthly:
    def get_type(self) -> str:
        return "monthly"


class IntervalBase:
    def __init__(self, interval_value: int, interval_type: Union[Daily, Weekly, Monthly, None]) -> None:
        self.interval_type  = interval_type
        self.interval_value = interval_value

    def get_interval_type(self) -> Union[Daily, Weekly, Monthly, None]:
        interval_type = self.interval_type

        return interval_type.get_type(self)

    def get_interval_value(self) -> int:
        return self.interval_value


class IntervalDaily(IntervalBase):
    def __init__(self, interval: int) -> None:
        super().__init__(interval, Daily)

class IntervalWeekly(IntervalBase):
    def __init__(self, interval: int) -> None:
        super().__init__(interval, Weekly)

class IntervalMonthly(IntervalBase):
    def __init__(self, interval: int) -> None:
        super().__init__(interval, Monthly)


class ExecuteInterval:
    def __init__(self, interval: IntervalBase) -> None:
        self.interval = interval

    def get_interval(self) -> dict:
        return {
            "Type": self.interval.get_interval_type(),
            "Interval": self.interval.get_interval_value()
        }

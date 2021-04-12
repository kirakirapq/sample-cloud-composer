


def create_interval(type: str, value: int):
    """ExecuteIntervalを生成する

    Args:
        type (str): daily or weekly or monthly
        value (int): 整数値を指定

    Returns:
        ExecuteInterval: [description]
    """
    if type.lower() == "daily":
        return "daily"

    if type.lower() == "weekly":
        return "weekly"

    return "monthly"

interval = create_interval("daily", 1)



print(interval)

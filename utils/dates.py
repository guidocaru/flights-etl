import pendulum


def today() -> str:
    return pendulum.now().date().to_date_string()


def yesterday() -> str:
    return pendulum.now().subtract(days=1).date().to_date_string()

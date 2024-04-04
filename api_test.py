def check_python_version():
    from utils import check_python_version

    check_python_version()


def check_api():
    from external.client import YandexWeatherAPI
    from utils import get_url_by_city_name

    CITY_NAME_FOR_TEST = "MOSCOW"

    data_url = get_url_by_city_name(CITY_NAME_FOR_TEST)
    resp = YandexWeatherAPI.get_forecasting(data_url)
    all_keys = resp.keys()
    print(all_keys)
    print(resp["info"])


if __name__ == "__main__":
    check_python_version()
    check_api()

from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Manager
from multiprocessing import Pool
from os import cpu_count

# from external.client import YandexWeatherAPI
from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)
from utils import CITIES


def forecast_weather() -> None:
    """
    Анализ погодных условий по городам
    """
    manager = Manager()
    data_aggregation_queue = manager.Queue()

    # Получение данных через API
    data_fetching_task = DataFetchingTask()
    with ThreadPoolExecutor() as executor:
        fetched_data = executor.map(data_fetching_task, list(CITIES.keys()))

    # Вычисление погодных параметров
    data_calculation_task = DataCalculationTask(queue=data_aggregation_queue)
    with Pool(processes=cpu_count()) as pool:
        pool.map(data_calculation_task, fetched_data)

    # Объединение вычисленных данных
    data_aggregation_task = DataAggregationTask(queue=data_aggregation_queue)
    data_aggregation_task.start()
    data_aggregation_task.join()

    # Финальный анализ и получение результата
    data_analyzing_task = DataAnalyzingTask()
    data_analyzing_task()


if __name__ == "__main__":
    forecast_weather()

from logging import INFO
from logging import basicConfig
from logging import getLogger
from multiprocessing import Process
from multiprocessing import Queue

RESULT_PATH = 'output.json'

logger = getLogger(__name__)
basicConfig(level=INFO)


class DataFetchingTask:
    """Получение данных через API"""

    def __call__(self, city_name: str) -> dict:
        from external.client import YandexWeatherAPI
        from utils import get_url_by_city_name

        logger.info(f'Get data for city: {city_name}')
        url_with_data = get_url_by_city_name(city_name)
        try:
            resp = YandexWeatherAPI.get_forecasting(url_with_data)
            if resp:
                resp['city_name'] = city_name
                return resp
        except Exception:
            logger.error(f'Get data for city {city_name} failed')
        return dict()


class DataCalculationTask:
    """Вычисление погодных параметров"""

    def __init__(self, queue: Queue):
        self._output_queue = queue

    def __call__(self, city_data: dict) -> None:
        from external.analyzer import analyze_json

        if city_data:
            # Вычисление погодных параметров
            calculated_data = analyze_json(city_data)

            # Вычисление средней температуры и среднего количества дней без осадков за все дни
            avg_temperature = 0.0
            avg_hours_without_precipitation = 0.0
            days_with_avg_temperature = 0
            for day in calculated_data['days']:
                if isinstance(day['temp_avg'], float):
                    avg_temperature += day['temp_avg']
                    days_with_avg_temperature += 1
                avg_hours_without_precipitation += day['relevant_cond_hours']
            avg_temperature /= days_with_avg_temperature
            avg_hours_without_precipitation /= len(calculated_data['days'])

            logger.info(f'Calculation done: {calculated_data}')
            self._output_queue.put(
                {
                    'city_name': city_data['city_name'],
                    'days': calculated_data['days'],
                    'avg_temperature': avg_temperature,
                    'avg_hours_without_precipitation': avg_hours_without_precipitation,
                    'rating': None,
                }
            )
        else:
            logger.error(f'Calculation failed, invalid data: {city_data} ')


class DataAggregationTask(Process):
    """Объединение вычисленных данных"""

    def __init__(self, queue: Queue):
        super().__init__()
        self._input_queue = queue

    def run(self) -> None:
        from json import dumps

        result = list()
        while True:
            if self._input_queue.empty():
                logger.info('Queue is empty')
                break
            else:
                data = self._input_queue.get()
                result.append(data)
                logger.info(f'Aggregation: {data}')

        with open(RESULT_PATH, 'w') as file:
            logger.info(f'Aggregation done: {result}')
            formatted_result = dumps(result, indent=4)
            file.write(formatted_result)


class DataAnalyzingTask:
    """Финальный анализ и получение результата"""

    def __call__(self) -> None:
        from json import dumps
        from json import loads

        with open(RESULT_PATH, 'r') as file:
            data = loads(file.read())

        # Финальный анализ и получение результата
        logger.info('Start analyzing')
        city_avg_values = [
            (
                city['city_name'],
                city['avg_temperature'],
                city['avg_hours_without_precipitation'],
            ) for city in data
        ]
        city_avg_values.sort(key=lambda x: (x[1], x[2]), reverse=True)
        city_rating = dict()
        for i in range(len(city_avg_values)):
            city_rating[city_avg_values[i][0]] = i + 1
        for city in data:
            city['rating'] = city_rating[city['city_name']]
        with open(RESULT_PATH, 'w') as file:
            logger.info(f'Analyzing done: {data}')
            formatted_result = dumps(data, indent=4)
            file.write(formatted_result)

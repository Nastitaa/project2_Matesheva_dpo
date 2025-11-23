# src/primitive_db/decorators.py
import time
from functools import wraps


def handle_db_errors(func):
    """
    Декоратор для обработки ошибок базы данных.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except KeyError as e:
            print(f"Ошибка: Обращение к несуществующему ключу - {e}")
            return args[0] if args else None  
        except ValueError as e:
            print(f"Ошибка валидации данных: {e}")
            return args[0] if args else None
        except FileNotFoundError as e:
            print(f"Ошибка: Файл не найден - {e}")
            return args[0] if args else None
        except Exception as e:
            print(f"Неожиданная ошибка в функции {func.__name__}: {e}")
            return args[0] if args else None
    return wrapper


def confirm_action(action_name):
    """
    Декоратор для запроса подтверждения опасных операций.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            response = input(
                f'Вы уверены, что хотите выполнить "{action_name}"? [y/n]: '
            ).strip().lower()
            if response != 'y':
                print("Операция отменена.")
                return args[0] if args else None
            return func(*args, **kwargs)
        return wrapper
    return decorator


def log_time(func):
    """
    Декоратор для замера времени выполнения функции.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.monotonic()
        result = func(*args, **kwargs)
        end_time = time.monotonic()
        execution_time = end_time - start_time
        print(f"функция {func.__name__} выполнилась за {execution_time:.3f} секунд")
        return result
    return wrapper


def create_cacher():
    """
    Фабрика функций для кэширования результатов.
    """
    cache = {}

    def cache_result(key, value_func):
        """
        Кэширует результат выполнения функции.
        
        Args:
            key: Ключ для кэша
            value_func: Функция для получения значения, если его нет в кэше
            
        Returns:
            Результат из кэша или результат выполнения value_func
        """
        if key in cache:
            return cache[key]
        
        result = value_func()
        cache[key] = result
        return result

    return cache_result


# Создаем глобальный экземпляр кэшера
cacher = create_cacher()
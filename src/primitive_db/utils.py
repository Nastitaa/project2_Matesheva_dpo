
# src/primitive_db/utils.py

import json
import os


def load_metadata(filepath):
    """
    Загружает данные из JSON-файла.
    
    Args:
        filepath (str): Путь к JSON-файлу
        
    Returns:
        dict: Данные из файла или пустой словарь, если файл не найден
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            return json.load(file)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        print(f"Ошибка: Файл {filepath} содержит некорректный JSON")
        return {}


def save_metadata(filepath, data):
    """
    Сохраняет переданные данные в JSON-файл.
    
    Args:
        filepath (str): Путь к JSON-файлу
        data (dict): Данные для сохранения
    """
    try:
        with open(filepath, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"Ошибка при сохранении файла {filepath}: {e}")


def load_table_data(table_name):
    """
    Загружает данные таблицы из JSON-файла.
    
    Args:
        table_name (str): Имя таблицы
        
    Returns:
        list: Данные таблицы или пустой список, если файл не найден
    """
    # Создаем директорию data, если она не существует
    os.makedirs("data", exist_ok=True)
    
    filepath = f"data/{table_name}.json"
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            return json.load(file)
    except FileNotFoundError:
        return []
    except json.JSONDecodeError:
        print(f"Ошибка: Файл {filepath} содержит некорректный JSON")
        return []


def save_table_data(table_name, data):
    """
    Сохраняет данные таблицы в JSON-файл.
    
    Args:
        table_name (str): Имя таблицы
        data (list): Данные для сохранения
    """
    # Создаем директорию data, если она не существует
    os.makedirs("data", exist_ok=True)
    
    filepath = f"data/{table_name}.json"
    try:
        with open(filepath, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"Ошибка при сохранении файла {filepath}: {e}")
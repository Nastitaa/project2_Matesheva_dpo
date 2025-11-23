# src/primitive_db/core.py
from .decorators import cacher, confirm_action, handle_db_errors, log_time
from .utils import load_table_data, save_table_data


@handle_db_errors
def create_table(metadata, table_name, columns):
    """
    Создает новую таблицу в метаданных.
    """
    # Проверяем, существует ли уже таблица с таким именем
    if "tables" in metadata and table_name in metadata["tables"]:
        print(f"Ошибка: Таблица '{table_name}' уже существует")
        return metadata
    
    # Проверяем корректность типов данных
    allowed_types = {"int", "str", "bool"}
    for column_name, column_type in columns:
        if column_type not in allowed_types:
            error_msg = (
                f"Ошибка: Недопустимый тип '{column_type}' "
                f"для столбца '{column_name}'. "
                f"Допустимые типы: {', '.join(allowed_types)}"
            )
            print(error_msg)
            return metadata
    
    # Добавляем столбец ID:int в начало списка столбцов
    columns_with_id = [("ID", "int")] + columns
    
    # Инициализируем структуру таблицы в метаданных
    if "tables" not in metadata:
        metadata["tables"] = {}
    
    # Создаем запись о таблице
    metadata["tables"][table_name] = {
        "columns": columns_with_id,
        "data": []
    }
    
    # Создаем файл для данных таблицы
    save_table_data(table_name, [])
    
    print(f"Таблица '{table_name}' успешно создана")
    print(f"Столбцы: {[col[0] for col in columns_with_id]}")
    
    return metadata


@handle_db_errors
@confirm_action("удаление таблицы")
def drop_table(metadata, table_name):
    """
    Удаляет таблицу из метаданных.
    """
    # Проверяем существование таблицы
    if "tables" not in metadata or table_name not in metadata["tables"]:
        print(f"Ошибка: Таблица '{table_name}' не существует")
        return metadata
    
    # Удаляем таблицу из метаданных
    del metadata["tables"][table_name]
    
    # Удаляем файл с данными таблицы (если существует)
    import os
    filepath = f"data/{table_name}.json"
    if os.path.exists(filepath):
        os.remove(filepath)
    
    print(f"Таблица '{table_name}' успешно удалена")
    
    # Если таблиц не осталось, удаляем пустой словарь tables
    if not metadata["tables"]:
        del metadata["tables"]
    
    return metadata


@handle_db_errors
@log_time
def insert(metadata, table_name, values):
    """
    Вставляет новую запись в таблицу.
    """
    # Проверяем существование таблицы
    if "tables" not in metadata or table_name not in metadata["tables"]:
        print(f"Ошибка: Таблица '{table_name}' не существует")
        return []
    
    table_info = metadata["tables"][table_name]
    columns = table_info["columns"]
    
    # Проверяем количество значений (минус ID)
    if len(values) != len(columns) - 1:
        print(
            f"Ошибка: Ожидалось {len(columns) - 1} значений, "
            f"получено {len(values)}"
        )
        return []
    
    # Загружаем текущие данные таблицы
    table_data = load_table_data(table_name)
    
    # Генерируем новый ID
    if table_data:
        new_id = max(row["ID"] for row in table_data) + 1
    else:
        new_id = 1
    
    # Создаем новую запись
    new_row = {"ID": new_id}
    
    # Валидируем типы данных и добавляем значения
    for i, (col_name, col_type) in enumerate(columns[1:], 1):
        value = values[i - 1]
        if col_type == "int":
            validated_value = int(value)
        elif col_type == "bool":
            if isinstance(value, str):
                validated_value = value.lower() in ("true", "1", "yes")
            else:
                validated_value = bool(value)
        else:  # str
            validated_value = str(value)
        
        new_row[col_name] = validated_value
    
    # Добавляем запись и сохраняем
    table_data.append(new_row)
    save_table_data(table_name, table_data)
    
    print(f"Запись успешно добавлена в таблицу '{table_name}' (ID: {new_id})")
    return table_data


@handle_db_errors
@log_time
def select(table_data, where_clause=None):
    """
    Выбирает записи из данных таблицы.
    """
    if where_clause is None:
        return table_data
    
    # Создаем ключ для кэша на основе данных и условия
    cache_key = (
        "select_" 
        + str(hash(str(table_data))) 
        + "_" 
        + str(hash(str(where_clause)))
    )
    
    # Используем кэширование
    def perform_select():
        filtered_data = []
        for row in table_data:
            match = True
            for key, value in where_clause.items():
                if row.get(key) != value:
                    match = False
                    break
            if match:
                filtered_data.append(row)
        return filtered_data
    
    return cacher(cache_key, perform_select)


@handle_db_errors
def update(table_data, set_clause, where_clause):
    """
    Обновляет записи в данных таблицы.
    """
    updated_count = 0
    
    for row in table_data:
        match = True
        for key, value in where_clause.items():
            if row.get(key) != value:
                match = False
                break
        
        if match:
            for key, value in set_clause.items():
                if key in row and key != "ID":  # ID нельзя обновлять
                    row[key] = value
            updated_count += 1
    
    print(f"Обновлено записей: {updated_count}")
    return table_data


@handle_db_errors
@confirm_action("удаление записей")
def delete(table_data, where_clause):
    """
    Удаляет записи из данных таблицы.
    """
    if where_clause is None:
        print("Ошибка: Для удаления необходимо указать условие WHERE")
        return table_data
    
    initial_count = len(table_data)
    
    # Фильтруем данные, исключая записи, соответствующие условию
    filtered_data = []
    for row in table_data:
        match = True
        for key, value in where_clause.items():
            if row.get(key) != value:
                match = False
                break
        
        if not match:
            filtered_data.append(row)
    
    deleted_count = initial_count - len(filtered_data)
    print(f"Удалено записей: {deleted_count}")
    
    return filtered_data
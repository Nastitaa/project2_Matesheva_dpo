# src/primitive_db/parser.py

def parse_where_condition(where_str):
    """
    Парсит строку условия WHERE в словарь.
    
    Args:
        where_str (str): Строка условия, например "age = 28"
        
    Returns:
        dict: Словарь с условиями или None в случае ошибки
    """
    if not where_str:
        return None
    
    try:
        # Разделяем по оператору '='
        if '=' not in where_str:
            print("Ошибка: Неверный формат условия WHERE. Используйте: поле=значение")
            return None
        
        parts = where_str.split('=', 1)
        if len(parts) != 2:
            print("Ошибка: Неверный формат условия WHERE")
            return None
        
        field = parts[0].strip()
        value_str = parts[1].strip()
        
        # Парсим значение
        parsed_value = parse_value(value_str)
        if parsed_value is None:
            return None
        
        return {field: parsed_value}
    
    except Exception as e:
        print(f"Ошибка при разборе условия WHERE: {e}")
        return None


def parse_set_clause(set_str):
    """
    Парсит строку SET в словарь.
    
    Args:
        set_str (str): Строка SET, например "name='John', age=30"
        
    Returns:
        dict: Словарь с полями для обновления или None в случае ошибки
    """
    if not set_str:
        return None
    
    try:
        set_clause = {}
        
        # Разделяем по запятым
        assignments = set_str.split(',')
        
        for assignment in assignments:
            assignment = assignment.strip()
            if '=' not in assignment:
                print(f"Ошибка: Неверный формат присваивания: {assignment}")
                return None
            
            parts = assignment.split('=', 1)
            if len(parts) != 2:
                print(f"Ошибка: Неверный формат присваивания: {assignment}")
                return None
            
            field = parts[0].strip()
            value_str = parts[1].strip()
            
            # Парсим значение
            parsed_value = parse_value(value_str)
            if parsed_value is None:
                return None
            
            set_clause[field] = parsed_value
        
        return set_clause
    
    except Exception as e:
        print(f"Ошибка при разборе SET: {e}")
        return None


def parse_value(value_str):
    """
    Парсит строковое значение в соответствующий тип.
    
    Args:
        value_str (str): Строковое значение
        
    Returns:
        Любой тип: Распарсенное значение или None в случае ошибки
    """
    value_str = value_str.strip()
    
    # Проверяем на строку в кавычках
    if (value_str.startswith('"') and value_str.endswith('"')) or \
       (value_str.startswith("'") and value_str.endswith("'")):
        return value_str[1:-1]
    
    # Проверяем на булево значение
    if value_str.lower() in ('true', 'false'):
        return value_str.lower() == 'true'
    
    # Проверяем на целое число
    try:
        return int(value_str)
    except ValueError:
        pass
    
    # Если ничего не подошло, возвращаем как строку
    return value_str
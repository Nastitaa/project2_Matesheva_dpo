# src/primitive_db/engine.py
import shlex

from prettytable import PrettyTable

from .core import create_table, delete, drop_table, insert, select, update
from .decorators import handle_db_errors
from .parser import parse_set_clause, parse_where_condition
from .utils import load_metadata, load_table_data, save_metadata, save_table_data


def print_help():
    """Prints the help message for the current mode."""
    print("\n***Процесс работы с таблицей***")
    print("Функции управления таблицами:")
    print("  create_table <имя_таблицы> <столбец1:тип> .. - создать таблицу")
    print("  list_tables - показать список всех таблиц")
    print("  drop_table <имя_таблицы> - удалить таблицу")
    
    print("\nCRUD операции:")
    print("  insert <таблица> <значение1> <значение2> ... - добавить запись")
    print("  select <таблица> [where поле=значение] - выбрать записи")
    print("  update <таблица> set поле=значение [where поле=значение] - обновить")
    print("  delete <таблица> where поле=значение - удалить записи")
    
    print("\nОбщие команды:")
    print("  exit - выход из программы")
    print("  help - справочная информация\n")


def list_tables(metadata):
    """Показывает список всех таблиц."""
    if "tables" not in metadata or not metadata["tables"]:
        print("Таблицы не найдены")
        return
    
    print("\nСписок таблиц:")
    for table_name, table_info in metadata["tables"].items():
        columns = [f"{col[0]}:{col[1]}" for col in table_info["columns"]]
        print(f"  {table_name}: {', '.join(columns)}")


def print_table_data(table_data, columns):
    """Выводит данные таблицы в красивом формате."""
    if not table_data:
        print("Данные не найдены")
        return
    
    table = PrettyTable()
    table.field_names = [col[0] for col in columns]
    
    for row in table_data:
        table.add_row([row.get(col[0], '') for col in columns])
    
    print(table)

@handle_db_errors
def run():
    """Главная функция с основным циклом программы."""
    print("Добро пожаловать в примитивную базу данных!")
    print_help()
    
    while True:
        # Загружаем актуальные метаданные
        metadata = load_metadata("database.json")
        
        try:
            # Запрашиваем ввод у пользователя
            user_input = input("Введите команду: ").strip()
            
            # Разбираем введенную строку на команду и аргументы
            args = shlex.split(user_input)
            if not args:
                continue
                
            command = args[0].lower()
            
            # Обрабатываем команды
            if command == "exit":
                print("Выход из программы...")
                break
                
            elif command == "help":
                print_help()
                
            elif command == "create_table":
                if len(args) < 3:
                    print(
                        "Ошибка: Используйте: create_table <имя_таблицы> "
                        "<столбец1:тип> [столбец2:тип ...]"
                    )
                    continue
                
                table_name = args[1]
                columns = []
                
                for col_arg in args[2:]:
                    if ":" not in col_arg:
                        print(
                            f"Ошибка: Неверный формат столбца '{col_arg}'. "
                            "Используйте: имя:тип"
                        )
                        break
                    col_name, col_type = col_arg.split(":", 1)
                    columns.append((col_name.strip(), col_type.strip().lower()))
                else:
                    # Все столбцы успешно разобраны
                    metadata = create_table(metadata, table_name, columns)
                    save_metadata("database.json", metadata)
                    
            elif command == "list_tables":
                list_tables(metadata)
                
            elif command == "drop_table":
                if len(args) < 2:
                    print("Ошибка: Используйте: drop_table <имя_таблицы>")
                    continue
                
                table_name = args[1]
                metadata = drop_table(metadata, table_name)
                save_metadata("database.json", metadata)
                
            elif command == "insert":
                if len(args) < 3:
                    print(
                        "Ошибка: Используйте: insert <таблица> "
                        "<значение1> <значение2> ..."
                    )
                    continue
                
                table_name = args[1]
                values = args[2:]
                
                # Загружаем данные таблицы
                table_data = load_table_data(table_name)
                
                # Выполняем вставку
                new_data = insert(metadata, table_name, values)
                if new_data:
                    save_table_data(table_name, new_data)
                    print("Запись успешно добавлена")
                
            elif command == "select":
                if len(args) < 2:
                    print("Ошибка: Используйте: select <таблица> [where поле=значение]")
                    continue
                
                table_name = args[1]
                
                # Проверяем существование таблицы
                if "tables" not in metadata or table_name not in metadata["tables"]:
                    print(f"Ошибка: Таблица '{table_name}' не существует")
                    continue
                
                # Загружаем данные таблицы
                table_data = load_table_data(table_name)
                
                # Парсим условие WHERE если есть
                where_clause = None
                if len(args) > 3 and args[2].lower() == "where":
                    where_str = ' '.join(args[3:])
                    where_clause = parse_where_condition(where_str)
                
                # Выполняем выборку
                result_data = select(table_data, where_clause)
                
                # Выводим результат
                table_info = metadata["tables"][table_name]
                print_table_data(result_data, table_info["columns"])
                
            elif command == "update":
                if len(args) < 4:
                    print(
                        "Ошибка: Используйте: update <таблица> "
                        "set поле=значение [where поле=значение]"
                    )
                    continue
                
                table_name = args[1]
                
                # Проверяем существование таблицы
                if "tables" not in metadata or table_name not in metadata["tables"]:
                    print(f"Ошибка: Таблица '{table_name}' не существует")
                    continue
                
                # Загружаем данные таблицы
                table_data = load_table_data(table_name)
                
                # Парсим SET и WHERE условия
                set_str = ""
                where_str = ""
                where_index = -1
                
                # Находим индекс WHERE
                for i, arg in enumerate(args):
                    if arg.lower() == "where":
                        where_index = i
                        break
                
                if args[2].lower() != "set":
                    print("Ошибка: Ожидалось ключевое слово 'set'")
                    continue
                
                if where_index != -1:
                    set_str = ' '.join(args[3:where_index])
                    where_str = ' '.join(args[where_index + 1:])
                else:
                    set_str = ' '.join(args[3:])
                
                set_clause = parse_set_clause(set_str)
                where_clause = parse_where_condition(where_str) if where_str else None
                
                if set_clause is None:
                    continue
                
                # Выполняем обновление
                new_data = update(table_data, set_clause, where_clause)
                if new_data:
                    save_table_data(table_name, new_data)
                    print("Данные успешно обновлены")
                
            elif command == "delete":
                if len(args) < 4 or args[2].lower() != "where":
                    print("Ошибка: Используйте: delete <таблица> where поле=значение")
                    continue
                
                table_name = args[1]
                
                # Проверяем существование таблицы
                if "tables" not in metadata or table_name not in metadata["tables"]:
                    print(f"Ошибка: Таблица '{table_name}' не существует")
                    continue
                
                # Загружаем данные таблицы
                table_data = load_table_data(table_name)
                
                # Парсим условие WHERE
                where_str = ' '.join(args[3:])
                where_clause = parse_where_condition(where_str)
                
                if where_clause is None:
                    continue
                
                # Выполняем удаление
                new_data = delete(table_data, where_clause)
                if new_data is not None:
                    save_table_data(table_name, new_data)
                    print("Данные успешно удалены")
                
            else:
                print(f"Неизвестная команда: {command}")
                print("Введите 'help' для справки")
                
        except KeyboardInterrupt:
            print("\nВыход из программы...")
            break
        except Exception as e:
            print(f"Произошла ошибка: {e}")
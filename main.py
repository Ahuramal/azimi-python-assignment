import math
import sqlalchemy as db
import pandas as pd


def main():
    engine = db.create_engine(f"mysql+pymysql://{'admin'}:{'azimi#1234'}@localhost/assignment")
    connection = engine.connect()
    training_data = pd.read_csv('datasets/train.csv')
    training_data.to_sql('training_data', engine, index=False)

    ideal_functions = pd.read_csv('datasets/ideal.csv')
    ideal_functions.to_sql('ideal_functions', engine, index=False)

    selected_ideal_functions = {}
    for x in range(1, 5):
        training_tb = db.Table('training_data', db.MetaData(),
                               include_columns=['x', f'y{x}'], autoload=True, autoload_with=engine)
        training_set = connection.execute(db.select([training_tb])).fetchall()
        minimum_deviation = math.inf
        selected_function = 0
        max_deviation = 0
        for y in range(1, 51):
            total_deviation = 0
            maximum_deviation = 0
            ideal = db.Table('ideal_functions', db.MetaData(),
                             include_columns=['x', f'y{y}'], autoload=True, autoload_with=engine)
            ideal_set = connection.execute(db.select([ideal])).fetchall()
            for z in range(len(training_set)):
                deviation = math.fabs(ideal_set[z][1] - training_set[z][1])
                total_deviation += math.pow(deviation, 2)
                if deviation > maximum_deviation:
                    maximum_deviation = deviation
            if total_deviation < minimum_deviation:
                minimum_deviation = total_deviation
                selected_function = y
                max_deviation = maximum_deviation
        selected_ideal_functions.update({x: (selected_function, minimum_deviation, max_deviation)})
    print('The Four Ideal Functions:')
    print(selected_ideal_functions)

    test_data = pd.read_csv('datasets/test.csv')
    test_data.to_sql('mapping_test_data', engine, index=False)
    query = f'-- ALTER TABLE mapping_test_data ADD selected_ideal_function VARCHAR(32), ADD deviation DOUBLE;'
    connection.execute(query)

    test_data_tb = db.Table("mapping_test_data", db.MetaData(), autoload=True, autoload_with=engine)
    for i in test_data.values:
        selected_function_for_test = 'NONE'
        selected_function_deviation = math.inf
        found_ideal_function = False
        for j in range(1, 5):
            ideal = db.Table('ideal_functions', db.MetaData(),
                             include_columns=['x', f'y{selected_ideal_functions[j][0]}'],
                             autoload=True, autoload_with=engine)
            ideal_data = connection.execute(db.select([ideal]).where(ideal.c.x == i[0])).fetchall()

            current_deviation = math.fabs(i[1] - float(ideal_data[0][1]))
            is_selected = current_deviation <= float(selected_ideal_functions[j][2]) * math.sqrt(2)

            if is_selected and current_deviation < selected_function_deviation:
                found_ideal_function = True
                selected_function_for_test = f'y{selected_ideal_functions[j][0]}'
                selected_function_deviation = current_deviation

        if found_ideal_function:
            sql_query = db.update(test_data_tb).values(selected_ideal_function=selected_function_for_test,
                                                       deviation=selected_function_deviation) \
                .where(test_data_tb.columns.x == i[0])
            connection.execute(sql_query)


if __name__ == '__main__':
    main()

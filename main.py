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
                deviation = ideal_set[z][1] - training_set[z][1]
                total_deviation += math.pow(deviation, 2)
                if deviation > maximum_deviation:
                    maximum_deviation = deviation
            if total_deviation < minimum_deviation:
                minimum_deviation = total_deviation
                selected_function = y
                max_deviation = maximum_deviation
        selected_ideal_functions.update({x: (selected_function, minimum_deviation, max_deviation)})
    print(selected_ideal_functions)


if __name__ == '__main__':
    main()


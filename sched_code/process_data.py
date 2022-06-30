import os

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
import pandas as pd


def save_into_csv(file_name, file_folder, data_df):
    path = os.getcwd() + '\\' + file_folder + '\\'
    if not os.path.exists(path):
        os.makedirs(path)
    data_df.to_csv(path + file_name + '.csv', encoding="utf-8-sig", index=0)


def get_files_list(file_folder, abs_dir_flag=False, file_type='.xlsx'):
    if abs_dir_flag:
        path = file_folder + '\\'
    else:
        path = os.getcwd() + '\\' + file_folder + '\\'
    files_list = []
    if not os.path.exists(path):
        return files_list
    for file_name in os.listdir(path):
        if os.path.splitext(file_name)[1] == file_type:
            index = file_name.rfind(".")
            files_list.append([file_name[:index], path + file_name])
    return files_list


def generate_train_test_data(rate, processed_file_folder, lag_num):
    # files_list = get_files_list(processed_file_folder, abs_dir_flag=False, file_type='.csv')
    files_list = [processed_file_folder]
    data = {}
    for file_info in files_list:
        file_name = file_info
        file_path = 'processed_data/' + file_info
        print("<<<<<<<<<<<<<<<get the data from the file: " + file_info)
        data_df = pd.read_csv(file_path)

        x_train = pd.DataFrame()
        y_train = pd.DataFrame()
        x_test = pd.DataFrame()
        y_test = pd.DataFrame()
        id_name = []
        for data_column in data_df.columns:
            temp = pd.DataFrame()
            temp[data_column] = data_df[data_column].dropna()
            if len(temp[data_column]) < lag_num + 1:
                continue
            divide_length = int(len(temp[data_column]) * rate)

            train_y_series = temp[data_column].shift(-1)[:(divide_length - 1)]

            y_train = pd.concat([y_train, train_y_series])

            train_x_df = pd.DataFrame()
            for i in range(lag_num):
                train_x_df[i] = temp[data_column].shift(i)[:(divide_length - 1)].fillna(0)
            x_train = pd.concat([x_train, train_x_df])

            predict_x_df = pd.DataFrame()
            for j in range(lag_num):
                predict_x_df[j] = temp[data_column].shift(j)[(divide_length - 1):-1].fillna(0)
            x_test = pd.concat([x_test, predict_x_df])
            true_y_list = temp[data_column][divide_length:]
            y_test = pd.concat([y_test, true_y_list])
            for i in range(len(true_y_list)):
                id_name.append(data_column)
        data[file_name] = ([x_train, y_train], [x_test, y_test], id_name)
   
    return data








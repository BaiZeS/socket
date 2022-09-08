from sql_server import execute_sql, connect_sqlserver, close_sql
import json, os, time
from hash_lib import hash_code


data_path = os.path.join(os.path.dirname(__file__), 'data')
log_path = os.path.join(os.path.dirname(__file__), 'log')
json_files = [os.path.join(data_path, file) for file in os.listdir(data_path)]
log_file = os.path.join(log_path, 'sqlerror.log')

devices = ['50002E001150565648383120', '6D005900135056594D373420', '4D002E000C50475842303620']
deviceID_new = ['SDQG000006', 'SDQG000005', 'SDQG000003']
# deviceName = ['茂县三道桥沟隧道口', '茂县三道桥沟山坡上', '茂县三道桥沟河道边上']
hash_key = 'waterwjspassword@username'


def file2sql(json_files):
    '''
    将本地json文件写入sqlserver
    '''
    for json_file in json_files:
        # 定义数据列表
        rainfall_data = []
        spec_data = []
        water_data = []
        elec_data = []      # 写入电量表

        # 读取单个文件并转换数据格式
        with open(json_file, 'r', encoding='utf-8') as f:
            data = [json.loads(data.replace('\n', '').replace('\t', '')) for data in f.read().strip().split('\n\n')]        # 读取文件并转换为dic
            f.close()

        # 构建sql写入语句,加入电量表
        insert_rainfall = r'insert into RainfallData values ({})'.format(','.join(['%s']*6))
        insert_spec = r'insert into SpecParamsCollect(DeviceID, CollectionTime, Dcm_Para1, Dcm_Para8) values ({})'.format(','.join(['%s']*4))
        insert_water = r'insert into WaterHisData values ({})'.format(','.join(['%s']*7))
        insert_elec = r'insert into DeviceState(DeviceStateID, DeviceID, BatteryVoltage, CollectionTime, OtherState1) values ({})'.format(','.join(['%s']*5))
        sql_list = [insert_rainfall, insert_water, insert_spec, insert_elec]

        # 构造写入数据
        for data_one in data:
            device_id = data_one['device_id']
            device_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(data_one['time'])))
            device_elec = data_one['vol']
            device_data = data_one['datas']
            update_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            hash_id = hash_code(hash_key, device_id+str(device_time))
            # 构造降雨量数据
            if device_id in (devices[0], devices[1]):
                rainfall_data.append((deviceID_new[devices.index(device_id)], device_time, 'ADD', float(device_data[0]['data']), 50, update_time))
                # 构造含水率数据
                dp8 = float(device_data[1]['data']) if device_data[1]['data'] else 0.0
                dp1 = float(device_data[2]['data']) if len(device_data)>2 else 0.0
                spec_data.append((deviceID_new[devices.index(device_id)], device_time, dp1, dp8))
            # 构造流速数据
            if device_id == devices[2]:
                water_data.append((hash_id, deviceID_new[devices.index(device_id)], 0.0, device_data[0]['data'], device_time, 0, update_time))
            # 构造电量数据
            elec_data.append((hash_id, deviceID_new[devices.index(device_id)], device_elec, device_time, 20))
        
        # 合并数据
        sql_data = [tuple(rainfall_data), tuple(water_data), tuple(spec_data), tuple(elec_data)]
        
        # 将数据写入数据库
        try:
            sql_conn = connect_sqlserver()  # 连接sql数据库
            for insert_sql, insert_data in list(zip(sql_list, sql_data)):
                execute_sql(sql_conn, insert_sql, insert_data)    # 写入数据
                print('insert into sqlserver successed~')
            close_sql(sql_conn) # 关闭sql连接
        except Exception as e:
            with open(log_file, 'a+') as f:
                f.write('%s: something error in insert into sql server: %s' % (str(update_time), repr(e)))
                f.write('\n')
                f.close()



def json2sql(json_data):
    '''
    将json数据写入sqlserver
    '''
    # 数据清洗
    data_one = json.loads(json_data.replace('\n', '').replace('\t', ''))

    # 构建sql写入语句
    insert_rainfall = r'insert into RainfallData values ({})'.format(','.join(["'%s'"]*6))
    insert_spec = r'insert into SpecParamsCollect(DeviceID, CollectionTime, Dcm_Para1, Dcm_Para8) values ({})'.format(','.join(["'%s'"]*4))
    insert_water = r'insert into WaterHisData values ({})'.format(','.join(["'%s'"]*7))
    insert_elec = r'insert into DeviceState(DeviceStateID, DeviceID, BatteryVoltage, CollectionTime, OtherState1) values ({})'.format(','.join(['%s']*5))

    # 构造写入数据
    device_id = data_one['device_id']
    device_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(data_one['time'])))
    device_data = data_one['datas']
    update_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    device_elec = data_one['vol']
    hash_id = hash_code(hash_key, device_id+str(device_time))

    # 构造降雨量数据
    if device_id in (devices[0], devices[1]):
        insert_data = insert_rainfall % (deviceID_new[devices.index(device_id)], device_time, 'ADD', float(device_data[0]['data']), 50, update_time)
        # 构造含水率数据
        dp8 = float(device_data[1]['data']) if device_data[1]['data'] else 0.0
        dp1 = float(device_data[2]['data']) if len(device_data)>2 else 0.0
        insert_data = insert_spec % (deviceID_new[devices.index(device_id)], device_time, dp1, dp8)
    # 构造流速数据
    if device_id == devices[2]:
        water_id = hash_code(hash_key, device_id+str(device_time))
        insert_data = insert_water % (water_id, deviceID_new[devices.index(device_id)], 0.0, device_data[0]['data'], device_time, 0, update_time)
    # 构造电量数据
    insert_elec_vol = insert_elec % (hash_id, deviceID_new[devices.index(device_id)], device_elec, device_time, 20)
    
    # 将数据写入数据库
    try:
        sql_conn = connect_sqlserver()  # 连接sql数据库
        execute_sql(sql_conn, insert_data)    # 写入数据
        execute_sql(sql_conn, insert_elec_vol)      # 写入电量数据
        close_sql(sql_conn) # 关闭sql连接
    except Exception as e:
        with open(log_file, 'a+') as f:
            f.write('%s: something error in insert into sql server: %s' % (str(update_time), repr(e)))
            f.write('\n')
            f.close()


# 将本地数据写入数据库
# file2sql(json_files)
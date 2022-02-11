import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import re
import threading

AK = 'asdasdsadasdsasdasdasdasdasdasdsad' #Shawn
key = 'asddddddddddddddddddddssssssssssss' #Shawn`s 高德

'''
    百度地图坐标转换为坐标系
'''
def Pos2Coord(address):
    """
    :param address:
    :return: lng
            lat
            precise
            conf
            comp
            level
    """
    url = 'https://api.map.baidu.com/geocoding/v3/?address=%s&output=json&ak=%s' % (address, AK)
    res = requests.get(url) #返回一个包含服务器资源的Response对象
    if res.status_code == 200:
        val = res.json()
        if val['status'] == 0:
            retVal = {'lng': val['result']['location']['lng'], 'lat': val['result']['location']['lat'],
                      'precise': val['result']['precise'], 'conf': val['result']['confidence'],
                      'comp': val['result']['comprehension'], 'level': val['result']['level']}
        else:
            retVal = None
        return retVal
    else:
        print('无法获取%s经纬度' % address)


def Coord2Pos(lng, lat, town='true'):
    '''
		@func: 通过百度地图API将经纬度转换成地理名称
		@input:
			lng: 经度
			lat: 纬度
			town: 是否获取乡镇级地理位置信息，默认获取。可选参数（true/false）
		@output:
			address:解析后的地理位置名称
			province:省份名称
			city:城市名
			district:县级行政区划名
			town: 乡镇级行政区划
			adcode: 县级行政区划编码
			town_code: 镇级行政区划编码
	'''
    url = 'http://api.map.baidu.com/reverse_geocoding/v3/?output=json&ak=%s&location=%s,%s&extensions_town=%s'%(AK, lat, lng, town)
    res = requests.get(url)
    if res.status_code == 200:
        val = res.json()
        if val['status'] == 0:
            val = val['result']
            retVal = {'address': val['formatted_address'], 'province': val['addressComponent']['province'],
                      'city': val['addressComponent']['city'], 'district': val['addressComponent']['district'],
                      'town': val['addressComponent']['town'], 'adcode': val['addressComponent']['adcode'],
                      'town_code': val['addressComponent']['town_code']}
        else:
            retVal = None
        return retVal
    else:
        print('无法获取(%s,%s)的地理信息！' % (lat, lng))

'''
    高德地图的地址信息转换坐标系
'''
def Pos2Coord_Gaode(address):
    """
    :param address:
    :return:
        json
    """
    url = 'https://restapi.amap.com/v3/geocode/geo?key=%s&address=%s&city=上海'%(key, address)
    res = requests.get(url)
    if res.status_code == 200:
        val = res.json() #解读为 json 格式
        if val['status'] == '1':
            str = val['geocodes'][0]['location'].split(',')
            """
            retVal = {'lng': str[0], 'lat': str[1], 'formatted_address': val['geocodes'][0]['formatted_address'],
                      'township': val['geocodes'][0]['township'], 'neighborhood_name': val['geocodes'][0]['neighborhood']['name'],
                      'neighborhood_type': val['geocodes'][0]['neighborhood']['type'], 'building_name': val['geocodes'][0]['building']['name'],
                      'building_type': val['geocodes'][0]['building']['type'], 'adcode': val['geocodes'][0]['adcode'], 'street': val['geocodes'][0]['street'],
                      'street_number': val['geocodes'][0]['number'], 'level': val['geocodes'][0]['level']}
            """
            retVal = {'lng': str[0], 'lat': str[1], 'adcode': val['geocodes'][0]['adcode'], 'level': val['geocodes'][0]['level']}
        else:
            retVal = None
        return retVal
    else:
        print('无法获取%s经纬度' % address)

def thread_run(_tuple):
    line_list = _tuple
    # 开启进程，利用的是ThreadPoolExecutor，开启100个线程
    executor = ThreadPoolExecutor(max_workers=100)
    all_tasks = [executor.submit(do_data_process, line) for line in line_list]
    data_list = list()
    for future in as_completed(all_tasks):
        result_dict = future.result()
    if result_dict:
        data_list.append(result_dict)

    # 这边可以插入数据

    #df = pd.DataFRame（data_list）

    #df.to_csv("xx.csv")

#处理数据，将地址信息处理为无()号的形式
def do_string(address) -> str:
    pattern = r'\(.*?\)'
    temp = re.compile(pattern) #预先编译，这样更快
    res1 = temp.search(address) #查找位置
    if (res1 != None):
        res = re.sub(temp, '', address)  # 替代位置
        print(res)
        return res
    #res = address.split('号')
    #return res[0] + '号'
    return address


def do_data_process():
    # 该功能是数据处理好后返回数据
    pass

def to_csv(num):
    start = time.perf_counter()
    # df = pd.read_csv(r'D:\Data\temp.csv')
    temp_pattern = r'\(.*?\)'
    pattern = re.compile(temp_pattern)  # 预先编译，这样更快
    temp_num = r'D:\Data\cut_data\part_' + str(num) + '.csv'
    df = pd.read_csv(temp_num)

    lens = len(df)
    for i in range(0, len):
        # address = r'浦东新区老港镇建港村港西13组149号(G44089/1006#)'
        address = df.iloc[i]['district'] + df.iloc[i]['address']
        print(address)
        res1 = pattern.search(address)  # 查找位置
        if (res1 != None):
            address = re.sub(pattern, '', address)  # 替代位置
        res = Pos2Coord(address)  # 访问得到坐标
        try:
            df.loc[i, ['lng', 'lat', 'precise', 'conf', 'comp', 'level']] = [res['lng'], res['lat'], res['precise'],
                                                                             res['conf'], res['comp'], res['level']]
        except TypeError:
            print(i)
            fh = open("badata.txt", 'a')  # ’a‘ 表示为追加写入 'w'表示为覆盖写入
            fh.write(str(int(df.loc[i, ['Num']])) + '\n')
            fh.close()
        print(res)

    df.to_csv(r'D:\Data\cut_data\cov_01.csv', index=False)
    end = time.perf_counter()
    print(str(end - start))
    """
        address = '浦东新区老港镇建港村港西13组149号(G44089/1006#)'
        res = Pos2Coord(address)
        print(res)
        address = do_string(address)
        res = Pos2Coord(address)
        print(res)
        """


if __name__ == '__main__':
    """
    百度
    start = time.perf_counter()
    # df = pd.read_csv(r'D:\Data\temp.csv')
    temp_pattern = r'\(.*?\)'
    pattern = re.compile(temp_pattern)  # 预先编译，这样更快
    dir = '/home/ubuntu/data/'
    for j in range (100, 254):
        temp_num = dir + 'cut/part_' + str(j) + '.csv'
        temp_save = dir + 'res/part_' + str(j) + '.csv'
        df = pd.read_csv(temp_num)
        lens = len(df)
        for i in range(0, lens):
            # address = r'浦东新区老港镇建港村港西13组149号(G44089/1006#)'
            address = df.iloc[i]['district'] + df.iloc[i]['address']
            print(address)
            res1 = pattern.search(address)  # 查找位置
            if (res1 != None):
                address = re.sub(pattern, '', address)  # 替代位置
            res = Pos2Coord(address)  # 访问得到坐标
            try:
                df.loc[i, ['lng', 'lat', 'precise', 'conf', 'comp', 'level']] = [res['lng'], res['lat'], res['precise'],
                                                                             res['conf'], res['comp'], res['level']]
            except TypeError:
                print(i)
                fh = open("/home/ubuntu/data/badata.txt", 'a')  # ’a‘ 表示为追加写入 'w'表示为覆盖写入
                fh.write(str(int(df.loc[i, ['Num']])) + '\n')
                fh.close()
            print(res)

        df.to_csv(temp_save, index=False)
    """

    """
    高德
    """
    start = time.perf_counter()
    # df = pd.read_csv(r'D:\Data\temp.csv')
    temp_pattern = r'\(.*?\)'
    pattern = re.compile(temp_pattern)  # 预先编译，这样更快
    dir = '/home/ubuntu/data/' #Linux-Tencent
    #dir = '/home/zzl/' #Linux-212
    for j in range(255, 257):
        temp_num = dir + 'cut/temp/part_' + str(j) + '.csv'
        temp_save = dir + 'res/part_' + str(j) + '.csv'
        df = pd.read_csv(temp_num)
        lens = len(df)
        for i in range(0, lens):
            # address = r'浦东新区老港镇建港村港西13组149号(G44089/1006#)'
            address = df.iloc[i]['district'] + df.iloc[i]['address']
            print(address)
            res1 = pattern.search(address)  # 查找位置
            if (res1 != None):
                address = re.sub(pattern, '', address)  # 替代位置
            res = Pos2Coord_Gaode(address)  # 访问得到坐标
            try:
                df.loc[i,['lng','lat','adcode','level']] = [res['lng'],res['lat'],res['adcode'],res['level']]
            except TypeError:
                print(i)
                fh = open("/home/ubuntu/data/badata.txt", 'a')  # ’a‘ 表示为追加写入 'w'表示为覆盖写入
                fh.write(str(int(df.loc[i, ['Num']])) + '\n')
                fh.close()
            print(res)

        df.to_csv(temp_save, index=False)

    """
    尝试
    df = pd.read_csv(r'D:\Data\temp\part_253.csv')
    lens = len(df)
    for i in range(0, lens):
    #res = Pos2Coord(address)
        address = df.iloc[i]['district'] + df.iloc[i]['address']
        res = Pos2Coord_Gaode(address)
        print(res)
        df.loc[i,['lng','lat','adcode','level']] = [res['lng'],res['lat'],res['adcode'],res['level']]
    df.to_csv(r'D:\Data\temp\part_255.csv', index=False)
    #res = Pos2Coord(address)
    #print(res)
    """




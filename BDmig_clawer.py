from Clawer_Base.logger import logger
from Clawer_Base.clawer_frame import Clawer
from Clawer_Base.user_agents import User_agents
import requests
import pandas as pd
from multiprocessing.dummy import Pool as ThreadPool
from requests.exceptions import ReadTimeout, ConnectionError
from datetime import datetime
import json
import os
import prettytable
import time
import random

class Migration_Params(dict):
    """将传入的块转化为网页所需的表单"""
    params = {
            'type': '',
            'sort_by': 'low_index',
            'limit': '20',
            'city_name': '',
            'date_start': '',
            'date_end': ''
        }
    def __init__(self, city_name, date, m_type):
        m_type_dict = {'type': m_type}
        date_dict = {'date_start': date,
                     'date_end': date
                     }
        city_name_dict = {'city_name': city_name}
        self.update(self.params)
        self.update(city_name_dict)
        self.update(date_dict)
        self.update(m_type_dict)


class BD_Migration(Clawer):
    def __init__(self, params):
        super(BD_Migration, self).__init__(params)
        self.url = 'http://qianxi.baidu.com/api/city-migration.php?'

    def scheduler(self):
        return self.parser()

    def parser(self):
        # print(self.respond)
        if self.respond:
            df = pd.DataFrame(self.respond)
            df['city'] = self.params['city_name']
            df['date'] = self.params['date_end']
            # res_list = df.to_dict('records')
            return df
        else:
            print(self.req_url)
            logger.info('%s 没有数据' % self.params['city_name'])

def datelist(start='20180322', end='20180123'):
    date_df = pd.date_range(start, end)
    date_list = [date.strftime('%Y%m%d') for date in date_df]
    return date_list


def param_info(info_dict):
    info_table = prettytable.PrettyTable(['项目', '描述'])
    for key in list(info_dict.keys()):
        info_table.add_row([key, info_dict[key]])
    info_table.align = 'l'
    logger.info('\n' + str(info_table))

def main(start, end):
    dateList = datelist(start, end)
    city_df = pd.read_excel(r'D:\program_lib\BD_migration\base_data\city_names1.xlsx', index_col='province')
    cityList = list(city_df['city'])
    def by_date(date):
        if os.path.exists('Migration_result/%s' % date):
            pass
        else:
            os.makedirs('Migration_result/%s' % date)
        def by_mtype(mtype):
            def by_city(city_name):
                print("开始抓取 %s %s %s" % (date, mtype, city_name))
                migration_params = Migration_Params(city_name, date, mtype)
                a_clawer = BD_Migration(migration_params)
                return a_clawer.process()
            pool = ThreadPool()
            results = pool.map(by_city, cityList)
            pool.close()
            pool.join()
            results = [i for i in results if type(i) == pd.core.frame.DataFrame]
            # print(results)
            if results:
                df = pd.concat(results)
                df.to_csv('Migration_result/%s/%s_%s.csv' % (date, date, mtype))
            else:
                print('%s-%s 没有数据'% (date, mtype))

        pool_lv1 = ThreadPool(2)
        pool_lv1.map(by_mtype, ['migrate_in', 'migrate_out'])
        pool_lv1.close()
        pool_lv1.join()

    pool_lv2 = ThreadPool(1)
    pool_lv2.map(by_date, dateList)
    pool_lv2.close()
    pool_lv2.join()

if __name__ == '__main__':
    local_ip = ''
    proxy_pool = []
    info_dict = {'名称': '迁徙信息抓取工具V1.0',
                 '邮箱': '575548935@qq.com',
                 '起始时间': '20180323',
                 '终止时间': '20180424'
                 }
    param_info(info_dict)
    start = info_dict['起始时间']
    end = info_dict['终止时间']
    main(start, end)
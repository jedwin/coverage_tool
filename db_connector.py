import psycopg2
import logging
import time
import math
import coordinate_converter as cc
import os

# import geopandas
# import numpy as np
# import pandas as pd
# import shapely
# import missingno as msn
# import seaborn as sns
# import matplotlib.pyplot as plt
# import descartes
# from geoalchemy2 import Geometry, WKTElement
# from sqlalchemy import *
Data_Type_Both = 'Both'
Data_Type_HO = 'Handover'
Data_Type_Coverage = 'Coverage'
tb_name_cell = 'cell_info'
tb_name_ho_relation = 'cell_ho_info'
tb_name_coverage_relation = 'cell_relation_gd'
tb_name_planned_pci = 'cell_planning'


class PostgresDb:
    db_host = 'localhost'
    # db_host = '132.96.10.83'
    db_name = 'postgres'
    db_user = 'postgres'
    db_password = 'postgres'
    options = '-c search_path=dbo,public'
    db_port = 5432
    conn_str = ''
    conn = None
    engine_str = ''
    buffer_radius = 1000  # 扩展孤岛小区半径，单位：米.扩展越大越能保证孤岛站pci复用距离，但会使规划更困难
    isolate_cell_relation_thresh = 5  # 邻接小区数目小于该门限判断为孤岛小区
    isolate_cell_relation_factor = 0.01  # 为了减少扩展后的"虚拟"重叠面积计算结果，需要乘上加此系数

    def __init__(self):
        self.conn = psycopg2.connect(host=self.db_host, port=5432, dbname=self.db_name,
                                     user=self.db_user, password=self.db_password, options=self.options)
        self.engine_str = 'postgresql://%s:%s@%s:%d/%s' % \
                          (self.db_user, self.db_password, self.db_host, self.db_port, self.db_name)

    def table_is_exist(self, table_name):
        """
        检查输入表是否在数据库中存在
        :param table_name: 表名
        :return: True/False
        """
        try:
            cur = self.conn.cursor()
            cur.execute(f'select count(*) from pg_class where relname = \'{table_name}\';')
            table_count = cur.fetchone()
            # table_count should be a tuple
            if table_count[0] > 0:
                return True
            else:
                return False
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            return False
        finally:
            if cur is not None:
                cur.close()

    def index_is_exist(self, table_name, index_name):
        '''
        检查某张表内是否已存在某个index

        :param table_name: 表名
        :param index_name: 索引名
        :return: 如果已存在则返回True，否则返回False
        '''
        try:
            strSQL = f'''
                select
                    t.relname as table_name,
                    i.relname as index_name,
                    a.attname as column_name
                from
                    pg_class t,
                    pg_class i,
                    pg_index ix, 
                    pg_attribute a 
                where 
                    t.oid = ix.indrelid 
                    and i.oid = ix.indexrelid 
                    and a.attrelid = t.oid 
                    and a.attnum = ANY(ix.indkey) 
                    and t.relkind = 'r' 
                    and t.relname = '{table_name}' 
                    and i.relname = '{index_name};' 
                '''
            cur = self.conn.cursor()
            cur.execute(strSQL)
            index_count = cur.fetchone()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            return False
        finally:
            if cur is not None:
                cur.close()
                return True
            else:
                return False

    def test(self):
        try:
            cur = self.conn.cursor()
            cur.execute('SELECT version()')
            db_version = cur.fetchone()
            return db_version
            cur.close()

            return True
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            return False
        finally:
            if cur is not None:
                cur.close()
                # print('Cursor is closed.')

    def close(self):
        if self.conn is not None:
            self.conn.close()
            print('Database connection closed.')

    def get_relation(self, cities=[], num_of_lines=0, relation_type='coverage', auto_neighbor=True):
        try:
            num_limit = ''
            cur = self.conn.cursor()
            where_clause = ' where '
            if relation_type == Data_Type_Coverage:
                tb_name = tb_name_coverage_relation
                relation_field = 'st_area'
            else:
                tb_name = tb_name_ho_relation
                relation_field = 'ho_req'
            where_clause += f'{relation_field} > 0 '
            if len(cities) > 0:
                print(f'Getting cells {relation_type} relation from {str(cities)[1:-1]} and neighbor cities')
                if auto_neighbor:
                    cities_list_string = []
                    for city in cities:
                        neighbor_cities_data = self.get_neighbor_city(city)
                        '''
                                columns:
                                 0   city 
                                 1   b_city  已包括自身
                                 2   count
                        '''
                        for neighbor_cities in neighbor_cities_data:
                            cities_list_string.append(neighbor_cities[1])
                else:
                    cities_list_string = cities
                # print(f'cities_list_string: {cities_list_string}')
                where_clause += f' and city in ({str(set(cities_list_string))[1:-1]}) '
                where_clause += f' and b_city in ({str(set(cities_list_string))[1:-1]})'
                if relation_type == Data_Type_Coverage:
                    where_clause += f' and not expanded'
            if num_of_lines > 0:
                num_limit = f'limit {num_of_lines}'

            sql_string = f'''SELECT enodeb_id, cell_id, 
                        b_enodeb_id, b_cell_id, 
                        {relation_field}  
                        from {tb_name} {where_clause} {num_limit} 
                        '''
            # logger.debug(sql_string)
            # print(sql_string)
            cur.execute(sql_string)
            return cur.fetchall()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            return False
        finally:
            if cur is not None:
                cur.close()

    def get_cells(self, cities=[], sql_string='', num_of_lines=0, auto_neighbor=True):
        """
        从数据库中读取小区资料

        :param auto_neighbor: 是否自动获取相邻地市
        :param cities:
        :param sql_string: 如果指定SQL语句，则直接按该语句执行，忽略其他设置
        :param num_of_lines:
        :return:
        """
        try:

            num_limit = ''
            where_clause = ''
            cur = self.conn.cursor()
            print(f'Getting cells info from {str(cities)[1:-1]} and neighbor cities')
            if len(cities) > 0:

                if auto_neighbor:
                    cities_list_string = []
                    for city in cities:
                        neighbor_cities_data = self.get_neighbor_city(city)
                        '''
                                columns:
                                 0   city 
                                 1   b_city  已包括自身
                                 2   count
                        '''
                        for neighbor_cities in neighbor_cities_data:
                            cities_list_string.append(neighbor_cities[1])
                else:
                    cities_list_string = cities

                if len(tb_name_planned_pci) > 0:
                    sql_string = f'''
                    select distinct * from (
                            SELECT distinct  city, enodeb_id, cell_id, arfcn, pci,  null, 0 area, 
                                true can_be_changed, 0 longitude, 0 latitude 
                            from {tb_name_coverage_relation} 
                            where city in ({str(set(cities_list_string))[1:-1]}) and arfcn notnull				  
                        union 
                            SELECT distinct  city, enodeb_id, cell_id, arfcn, pci,  null, 0 area, 
                                true can_be_changed, 0 longitude, 0 latitude 
                            from {tb_name_ho_relation}
                            where city in ({str(set(cities_list_string))[1:-1]}) and arfcn notnull
                        union
                            SELECT distinct  b_city, b_enodeb_id, b_cell_id, b_arfcn, b_pci, null, 0 area, 
                                true can_be_changed, 0 longitude, 0 latitude 
                            from {tb_name_coverage_relation} 
                            where b_city in ({str(set(cities_list_string))[1:-1]}) and b_arfcn notnull
                        union 
                            SELECT distinct  b_city, b_enodeb_id, b_cell_id, b_arfcn, b_pci, null, 0 area, 
                                true can_be_changed, 0 longitude, 0 latitude 
                            from {tb_name_ho_relation}
                            where b_city in ({str(set(cities_list_string))[1:-1]}) and b_arfcn notnull) a
                    order by city desc
                    '''
                    # print(sql_string)
                    cur.execute(sql_string)
                    return cur.fetchall()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            return False
        finally:
            if cur is not None:
                cur.close()

    def get_neighbor_city(self, city):
        cur = self.conn.cursor()
        cur.execute(f'''select city, b_city,count(*) from {tb_name_ho_relation}
                    where (city=\'{city}\' or b_city=\'{city}\') and city notnull and b_city notnull
                    group by city, b_city''')
        return cur.fetchall()

    def prepare_data(self, table_name, geom_idx='geom_idx', handle_polygon=False, calc_area=True, create_index=True):
        """
        数据预处理：
        1、增加面积字段、geom2临时字段
        2、将可能出现异常的geometry makevalid，然后在统一转换为Multi Polygon格式
        3、创建索引
        4、计算每个小区覆盖面积
        :return:
        """
        try:
            cur = self.conn.cursor()
            if len(table_name) > 0:
                if self.table_is_exist(table_name):
                    print(f'开始对{table_name}进行预处理')
                    if handle_polygon:
                        # 由于在导入数据时已经处理，因此忽略这个步骤
                        pass
                        # print(f'adding geom2 to {table_name}')
                        # 增加面积字段、geom2临时字段
                        # cur.execute(f'ALTER TABLE {table_name} ADD COLUMN geom2 geometry;')
                        # cur.execute('commit;')
                        # 将可能出现异常的geometry makevalid，然后在统一转换为Multi Polygon格式。
                        # print(f'开始处理异常多边形，可能需要好几分钟……')
                        # cur.execute(f'update {table_name} Set geom2 =  ST_CollectionExtract((st_makevalid(geom)),3);')
                        # cur.execute('commit;')
                        # cur.execute(f'ALTER TABLE {table_name} DROP COLUMN geom;')
                        # cur.execute(f'ALTER TABLE {table_name} RENAME geom2 TO geom;')
                        # cur.execute('commit;')

                    if calc_area:
                        print('计算每个小区覆盖面积')
                        # cur.execute(f'ALTER TABLE {table_name} ADD COLUMN area double precision;')
                        cur.execute(f'update {table_name} Set area = st_area(geom::geography);')
                        cur.execute('commit;')

                    if create_index:
                        print(f'开始创建空间索引')
                        if self.index_is_exist(table_name=table_name, index_name=geom_idx):
                            print(f'索引已存在，先删除')
                            cur.execute(f'drop index {geom_idx};')

                        cur.execute(f'CREATE INDEX {geom_idx} ON {table_name} USING GIST (geom);')
                        cur.execute(f'ALTER TABLE {table_name} ALTER COLUMN geom SET NOT NULL;')
                        cur.execute('commit;')
                else:
                    print(f'表：{table_name} 不存在')
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            return False
        finally:
            if cur is not None:
                cur.close()

    def generate_relation(self, table_name, relation_table, cities, arfcn=[], gen_expand=False):
        """
        用于生成每个小区的邻接重叠小区关系表
        :param gen_expand: 是否进行小区扩展
        :param table_name: 小区覆盖数据表名
        :param relation_table: 重叠关系表名
        :param cities: 用于生成重叠关系的地市，并根据city_relation字典获取相邻地市。
        :param arfcn:
        :return:
        """
        try:
            cur = self.conn.cursor()
            if not self.table_is_exist(relation_table):
                if not self.create_relation_table(relation_table, f'relation_idx_{relation_table}'):
                    return False
            if len(table_name) > 0:
                if self.table_is_exist(table_name):
                    print(f'开始对{table_name}计算重叠关系')
                    if len(cities) == 0:
                        return False
                    t1 = time.time()
                    for city in cities:
                        print(f'清空{city}地市的记录')
                        cur.execute(f'delete from {relation_table} where city=\'{city}\';')
                        cur.execute('commit;')
                        if len(arfcn) == 0:
                            arfcn = [75, 100, 1825, 1850, 2446, 2452]
                        arfcn_1 = arfcn.copy()
                        arfcn_2 = arfcn.copy()
                        for a_arfcn in arfcn_1:
                            for b_arfcn in arfcn_2:
                                select_clause = f'''
                                select a.city, a.enodeb_id, a.cell_id, a.arfcn, a.area,
                                a.pci, b.city b_city, b.enodeb_id b_enodeb_id, b.cell_id b_cell_id,
                                b.arfcn b_arfcn, b.area b_area, b.pci b_pci,
                                st_area((st_intersection(a.geom, b.geom))::geography), false expanded'''

                                from_clause = f''' from {table_name} a, {table_name} b'''

                                where_clause = f''' where (a.geom && b.geom)
                                and (a.enodeb_id != b.enodeb_id or a.cell_id != b.cell_id)
                                and a.arfcn = {a_arfcn}
                                and b.arfcn = {b_arfcn}
                                and a.city = \'{city}\';'''
                                # and (b.city=a.city or b.city in ({str(city_relation[city])[1:-1]}))
                                if self.table_is_exist(relation_table):
                                    sql_string = (f'''insert into {relation_table}'''
                                                  + select_clause + from_clause + where_clause)
                                else:
                                    sql_string = (select_clause
                                                  + f''' into {relation_table}''' + from_clause + where_clause)

                                print(f'正在计算{city}的{a_arfcn}和{b_arfcn}重叠关系')

                                cur.execute(sql_string)
                                cur.execute('commit;')
                            # 未避免重复计算，每次b_arfcn循环完之后，就删除已计算过的频点
                            arfcn_2.remove(a_arfcn)

                        # 开始补全孤岛小区
                        if gen_expand:
                            print(f'开始扩展{city}的孤岛小区')
                            sql_string = f'''
                            with isolate_cells as (
                            select b.city, b.enodeb_id, b.cell_id, b.arfcn,b.area, b.pci,st_setsrid(st_buffer(
                            b.geom::geography,{self.buffer_radius})::geometry,4326) geom, count(*) relations 
                            from {relation_table} a 
                            right join {table_name} b on (a.enodeb_id=b.enodeb_id and a.cell_id=b.cell_id)
                            where b.city=\'{city}\'
                            group by b.city, b.enodeb_id, b.cell_id,b.arfcn,b.area,b.pci, b.geom
                            having count(*) < {self.isolate_cell_relation_thresh}
                            order by count(*))
                            
                            insert into {relation_table}
                            select a.city, a.enodeb_id, a.cell_id, a.arfcn, a.area, a.pci, b.city b_city, b.enodeb_id 
                            b_enodeb_id, b.cell_id b_cell_id, b.arfcn b_arfcn, b.area b_area, b.pci b_pci, 
                            st_area((st_intersection(a.geom, b.geom))::geography) * {self.isolate_cell_relation_factor},
                            true expanded 
                            from isolate_cells a, {table_name} b
                            where (a.geom && b.geom)
                            and (a.enodeb_id != b.enodeb_id or a.cell_id != b.cell_id);
                            '''
                            cur.execute(sql_string)
                            cur.execute('commit;')
                        print('删除重叠面积为0的记录')
                        cur.execute(f'delete from {relation_table} where st_area=0;')
                        cur.execute('commit;')
                    t2 = time.time()
                    print(f'共耗时{t2 - t1}秒')
                else:
                    print(f'表：{table_name} 不存在')
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            return False
        finally:
            if cur is not None:
                cur.close()
    # def export2db(self, file_name, tb_name):
    #     if os.path.exists(file_name):
    #         engine = create_engine(self.engine_str, use_batch_mode=True)
    #         try:
    #             geo_data = geopandas.read_file(file_name)
    #             crs = {'init': 'epsg:4326'}
    #             geo_data = geopandas.GeoDataFrame(geo_data, crs=crs)
    #             geo_data['geometry'] = geo_data.apply(lambda x: shapely.wkt.dumps(x.geometry), axis=1)
    #             geo_data.to_sql(tb_name, engine, if_exists='replace', index=None,
    #                       dtype={'geometry': Geometry('GEOMETRY')})
    #             return True
    #         except all:
    #             return False
    #
    #     else:
    #         return False

    def create_coverage_table(self, table_name, geom_idx_name, remove_first=False):
        cur = self.conn.cursor()

        if self.table_is_exist(table_name):
            if not remove_first:
                print(f'table: {table_name} is already exist')
                return False
            else:
                cur.execute(f'DROP TABLE public.{table_name};')
        sql_string = f'''
            CREATE TABLE {table_name}
                (
                    day bigint,
                    city character varying COLLATE pg_catalog."default",
                    enodeb_id integer,
                    cell_id integer,
                    cell_name character varying,
                    band character varying,
                    arfcn integer,
                    pci integer,
                    engine_angle integer,
                    electron_angle integer,
                    azimuth integer,
                    height integer,
                    geom geometry,
                    area double precision,
                    CONSTRAINT {table_name}_pkey PRIMARY KEY (day, enodeb_id, cell_id)
                )
                TABLESPACE pg_default;
                CREATE INDEX {geom_idx_name}
                    ON public.{table_name} USING gist (geom)
                TABLESPACE pg_default;
        '''
        # print(sql_string)
        cur.execute(sql_string)
        cur.execute('commit;')
        return True

    def create_relation_table(self, table_name, relation_idx_name, remove_first=False):
        cur = self.conn.cursor()

        if self.table_is_exist(table_name):
            if not remove_first:
                print(f'table: {table_name} is already exist')
                return False
            else:
                cur.execute(f'DROP TABLE public.{table_name};')
        sql_string = f'''
            CREATE TABLE {table_name}
                (
                    city character varying COLLATE pg_catalog."default",
                    enodeb_id integer,
                    cell_id integer,
                    arfcn integer,
                    area double precision,
                    pci integer,
                    b_city character varying COLLATE pg_catalog."default",
                    b_enodeb_id integer,
                    b_cell_id integer,
                    b_arfcn integer,
                    b_area double precision,
                    b_pci integer,
                    st_area double precision,
                    expanded boolean,
                    CONSTRAINT {table_name}_pkey PRIMARY KEY (enodeb_id, cell_id, b_enodeb_id, b_cell_id)
                )
                TABLESPACE pg_default;
        '''
        cur.execute(sql_string)
        cur.execute('commit;')
        cur.execute(f'''CREATE INDEX {relation_idx_name} ON {table_name} USING btree
            (enodeb_id ASC NULLS LAST, cell_id ASC NULLS LAST, b_enodeb_id ASC NULLS LAST, b_cell_id ASC NULLS LAST)
            TABLESPACE pg_default;''')
        return True

    def insert_data(self, in_data, table_name, in_day=[], in_city=[]):
        """
        将小区多边形数据插入数据库表中
        :param in_city:
        :param in_day:
        :param in_data: [day, city, enodeb_id, cell_id, band, arfcn, pci, ant_engine_angle,
                            ant_electron_angle, ant_azimuth, high, geom, area]
        :param table_name:
        :return:
        """

        if len(in_data) < 12:
            print('wrong format in in_data')
            return False
        else:
            cur = self.conn.cursor()
            day = int(in_data[0])
            if len(in_day) > 0 and day not in in_day:
                return False
            city = in_data[1]
            if len(in_city) > 0 and city not in in_city:
                return False
            enodeb_id = in_data[2]
            cell_id = in_data[3]
            cell_name = in_data[4]
            band = in_data[5]
            arfcn = math.floor(float(in_data[6]))
            pci = in_data[7]
            if in_data[8] == 'NULL':
                ant_engine_angle = 0
            else:
                ant_engine_angle = math.floor(float(in_data[8]))
            if in_data[9] == 'NULL':
                ant_electron_angle = 0
            else:
                ant_electron_angle = math.floor(float(in_data[9]))
            if in_data[10] == 'NULL':
                ant_azimuth = 0
            else:
                ant_azimuth = math.floor(float(in_data[10]))
            if in_data[11] == 'NULL':
                high = 0
            else:
                high = math.floor(float(in_data[11]))
            if len(in_data[12]) < 10:  # 多边形数据存在问题
                return False
            else:
                geom = in_data[12]
                # 以下代码进行百度与WGS84经纬度转换
                list1 = geom.split(';')
                final_list = []
                for a in list1:
                    list2 = a.split('@')
                    coordinates = []
                    for b in list2:
                        list3 = b.split(',')
                        long, lat = cc.bd09_to_wgs84(float(list3[0]), float(list3[1]))
                        coordinates.append([long, lat])
                    final_list.append(coordinates)
                    a_list = []
                    for a in final_list:
                        b_list = []
                        for b in a:
                            # print(b)
                            point = f'{b[0]} {b[1]}'
                            b_list.append(point)
                        polygon = f'({",".join(b_list)})'
                        a_list.append(polygon)
                    multipolygon = f'SRID=4326;MULTIPOLYGON(({",".join(a_list)}))'
            area = 0
            sql_string = f'''
                INSERT INTO public.{table_name}(day, city, enodeb_id, cell_id, cell_name, band, arfcn, pci, 
                    engine_angle, electron_angle, azimuth, height, geom, area) 
                VALUES ({day}, '{city}', {enodeb_id}, {cell_id}, '{cell_name}', '{band}', {arfcn}, {pci}, 
                    {ant_engine_angle}, {ant_electron_angle}, {ant_azimuth}, {high}, 
                    ST_CollectionExtract((st_makevalid(ST_GeomFromEWKT('{multipolygon}'))),3), {area});
            '''
            return sql_string
            # try:
            #     cur.execute(sql_string)
            # except:
            #     print(sql_string)


logging.basicConfig(level=logging.ERROR, format='', filename='/Users/nathantse/pci_planning_log.csv', filemode='w')
logger = logging.getLogger('db_connector')
